// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package kafka

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue/v2"
)

// ManagerConfig holds configuration for managing Kafka topics.
type ManagerConfig struct {
	CommonConfig
}

// finalize ensures the configuration is valid, setting default values from
// environment variables as described in doc comments, returning an error if
// any configuration is invalid.
func (cfg *ManagerConfig) finalize() error {
	var errs []error
	if err := cfg.CommonConfig.finalize(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Manager manages Kafka topics.
type Manager struct {
	cfg         ManagerConfig
	client      *kgo.Client
	adminClient *kadm.Client
	tracer      trace.Tracer
	deleted     metric.Int64Counter
}

// NewManager returns a new Manager with the given config.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if err := cfg.finalize(); err != nil {
		return nil, fmt.Errorf("kafka: invalid manager config: %w", err)
	}
	client, err := cfg.newClient(nil)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed creating kafka client: %w", err)
	}
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	meter := cfg.MeterProvider.Meter("github.com/elastic/apm-queue/kafka")
	deleted, err := meter.Int64Counter("topics.deleted.count",
		metric.WithDescription("The number of deleted topics"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed creating 'topics.deleted.count' metric: %w", err)
	}
	return &Manager{
		cfg:         cfg,
		client:      client,
		adminClient: kadm.NewClient(client),
		tracer:      cfg.tracerProvider().Tracer("kafka"),
		deleted:     deleted,
	}, nil
}

// Close closes the manager's resources, including its connections to the
// Kafka brokers and any associated goroutines.
func (m *Manager) Close() error {
	m.client.Close()
	return nil
}

// DeleteTopics deletes one or more topics.
//
// No error is returned for topics that do not exist.
func (m *Manager) DeleteTopics(ctx context.Context, topics ...apmqueue.Topic) error {
	// TODO(axw) how should we record topics?
	ctx, span := m.tracer.Start(ctx, "DeleteTopics", trace.WithAttributes(
		semconv.MessagingSystemKey.String("kafka"),
	))
	defer span.End()

	namespacePrefix := m.cfg.namespacePrefix()
	topicNames := make([]string, len(topics))
	for i, topic := range topics {
		topicNames[i] = fmt.Sprintf("%s%s", namespacePrefix, topic)
	}
	responses, err := m.adminClient.DeleteTopics(ctx, topicNames...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "DeleteTopics returned an error")
		return fmt.Errorf("failed to delete kafka topics: %w", err)
	}
	var deleteErrors []error
	for _, response := range responses.Sorted() {
		topic := strings.TrimPrefix(response.Topic, namespacePrefix)
		logger := m.cfg.Logger.With(zap.String("topic", topic))
		if m.cfg.TopicLogFieldFunc != nil {
			logger = logger.With(m.cfg.TopicLogFieldFunc(topic))
		}

		if err := response.Err; err != nil {
			if errors.Is(err, kerr.UnknownTopicOrPartition) {
				logger.Debug("kafka topic does not exist")
			} else {
				span.RecordError(err)
				span.SetStatus(codes.Error, "failed to delete one or more topic")
				deleteErrors = append(deleteErrors,
					fmt.Errorf("failed to delete topic %q: %w", topic, err),
				)
				attrs := []attribute.KeyValue{
					semconv.MessagingSystemKey.String("kafka"),
					attribute.String("outcome", "failure"),
					attribute.String("topic", topic),
				}
				if kv := m.cfg.TopicAttributeFunc(topic); kv != (attribute.KeyValue{}) {
					attrs = append(attrs, kv)
				}
				m.deleted.Add(context.Background(), 1, metric.WithAttributeSet(
					attribute.NewSet(attrs...),
				))
			}
			continue
		}
		attrs := []attribute.KeyValue{
			semconv.MessagingSystemKey.String("kafka"),
			attribute.String("outcome", "success"),
			attribute.String("topic", topic),
		}
		if kv := m.cfg.TopicAttributeFunc(topic); kv != (attribute.KeyValue{}) {
			attrs = append(attrs, kv)
		}
		m.deleted.Add(context.Background(), 1, metric.WithAttributeSet(
			attribute.NewSet(attrs...),
		))
		logger.Info("deleted kafka topic")
	}
	return errors.Join(deleteErrors...)
}

// Healthy returns an error if the Kafka client fails to reach a discovered broker.
func (m *Manager) Healthy(ctx context.Context) error {
	if err := m.client.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping kafka brokers: %w", err)
	}
	return nil
}

type memberTopic struct {
	clientID string
	topic    string
}

type regexConsumer struct {
	regex    *regexp.Regexp
	consumer string
}

// MonitorConsumerLag registers a callback with OpenTelemetry
// to measure consumer group lag for the given topics.
func (m *Manager) MonitorConsumerLag(topicConsumers []apmqueue.TopicConsumer) (metric.Registration, error) {
	monitorTopicConsumers := make(map[apmqueue.TopicConsumer]struct{}, len(topicConsumers))
	var regex []regexConsumer
	for _, tc := range topicConsumers {
		monitorTopicConsumers[tc] = struct{}{}
		if tc.Regex != "" {
			re, err := regexp.Compile(tc.Regex)
			if err != nil {
				return nil, fmt.Errorf("failed to compile regex %s: %w",
					tc.Regex, err,
				)
			}
			regex = append(regex, regexConsumer{
				regex:    re,
				consumer: tc.Consumer,
			})
		}
	}

	mp := m.cfg.meterProvider()
	meter := mp.Meter("github.com/elastic/apm-queue/kafka")
	consumerGroupLagMetric, err := meter.Int64ObservableGauge("consumer_group_lag")
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to create consumer_group_lag metric: %w", err)
	}
	assignmentMetric, err := meter.Int64ObservableGauge("consumer_group_assignment")
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to create consumer_group.assignment metric: %w", err)
	}

	namespacePrefix := m.cfg.namespacePrefix()
	gatherMetrics := func(ctx context.Context, o metric.Observer) error {
		ctx, span := m.tracer.Start(ctx, "GatherMetrics")
		defer span.End()

		consumerSet := make(map[string]struct{})
		for _, tc := range topicConsumers {
			consumerSet[tc.Consumer] = struct{}{}
		}
		consumers := make([]string, 0, len(consumerSet))
		for consumer := range consumerSet {
			consumers = append(consumers, consumer)
		}
		m.cfg.Logger.Debug("reporting consumer lag", zap.Strings("consumers", consumers))

		lag, err := m.adminClient.Lag(ctx, consumers...)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("failed to calculate consumer lag: %w", err)
		}
		lag.Each(func(l kadm.DescribedGroupLag) {
			if err := l.Error(); err != nil {
				m.cfg.Logger.Warn("error calculating consumer lag",
					zap.String("group", l.Group),
					zap.Error(err),
				)
				return
			}
			// Map Consumer group member assignments.
			memberAssignments := make(map[memberTopic]int64)
			for topic, partitions := range l.Lag {
				if !strings.HasPrefix(topic, namespacePrefix) {
					// Ignore topics outside the namespace.
					continue
				}
				topic = topic[len(namespacePrefix):]

				logger := m.cfg.Logger
				if m.cfg.TopicLogFieldFunc != nil {
					logger = logger.With(m.cfg.TopicLogFieldFunc(topic))
				}

				var matchesRegex bool
				for _, re := range regex {
					if l.Group == re.consumer && re.regex.MatchString(topic) {
						matchesRegex = true
						break
					}
				}
				if _, ok := monitorTopicConsumers[apmqueue.TopicConsumer{
					Topic:    apmqueue.Topic(topic),
					Consumer: l.Group,
				}]; !ok && !matchesRegex {
					// Skip when no topic matches explicit name or regex.
					continue
				}
				for partition, lag := range partitions {
					if lag.Err != nil {
						logger.Warn("error getting consumer group lag",
							zap.String("group", l.Group),
							zap.String("topic", topic),
							zap.Int32("partition", partition),
							zap.Error(lag.Err),
						)
						continue
					}
					clientID := "nil"
					// If group is in state Empty the lag.Member is nil
					if lag.Member != nil {
						clientID = lag.Member.ClientID
					}
					key := memberTopic{topic: topic, clientID: clientID}
					count := memberAssignments[key]
					count++
					memberAssignments[key] = count
					attrs := []attribute.KeyValue{
						attribute.String("group", l.Group),
						attribute.String("topic", topic),
						attribute.Int("partition", int(partition)),
					}
					if kv := m.cfg.TopicAttributeFunc(topic); kv != (attribute.KeyValue{}) {
						attrs = append(attrs, kv)
					}
					o.ObserveInt64(
						consumerGroupLagMetric, lag.Lag,
						metric.WithAttributeSet(attribute.NewSet(attrs...)),
					)
				}
			}
			for key, count := range memberAssignments {
				attrs := []attribute.KeyValue{
					attribute.String("group", l.Group),
					attribute.String("topic", key.topic),
					attribute.String("client_id", key.clientID),
				}
				if kv := m.cfg.TopicAttributeFunc(key.topic); kv != (attribute.KeyValue{}) {
					attrs = append(attrs, kv)
				}
				o.ObserveInt64(assignmentMetric, count, metric.WithAttributeSet(
					attribute.NewSet(attrs...),
				))
			}
		})
		return nil
	}
	return meter.RegisterCallback(gatherMetrics, consumerGroupLagMetric,
		assignmentMetric,
	)
}

// CreateACLs creates the specified ACLs in the Kafka cluster.
func (m *Manager) CreateACLs(ctx context.Context, acls *kadm.ACLBuilder) error {
	res, err := m.adminClient.CreateACLs(ctx, acls)
	if err != nil {
		return fmt.Errorf("failed to create ACLs: %w", err)
	}
	var errs []error
	for _, r := range res {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errors.Join(errs...)
}
