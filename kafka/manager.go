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
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue"
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
}

// NewManager returns a new Manager with the given config.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if err := cfg.finalize(); err != nil {
		return nil, fmt.Errorf("kafka: invalid manager config: %w", err)
	}
	client, err := cfg.newClient()
	if err != nil {
		return nil, fmt.Errorf("kafka: failed creating kafka client: %w", err)
	}
	m := &Manager{
		cfg:         cfg,
		client:      client,
		adminClient: kadm.NewClient(client),
		tracer:      cfg.tracerProvider().Tracer("kafka"),
	}
	return m, nil
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
		if err := response.Err; err != nil {
			if errors.Is(err, kerr.UnknownTopicOrPartition) {
				logger.Debug("kafka topic does not exist")
			} else {
				span.RecordError(err)
				span.SetStatus(codes.Error, "failed to delete one or more topic")
				deleteErrors = append(deleteErrors,
					fmt.Errorf("failed to delete topic %q: %w", topic, err),
				)
			}
			continue
		}
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

// MonitorConsumerLag registers a callback with OpenTelemetry
// to measure consumer group lag for the given topics.
func (m *Manager) MonitorConsumerLag(topicConsumers []apmqueue.TopicConsumer) (metric.Registration, error) {
	monitorTopicConsumers := make(map[apmqueue.TopicConsumer]struct{}, len(topicConsumers))
	for _, tc := range topicConsumers {
		monitorTopicConsumers[tc] = struct{}{}
	}

	mp := m.cfg.meterProvider()
	meter := mp.Meter("github.com/elastic/apm-queue/kafka")
	consumerGroupLagMetric, err := meter.Int64ObservableGauge("consumer_group_lag")
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to create consumer_group_lag metric: %w", err)
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

		// Fetch commits for consumer groups.
		groups, err := m.adminClient.DescribeGroups(ctx, consumers...)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("failed to describe groups: %w", err)
		}
		consumerGroups := make([]string, 0, len(groups))
		for _, group := range groups.Sorted() {
			if group.ProtocolType != "consumer" {
				m.cfg.Logger.Debug(
					"ignoring non-consumer group",
					zap.String("group", group.Group),
					zap.String("protocol_type", group.ProtocolType),
				)
				continue
			}
			consumerGroups = append(consumerGroups, group.Group)
		}
		commits := m.adminClient.FetchManyOffsets(ctx, consumerGroups...)

		// Fetch end offsets.
		var endOffsets kadm.ListedOffsets
		listPartitions := groups.AssignedPartitions()
		listPartitions.Merge(commits.CommittedPartitions())
		if topics := listPartitions.Topics(); len(topics) > 0 {
			res, err := m.adminClient.ListEndOffsets(ctx, topics...)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return fmt.Errorf("error fetching end offsets: %w", err)
			}
			endOffsets = res
		}

		// Calculate lag per consumer group.
		for _, group := range groups {
			if group.ProtocolType != "consumer" || group.Err != nil {
				continue
			}
			groupLag := kadm.CalculateGroupLag(group, commits[group.Group].Fetched, endOffsets)
			for topic, partitions := range groupLag {
				if !strings.HasPrefix(topic, namespacePrefix) {
					// Ignore topics outside the namespace.
					continue
				}
				topic = topic[len(namespacePrefix):]

				if _, ok := monitorTopicConsumers[apmqueue.TopicConsumer{
					Topic:    apmqueue.Topic(topic),
					Consumer: group.Group,
				}]; !ok {
					// Ignore unmonitored topics.
					continue
				}
				for partition, lag := range partitions {
					if lag.Err != nil {
						m.cfg.Logger.Warn(
							"error getting consumer group lag",
							zap.String("group", group.Group),
							zap.String("topic", topic),
							zap.Int32("partition", partition),
							zap.Error(lag.Err),
						)
						continue
					}
					o.ObserveInt64(
						consumerGroupLagMetric, lag.Lag,
						metric.WithAttributes(
							attribute.String("group", group.Group),
							attribute.String("topic", topic),
							attribute.Int("partition", int(partition)),
						),
					)
				}
			}
		}
		return nil
	}

	return meter.RegisterCallback(gatherMetrics, consumerGroupLagMetric)
}
