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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue/v2"
)

// TopicCreatorConfig holds configuration for creating Kafka topics.
type TopicCreatorConfig struct {
	// PartitionCount is the number of partitions to assign to
	// newly created topics.
	//
	// Must be non-zero. If PartitonCount is -1, the broker's
	// default value (requires Kafka 2.4+).
	PartitionCount int

	// TopicConfigs holds any topic configs to assign to newly
	// created topics, such as `retention.ms`.
	//
	// See https://kafka.apache.org/documentation/#topicconfigs
	TopicConfigs map[string]string

	// MeterProvider used to create meters and record metrics (Optional).
	MeterProvider metric.MeterProvider
}

// Validate ensures the configuration is valid, returning an error otherwise.
func (cfg TopicCreatorConfig) Validate() error {
	var errs []error
	if cfg.PartitionCount == 0 {
		errs = append(errs, errors.New("kafka: partition count must be non-zero"))
	}
	return errors.Join(errs...)
}

// TopicCreator creates Kafka topics.
type TopicCreator struct {
	m                *Manager
	partitionCount   int
	origTopicConfigs map[string]string
	topicConfigs     map[string]*string
	created          metric.Int64Counter
}

// NewTopicCreator returns a new TopicCreator with the given config.
func (m *Manager) NewTopicCreator(cfg TopicCreatorConfig) (*TopicCreator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka: invalid topic creator config: %w", err)
	}
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	meter := cfg.MeterProvider.Meter("github.com/elastic/apm-queue/kafka")
	created, err := meter.Int64Counter("topics.created.count",
		metric.WithDescription("The number of created topics"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed creating 'topics.created.count' metric: %w", err)
	}
	var topicConfigs map[string]*string
	if len(cfg.TopicConfigs) != 0 {
		topicConfigs = make(map[string]*string, len(cfg.TopicConfigs))
		for k, v := range cfg.TopicConfigs {
			topicConfigs[k] = kadm.StringPtr(v)
		}
	}
	return &TopicCreator{
		m:                m,
		partitionCount:   cfg.PartitionCount,
		origTopicConfigs: cfg.TopicConfigs,
		topicConfigs:     topicConfigs,
		created:          created,
	}, nil
}

// CreateTopics creates one or more topics.
//
// Topics that already exist will be updated.
func (c *TopicCreator) CreateTopics(ctx context.Context, topics ...apmqueue.Topic) error {
	ctx, span := c.m.tracer.Start(
		ctx,
		"CreateTopics",
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("kafka"),
		),
	)
	defer span.End()

	namespacePrefix := c.m.cfg.namespacePrefix()
	topicNames := make([]string, len(topics))
	for i, topic := range topics {
		topicNames[i] = fmt.Sprintf("%s%s", namespacePrefix, topic)
	}

	existing, err := c.m.adminClient.ListTopics(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to list kafka topics: %w", err)
	}

	missingTopics, updatePartitions, existingTopics := c.categorizeTopics(existing, topicNames)

	var updateErrors []error
	if err := c.createMissingTopics(ctx, span, missingTopics); err != nil {
		updateErrors = append(updateErrors, err)
	}

	if err := c.updateTopicPartitions(ctx, span, updatePartitions); err != nil {
		updateErrors = append(updateErrors, err)
	}

	if err := c.alterTopicConfigs(ctx, span, existingTopics); err != nil {
		updateErrors = append(updateErrors, err)
	}

	return errors.Join(updateErrors...)
}

func (c *TopicCreator) categorizeTopics(
	existing kadm.TopicDetails,
	topicNames []string,
) (missingTopics, updatePartitions, existingTopics []string) {
	missingTopics = make([]string, 0, len(topicNames))
	updatePartitions = make([]string, 0, len(topicNames))
	existingTopics = make([]string, 0, len(topicNames))

	for _, wantTopic := range topicNames {
		if !existing.Has(wantTopic) {
			missingTopics = append(missingTopics, wantTopic)
			continue
		}
		existingTopics = append(existingTopics, wantTopic)
		if len(existing[wantTopic].Partitions) < c.partitionCount {
			updatePartitions = append(updatePartitions, wantTopic)
		}
	}

	return missingTopics, updatePartitions, existingTopics
}

func (c *TopicCreator) createMissingTopics(
	ctx context.Context,
	span trace.Span,
	missingTopics []string,
) error {
	if len(missingTopics) == 0 {
		return nil
	}

	responses, err := c.m.adminClient.CreateTopics(ctx,
		int32(c.partitionCount),
		-1, // default.replication.factor
		c.topicConfigs,
		missingTopics...,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to create kafka topics: %w", err)
	}

	namespacePrefix := c.m.cfg.namespacePrefix()

	loggerFields := []zap.Field{
		zap.Int("partition_count", c.partitionCount),
	}
	if len(c.origTopicConfigs) > 0 {
		loggerFields = append(loggerFields,
			zap.Reflect("topic_configs", c.origTopicConfigs),
		)
	}

	var updateErrors []error
	for _, response := range responses.Sorted() {
		topicName := strings.TrimPrefix(response.Topic, namespacePrefix)

		logger := c.m.cfg.Logger.With(loggerFields...)
		if c.m.cfg.TopicLogFieldsFunc != nil {
			logger = logger.With(c.m.cfg.TopicLogFieldsFunc(topicName)...)
		}

		if err := response.Err; err != nil {
			if errors.Is(err, kerr.TopicAlreadyExists) {
				logger.Debug("kafka topic already exists",
					zap.String("topic", topicName),
				)
				span.AddEvent("kafka topic already exists", trace.WithAttributes(
					semconv.MessagingDestinationKey.String(topicName),
				))
			} else {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				updateErrors = append(updateErrors, fmt.Errorf(
					"failed to create topic %q: %w", topicName, err,
				))
				c.created.Add(context.Background(), 1, metric.WithAttributeSet(
					attribute.NewSet(
						semconv.MessagingSystemKey.String("kafka"),
						attribute.String("outcome", "failure"),
						attribute.String("topic", topicName),
					),
				))
			}
			continue
		}

		c.created.Add(context.Background(), 1, metric.WithAttributeSet(
			attribute.NewSet(
				semconv.MessagingSystemKey.String("kafka"),
				attribute.String("outcome", "success"),
				attribute.String("topic", topicName),
			),
		))

		logger.Info("created kafka topic", zap.String("topic", topicName))
	}

	if len(updateErrors) > 0 {
		return errors.Join(updateErrors...)
	}

	return nil
}

func (c *TopicCreator) updateTopicPartitions(
	ctx context.Context,
	span trace.Span,
	updatePartitions []string,
) error {
	if len(updatePartitions) == 0 {
		return nil
	}

	updateResp, err := c.m.adminClient.UpdatePartitions(
		ctx,
		c.partitionCount,
		updatePartitions...,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf(
			"failed to update partitions for kafka topics: %v: %w",
			updatePartitions, err,
		)
	}

	namespacePrefix := c.m.cfg.namespacePrefix()

	loggerFields := []zap.Field{
		zap.Int("partition_count", c.partitionCount),
	}
	if len(c.origTopicConfigs) > 0 {
		loggerFields = append(loggerFields,
			zap.Reflect("topic_configs", c.origTopicConfigs),
		)
	}

	var updateErrors []error
	for _, response := range updateResp.Sorted() {
		topicName := strings.TrimPrefix(response.Topic, namespacePrefix)

		logger := c.m.cfg.Logger.With(loggerFields...)
		if c.m.cfg.TopicLogFieldsFunc != nil {
			logger = logger.With(c.m.cfg.TopicLogFieldsFunc(topicName)...)
		}

		if errors.Is(response.Err, kerr.InvalidRequest) {
			// If UpdatePartitions partition count isn't greater than the
			// current number of partitions, each individual response
			// returns `INVALID_REQUEST`.
			continue
		}

		if err := response.Err; err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			updateErrors = append(updateErrors, fmt.Errorf(
				"failed to update partitions for topic %q: %w",
				topicName, err,
			))
			continue
		}

		logger.Info(
			"updated partitions for kafka topic",
			zap.String("topic", topicName),
		)
	}

	if len(updateErrors) > 0 {
		return errors.Join(updateErrors...)
	}

	return nil
}

func (c *TopicCreator) alterTopicConfigs(
	ctx context.Context,
	span trace.Span,
	existingTopics []string,
) error {
	if len(existingTopics) == 0 || len(c.topicConfigs) == 0 {
		return nil
	}

	// Remove cleanup.policy if it exists,
	// since this field cannot be altered.
	delete(c.topicConfigs, "cleanup.policy")

	alterCfg := make([]kadm.AlterConfig, 0, len(c.topicConfigs))
	for k, v := range c.topicConfigs {
		alterCfg = append(alterCfg, kadm.AlterConfig{Name: k, Value: v})
	}

	alterResp, err := c.m.adminClient.AlterTopicConfigs(ctx, alterCfg, existingTopics...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf(
			"failed to update configuration for kafka topics: %v: %w",
			existingTopics, err,
		)
	}

	namespacePrefix := c.m.cfg.namespacePrefix()

	loggerFields := []zap.Field{
		zap.Int("partition_count", c.partitionCount),
	}
	if len(c.origTopicConfigs) > 0 {
		loggerFields = append(loggerFields,
			zap.Reflect("topic_configs", c.origTopicConfigs),
		)
	}

	var updateErrors []error
	for _, response := range alterResp {
		topicName := strings.TrimPrefix(response.Name, namespacePrefix)

		logger := c.m.cfg.Logger.With(loggerFields...)
		if c.m.cfg.TopicLogFieldsFunc != nil {
			logger = logger.With(c.m.cfg.TopicLogFieldsFunc(topicName)...)
		}

		if err := response.Err; err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			updateErrors = append(updateErrors, fmt.Errorf(
				"failed to alter configuration for topic %q: %w",
				topicName, err,
			))
			continue
		}

		logger.Info(
			"altered configuration for kafka topic",
			zap.String("topic", topicName),
		)
	}

	if len(updateErrors) > 0 {
		return errors.Join(updateErrors...)
	}

	return nil
}
