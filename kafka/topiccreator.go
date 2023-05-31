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

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue"
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
}

// NewTopicCreator returns a new TopicCreator with the given config.
func (m *Manager) NewTopicCreator(cfg TopicCreatorConfig) (*TopicCreator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka: invalid topic creator config: %w", err)
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
	}, nil
}

// CreateTopics creates one or more topics.
//
// Topics that already exist will be left unmodified.
func (c *TopicCreator) CreateTopics(ctx context.Context, topics ...apmqueue.Topic) error {
	// TODO(axw) how should we record topics?
	ctx, span := c.m.tracer.Start(ctx, "CreateTopics", trace.WithAttributes(
		semconv.MessagingSystemKey.String("kafka"),
	))
	defer span.End()

	topicNames := make([]string, len(topics))
	for i, topic := range topics {
		topicNames[i] = string(topic)
	}
	responses, err := c.m.client.CreateTopics(
		ctx,
		int32(c.partitionCount),
		-1, // default.replication.factor
		c.topicConfigs,
		topicNames...,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to create kafka topics: %w", err)
	}
	createTopicParamsFields := []zap.Field{
		zap.Int("partition_count", c.partitionCount),
	}
	if c.origTopicConfigs != nil {
		createTopicParamsFields = append(createTopicParamsFields,
			zap.Reflect("topic_configs", c.origTopicConfigs),
		)
	}
	var createErrors []error
	for _, response := range responses.Sorted() {
		logger := c.m.cfg.Logger.With(zap.String("topic", response.Topic))
		if err := response.Err; err != nil {
			if errors.Is(err, kerr.TopicAlreadyExists) {
				// NOTE(axw) apmotel currently does nothing with span events,
				// hence we log as well as create a span event.
				logger.Debug("kafka topic already exists")
				span.AddEvent("kafka topic already exists", trace.WithAttributes(
					semconv.MessagingDestinationKey.String(response.Topic),
				))
			} else {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				createErrors = append(createErrors,
					fmt.Errorf(
						"failed to create topic %q: %w",
						response.Topic, err,
					),
				)
			}
			continue
		}
		logger.Info("created kafka topic", createTopicParamsFields...)
	}
	return errors.Join(createErrors...)
}
