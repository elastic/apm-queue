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
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue"
)

var _ apmqueue.TopicCreator = (*Manager)(nil)

// ManagerConfig holds configuration for managing Kafka topics.
type ManagerConfig struct {
	CommonConfig

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

// Validate checks that cfg is valid, and returns an error otherwise.
func (cfg ManagerConfig) Validate() error {
	var errs []error
	if err := cfg.CommonConfig.Validate(); err != nil {
		errs = append(errs, err)
	}
	if cfg.PartitionCount == 0 {
		errs = append(errs, errors.New("kafka: partition count must be non-zero"))
	}
	return errors.Join(errs...)
}

// Manager manages Kafka topics.
type Manager struct {
	cfg          ManagerConfig
	topicConfigs map[string]*string
	client       *kadm.Client
	tracer       trace.Tracer
}

// NewManager returns a new Manager with the given config.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka: invalid manager config: %w", err)
	}
	var topicConfigs map[string]*string
	if len(cfg.TopicConfigs) != 0 {
		topicConfigs = make(map[string]*string, len(cfg.TopicConfigs))
		for k, v := range cfg.TopicConfigs {
			topicConfigs[k] = kadm.StringPtr(v)
		}
	}
	client, err := cfg.newClient()
	if err != nil {
		return nil, fmt.Errorf("kafka: failed creating kafka client: %w", err)
	}
	return &Manager{
		cfg:          cfg,
		topicConfigs: topicConfigs,
		client:       kadm.NewClient(client),
		tracer:       cfg.tracerProvider().Tracer("kafka"),
	}, nil
}

// Close closes the manager's resources, including its connections to the
// Kafka brokers and any associated goroutines.
func (m *Manager) Close() error {
	m.client.Close()
	return nil
}

// CreateTopics creates one or more topics.
//
// Topics that already exist will be left unmodified.
func (m *Manager) CreateTopics(ctx context.Context, topics ...apmqueue.Topic) error {
	// TODO(axw) how should we record topics?
	ctx, span := m.tracer.Start(ctx, "CreateTopics", trace.WithAttributes(
		semconv.MessagingSystemKey.String("kafka"),
	))
	defer span.End()

	topicNames := make([]string, len(topics))
	for i, topic := range topics {
		topicNames[i] = string(topic)
	}
	responses, err := m.client.CreateTopics(
		ctx,
		int32(m.cfg.PartitionCount),
		-1, // default.replication.factor
		m.topicConfigs,
		topicNames...,
	)
	if err != nil {
		return fmt.Errorf("failed to create kafka topics: %w", err)
	}
	createTopicParamsFields := []zap.Field{
		zap.Int("partition_count", m.cfg.PartitionCount),
	}
	if m.cfg.TopicConfigs != nil {
		createTopicParamsFields = append(createTopicParamsFields,
			zap.Reflect("topic_configs", m.cfg.TopicConfigs),
		)
	}
	var createErrors []error
	for _, response := range responses.Sorted() {
		logger := m.cfg.Logger.With(zap.String("topic", response.Topic))
		if err := response.Err; err != nil {
			if errors.Is(err, kerr.TopicAlreadyExists) {
				logger.Debug("kafka topic already exists")
			} else {
				span.RecordError(err)
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

// DeleteTopics deletes one or more topics.
//
// No error is returned for topics that do not exist.
func (m *Manager) DeleteTopics(ctx context.Context, topics ...apmqueue.Topic) error {
	// TODO(axw) how should we record topics?
	ctx, span := m.tracer.Start(ctx, "DeleteTopics", trace.WithAttributes(
		semconv.MessagingSystemKey.String("kafka"),
	))
	defer span.End()

	topicNames := make([]string, len(topics))
	for i, topic := range topics {
		topicNames[i] = string(topic)
	}
	responses, err := m.client.DeleteTopics(ctx, topicNames...)
	if err != nil {
		return fmt.Errorf("failed to delete kafka topics: %w", err)
	}
	var deleteErrors []error
	for _, response := range responses.Sorted() {
		logger := m.cfg.Logger.With(zap.String("topic", response.Topic))
		if err := response.Err; err != nil {
			if errors.Is(err, kerr.UnknownTopicOrPartition) {
				logger.Debug("kafka topic does not exist")
			} else {
				span.RecordError(err)
				deleteErrors = append(deleteErrors,
					fmt.Errorf(
						"failed to delete topic %q: %w",
						response.Topic, err,
					),
				)
			}
			continue
		}
		logger.Info("deleted kafka topic")
	}
	return errors.Join(deleteErrors...)

}
