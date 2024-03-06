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

package pubsublite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsublite"
	"github.com/googleapis/gax-go/v2/apierror"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue/v2"
)

// TopicCreatorConfig holds configuration for managing GCP Pub/Sub Lite topics.
type TopicCreatorConfig struct {
	// Reservation holds the unqualified ID of the reservation with
	// which topics will be associated. This will be combined with the
	// project ID, region ID, and namespace to form the reservation path.
	Reservation string

	// PartitionCount is the number of partitions to assign to newly
	// created topics.
	//
	// Must be greater than zero.
	PartitionCount int

	// PublishCapacityMiBPerSec defines the publish throughput capacity
	// per partition in MiB/s. Must be >= 4 and <= 16.
	PublishCapacityMiBPerSec int

	// SubscribeCapacityMiBPerSec defines the subscribe throughput capacity
	// per partition in MiB/s. Must be >= 4 and <= 32.
	SubscribeCapacityMiBPerSec int

	// PerPartitionBytes holds the provisioned storage, in bytes, per partition.
	//
	// If the number of bytes stored in any of the topic's partitions grows beyond
	// this value, older messages will be dropped to make room for newer ones,
	// regardless of the value of `RetentionDuration`. Must be >= 30 GiB.
	PerPartitionBytes int64

	// RetentionDuration indicates how long messages are retained. Must be > 0.
	RetentionDuration time.Duration
}

// validate checks that cfg is valid, and returns an error otherwise.
func (cfg TopicCreatorConfig) validate() error {
	var errs []error
	if cfg.Reservation == "" {
		errs = append(errs, errors.New("reservation must be set"))
	}
	if cfg.PartitionCount <= 0 {
		errs = append(errs, errors.New("partition count must be greater than zero"))
	}
	if cfg.PublishCapacityMiBPerSec < 4 || cfg.PublishCapacityMiBPerSec > 16 {
		errs = append(errs, errors.New("publish capacity must between 4 and 16, inclusive"))
	}
	if cfg.SubscribeCapacityMiBPerSec < 4 || cfg.SubscribeCapacityMiBPerSec > 32 {
		errs = append(errs, errors.New("subscribe capacity must between 4 and 32, inclusive"))
	}
	if cfg.PerPartitionBytes < 30*1024*1024*1024 {
		errs = append(errs, errors.New("per-partition bytes must be at least 30GiB"))
	}
	if cfg.RetentionDuration <= 0 {
		errs = append(errs, errors.New("retention duration must be greater than zero"))
	}
	return errors.Join(errs...)
}

// TopicCreator creates GCP Pub/Sub topics.
type TopicCreator struct {
	m   *Manager
	cfg TopicCreatorConfig
}

// NewTopicCreator returns a new TopicCreator with the given config.
func (m *Manager) NewTopicCreator(cfg TopicCreatorConfig) (*TopicCreator, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("pubsublite: invalid topic creator config: %w", err)
	}
	return &TopicCreator{m: m, cfg: cfg}, nil
}

// CreateTopics creates one or more topics.
//
// Topics that already exist will be left unmodified.
func (c *TopicCreator) CreateTopics(ctx context.Context, topics ...apmqueue.Topic) error {
	errch := make(chan error, len(topics))
	var wg sync.WaitGroup
	for _, topic := range topics {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			if err := c.createTopic(ctx, topic); err != nil {
				errch <- fmt.Errorf("failed to create pubsublite topic %q: %w", topic, err)
			}
		}(string(topic))
	}
	wg.Wait()
	close(errch)
	var errs []error
	for err := range errch {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (c *TopicCreator) createTopic(ctx context.Context, name string) error {
	namespacePrefix := c.m.cfg.namespacePrefix()
	logger := c.m.cfg.Logger.With(zap.String("topic", name))
	_, err := c.m.client.CreateTopic(ctx, pubsublite.TopicConfig{
		Name: fmt.Sprintf(
			"projects/%s/locations/%s/topics/%s%s",
			c.m.cfg.Project, c.m.cfg.Region, namespacePrefix, name,
		),
		ThroughputReservation: fmt.Sprintf(
			"projects/%s/locations/%s/reservations/%s%s",
			c.m.cfg.Project, c.m.cfg.Region, namespacePrefix, c.cfg.Reservation,
		),
		PartitionCount:             c.cfg.PartitionCount,
		PublishCapacityMiBPerSec:   c.cfg.PublishCapacityMiBPerSec,
		SubscribeCapacityMiBPerSec: c.cfg.SubscribeCapacityMiBPerSec,
		PerPartitionBytes:          c.cfg.PerPartitionBytes,
		RetentionDuration:          c.cfg.RetentionDuration,
	})
	if err != nil {
		if err, ok := apierror.FromError(err); ok && err.Reason() == "RESOURCE_ALREADY_EXISTS" {
			logger.Debug("pubsublite topic already exists")
			return nil
		}
		return fmt.Errorf("cannot create pubsublite topic: %w", err)
	}
	logger.Info(
		"created pubsublite topic",
		zap.String("reservation", c.cfg.Reservation),
		zap.Int("partition_count", c.cfg.PartitionCount),
		zap.Int64("per_partition_bytes", c.cfg.PerPartitionBytes),
		zap.Int("publish_capacity", c.cfg.PublishCapacityMiBPerSec),
		zap.Int("subscribe_capacity", c.cfg.SubscribeCapacityMiBPerSec),
		zap.Duration("retention_duration", c.cfg.RetentionDuration),
	)
	return nil
}
