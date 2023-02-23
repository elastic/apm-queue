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
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/pscompat"
	"github.com/elastic/apm-data/model"
	"github.com/elastic/apm-queue/queuecontext"
	"go.uber.org/zap"
	"google.golang.org/api/option"
)

// ConsumerConfig defines the configuration for the Kafka consumer.
type ConsumerConfig struct {
	// Project where the topic is located.
	Project string
	// Region where the topic is located.
	Region string
	// SubscriptionID to join as part of the subscriber group.
	SubscriptionID string

	// Logger to use for any errors.
	Logger *zap.Logger
	// Processor that will be used to process each event individually.
	Processor  model.BatchProcessor
	ClientOpts []option.ClientOption
}

// Validate ensures the configuration is valid, otherwise, returns an error.
func (cfg ConsumerConfig) Validate() error {
	var errs []error
	if cfg.SubscriptionID == "" {
		errs = append(errs, errors.New("pubsublite: subscriptionID must be set"))
	}
	if cfg.Project == "" {
		errs = append(errs, errors.New("pubsublite: project must be set"))
	}
	if cfg.Region == "" {
		errs = append(errs, errors.New("pubsublite: region must be set"))
	}
	if cfg.Logger == nil {
		errs = append(errs, errors.New("pubsublite: logger must be set"))
	}
	if cfg.Processor == nil {
		errs = append(errs, errors.New("pubsublite: processor must be set"))
	}
	return errors.Join(errs...)
}

// Consumer receives PubSub Lite messages from an existing subscription. The
// underlying library processes messages concurrently for each partition.
type Consumer struct {
	mu             sync.RWMutex
	cfg            ConsumerConfig
	consumer       *pscompat.SubscriberClient
	stopSubscriber context.CancelFunc
}

// NewConsumer creates a new consumer instance for a single subscription.
func NewConsumer(ctx context.Context, cfg ConsumerConfig) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	topic := fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s",
		cfg.Project, cfg.Region, cfg.SubscriptionID,
	)
	consumer, err := pscompat.NewSubscriberClient(ctx, topic, cfg.ClientOpts...)
	if err != nil {
		return nil, err
	}
	cfg.Logger = cfg.Logger.With(zap.String("subscription", cfg.SubscriptionID))
	return &Consumer{
		cfg:      cfg,
		consumer: consumer,
	}, nil
}

// Close closes the consumer. Once the consumer is closed, it can't be re-used.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopSubscriber()
	return nil
}

// Run executes the consumer in a blocking manner.
func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.stopSubscriber != nil {
		c.mu.Unlock()
		return errors.New("consumer already started")
	}
	ctx, c.stopSubscriber = context.WithCancel(ctx)
	c.mu.Unlock()
	return c.consumer.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		projectID := msg.Attributes["project_id"]
		ctx = queuecontext.WithProject(ctx, projectID)
		var event model.APMEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			defer msg.Nack()
			meta, _ := pscompat.ParseMessageMetadata(msg.ID)
			c.cfg.Logger.Error("unable to unmarshal json into model.APMEvent",
				zap.Error(err),
				zap.ByteString("message.value", msg.Data),
				zap.Int64("offset", meta.Offset),
				zap.Int32("partition", int32(meta.Partition)),
				zap.String("project_id", projectID),
			)
			return
		}
		batch := model.Batch{event}
		if err := c.cfg.Processor.ProcessBatch(ctx, &batch); err != nil {
			defer msg.Nack()
			meta, _ := pscompat.ParseMessageMetadata(msg.ID)
			c.cfg.Logger.Error("unable to process event",
				zap.Error(err),
				zap.Int64("offset", meta.Offset),
				zap.Int32("partition", int32(meta.Partition)),
				zap.String("project_id", projectID),
			)
			return
		}
		msg.Ack()
	})
}

// Healthy returns an error if the consumer isn't healthy.
func (c *Consumer) Healthy() error {
	return nil // TODO(marclop)
}
