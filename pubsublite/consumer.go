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

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/pscompat"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/queuecontext"
)

// Decoder decodes a []byte into a model.APMEvent
type Decoder interface {
	// Decode decodes an encoded model.APM Event into its struct form.
	Decode([]byte, *model.APMEvent) error
}

// ConsumerConfig defines the configuration for the PubSub Lite consumer.
type ConsumerConfig struct {
	// PubSub Lite subscriptions.
	Subscriptions []Subscription
	// Decoder holds an encoding.Decoder for decoding events.
	Decoder Decoder
	// Logger to use for any errors.
	Logger *zap.Logger
	// Processor that will be used to process each event individually.
	Processor model.BatchProcessor
	// Delivery mechanism to use to acknowledge the messages.
	// AtMostOnceDeliveryType and AtLeastOnceDeliveryType are supported.
	Delivery   apmqueue.DeliveryType
	ClientOpts []option.ClientOption
}

// Subscription represents a PubSub Lite subscription.
type Subscription struct {
	// Project where the subscription is located.
	Project string
	// Region where the subscription is located.
	Region string
	// Name/ID of the subscription.
	Name string
}

func (s Subscription) String() string {
	return fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s",
		s.Project, s.Region, s.Name,
	)
}

// Validate ensures the subscription is valid.
func (s Subscription) Validate() error {
	var errs []error
	if s.Name == "" {
		errs = append(errs, errors.New("pubsublite: subscription: name must be set"))
	}
	if s.Project == "" {
		errs = append(errs, errors.New("pubsublite: subscription: project must be set"))
	}
	if s.Region == "" {
		errs = append(errs, errors.New("pubsublite: subscription: region must be set"))
	}
	return errors.Join(errs...)
}

// Validate ensures the configuration is valid, otherwise, returns an error.
func (cfg ConsumerConfig) Validate() error {
	var errs []error
	if len(cfg.Subscriptions) == 0 {
		errs = append(errs,
			errors.New("pubsublite: at least one subscription must be set"),
		)
	}
	for i, subscription := range cfg.Subscriptions {
		if err := subscription.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("%d: %w", i, err))
		}
	}
	if cfg.Decoder == nil {
		errs = append(errs, errors.New("pubsublite: decoder must be set"))
	}
	if cfg.Logger == nil {
		errs = append(errs, errors.New("pubsublite: logger must be set"))
	}
	if cfg.Processor == nil {
		errs = append(errs, errors.New("pubsublite: processor must be set"))
	}
	switch cfg.Delivery {
	case apmqueue.AtLeastOnceDeliveryType:
	case apmqueue.AtMostOnceDeliveryType:
	default:
		errs = append(errs, errors.New("pubsublite: delivery is not valid"))
	}
	return errors.Join(errs...)
}

// Consumer receives PubSub Lite messages from a existing subscription(s). The
// underlying library processes messages concurrently per subscription and
// partition.
type Consumer struct {
	mu             sync.Mutex
	cfg            ConsumerConfig
	consumers      []consumer
	stopSubscriber context.CancelFunc
}

// NewConsumer creates a new consumer instance for a single subscription.
func NewConsumer(ctx context.Context, cfg ConsumerConfig) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	settings := pscompat.ReceiveSettings{
		// Pub/Sub Lite does not have a concept of 'nack'. If the nack handler
		// implementation returns nil, the message is acknowledged. If an error
		// is returned, it's considered a fatal error and the client terminates.
		// In Pub/Sub Lite, only a single subscriber for a given subscription
		// is connected to any partition at a time, and there is no other client
		// that may be able to handle messages.
		NackHandler: func(msg *pubsub.Message) error {
			// TODO(marclop) DLQ?
			partition, offset := partitionOffset(msg.ID)
			cfg.Logger.Error("handling nacked message",
				zap.Int("partition", partition),
				zap.Int64("offset", offset),
				zap.Any("attributes", msg.Attributes),
			)
			return nil // nil is returned to avoid terminating the subscriber.
		},
	}
	consumers := make([]consumer, 0, len(cfg.Subscriptions))
	for _, subscription := range cfg.Subscriptions {
		client, err := pscompat.NewSubscriberClientWithSettings(
			ctx, subscription.String(), settings, cfg.ClientOpts...,
		)
		if err != nil {
			return nil, err
		}
		consumers = append(consumers, consumer{
			SubscriberClient: client,
			delivery:         cfg.Delivery,
			processor:        cfg.Processor,
			decoder:          cfg.Decoder,
			logger: cfg.Logger.With(
				zap.String("subscription", subscription.Name),
				zap.String("region", subscription.Region),
				zap.String("project", subscription.Project),
			),
		})
	}
	return &Consumer{
		cfg:       cfg,
		consumers: consumers,
	}, nil
}

// Close closes the consumer. Once the consumer is closed, it can't be re-used.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopSubscriber()
	return nil
}

// Run executes the consumer in a blocking manner. It should only be called once,
// any subsequent calls will return an error.
func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.stopSubscriber != nil {
		c.mu.Unlock()
		return errors.New("pubsublite: consumer already started")
	}
	ctx, c.stopSubscriber = context.WithCancel(ctx)
	c.mu.Unlock()
	g, ctx := errgroup.WithContext(ctx)
	for _, consumer := range c.consumers {
		consumer := consumer
		g.Go(func() error {
			return consumer.Receive(ctx, consumer.processMessage)
		})
	}
	return g.Wait()
}

// Healthy returns an error if the consumer isn't healthy.
func (c *Consumer) Healthy() error {
	return nil // TODO(marclop)
}

// consumer wraps a PubSub Lite SubscriberClient.
type consumer struct {
	*pscompat.SubscriberClient
	logger    *zap.Logger
	delivery  apmqueue.DeliveryType
	processor model.BatchProcessor
	decoder   Decoder
}

func (c consumer) processMessage(ctx context.Context, msg *pubsub.Message) {
	var event model.APMEvent
	if err := c.decoder.Decode(msg.Data, &event); err != nil {
		defer msg.Nack()
		partition, offset := partitionOffset(msg.ID)
		c.logger.Error("unable to decode message.Data into model.APMEvent",
			zap.Error(err),
			zap.ByteString("message.value", msg.Data),
			zap.Int64("offset", offset),
			zap.Int("partition", partition),
		)
		return
	}
	batch := model.Batch{event}
	ctx = queuecontext.WithMetadata(ctx, msg.Attributes)
	var err error
	switch c.delivery {
	case apmqueue.AtMostOnceDeliveryType:
		msg.Ack()
	case apmqueue.AtLeastOnceDeliveryType:
		defer func() {
			if err != nil {
				msg.Nack()
			} else {
				msg.Ack()
			}
		}()
	}
	if err = c.processor.ProcessBatch(ctx, &batch); err != nil {
		partition, offset := partitionOffset(msg.ID)
		c.logger.Error("unable to process event",
			zap.Error(err),
			zap.Int64("offset", offset),
			zap.Int("partition", partition),
		)
		return
	}
}

// Parses the message partition and offset. If the metadata can't be parsed,
// zero values are returned.
func partitionOffset(id string) (partition int, offset int64) {
	if meta, _ := pscompat.ParseMessageMetadata(id); meta != nil {
		partition, offset = meta.Partition, meta.Offset
	}
	return
}
