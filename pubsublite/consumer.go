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
	"path"
	"sync"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/pscompat"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	apmqueue "github.com/elastic/apm-queue/v2"
	"github.com/elastic/apm-queue/v2/pubsublite/internal/telemetry"
	"github.com/elastic/apm-queue/v2/queuecontext"
)

// ConsumerConfig defines the configuration for the PubSub Lite consumer.
type ConsumerConfig struct {
	CommonConfig
	// Topics holds the list of topics from which to consume.
	Topics []apmqueue.Topic
	// ConsumerName holds the consumer name. This will be combined with
	// the topic names to identify Pub/Sub Lite subscriptions, and must
	// be unique per consuming service.
	ConsumerName string
	// Processor that will be used to process each event individually.
	// Processor may be called from multiple goroutines and needs to be
	// safe for concurrent use.
	Processor apmqueue.Processor
	// Delivery mechanism to use to acknowledge the messages.
	// AtMostOnceDeliveryType and AtLeastOnceDeliveryType are supported.
	Delivery apmqueue.DeliveryType
}

// finalize ensures the configuration is valid, setting default values from
// environment variables as described in doc comments, returning an error if
// any configuration is invalid.
func (cfg *ConsumerConfig) finalize() error {
	var errs []error
	if err := cfg.CommonConfig.finalize(); err != nil {
		errs = append(errs, err)
	}
	if len(cfg.Topics) == 0 {
		errs = append(errs, errors.New("at least one topic must be set"))
	}
	if cfg.ConsumerName == "" {
		errs = append(errs, errors.New("consumer name must be set"))
	}
	if cfg.Processor == nil {
		errs = append(errs, errors.New("processor must be set"))
	}
	switch cfg.Delivery {
	case apmqueue.AtLeastOnceDeliveryType:
	case apmqueue.AtMostOnceDeliveryType:
	default:
		errs = append(errs, errors.New("delivery is not valid"))
	}
	if len(errs) == 0 {
		cfg.Logger = cfg.Logger.With(zap.String("consumer", cfg.ConsumerName))
	}
	return errors.Join(errs...)
}

var _ apmqueue.Consumer = &Consumer{}

// Consumer receives PubSub Lite messages from a existing subscription(s). The
// underlying library processes messages concurrently per subscription and
// partition.
type Consumer struct {
	mu             sync.Mutex
	cfg            ConsumerConfig
	consumers      []*consumer
	stopSubscriber context.CancelFunc
	tracer         trace.Tracer
	metrics        telemetry.ConsumerMetrics
}

// NewConsumer creates a new consumer instance for a single subscription.
func NewConsumer(ctx context.Context, cfg ConsumerConfig) (*Consumer, error) {
	if err := cfg.finalize(); err != nil {
		return nil, fmt.Errorf("pubsublite: invalid consumer config: %w", err)
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

	commonTelemetryAttributes := []attribute.KeyValue{
		semconv.CloudRegion(cfg.Region),
		semconv.CloudAccountID(cfg.Project),
		attribute.String("consumer", cfg.ConsumerName),
	}
	var namespacePrefix string
	if cfg.Namespace != "" {
		namespacePrefix = cfg.namespacePrefix()
		commonTelemetryAttributes = append(
			commonTelemetryAttributes,
			attribute.String("namespace", cfg.Namespace),
		)
	}

	parent := fmt.Sprintf("projects/%s/locations/%s", cfg.Project, cfg.Region)
	consumers := make([]*consumer, 0, len(cfg.Topics))
	for _, topic := range cfg.Topics {
		subscriptionName := JoinTopicConsumer(topic, cfg.ConsumerName)
		subscriptionPath := path.Join(parent, "subscriptions", namespacePrefix+subscriptionName)
		client, err := pscompat.NewSubscriberClientWithSettings(
			ctx, subscriptionPath, settings, cfg.ClientOptions...,
		)
		if err != nil {
			return nil, fmt.Errorf("pubsublite: failed creating consumer: %w", err)
		}
		consumers = append(consumers, &consumer{
			SubscriberClient: client,
			delivery:         cfg.Delivery,
			processor:        cfg.Processor,
			topic:            topic,
			logger:           cfg.Logger.With(zap.String("topic", string(topic))),
			telemetryAttributes: append([]attribute.KeyValue{
				semconv.MessagingSourceName((string(topic))),
			}, commonTelemetryAttributes...),
		})
	}
	metrics, err := telemetry.NewConsumerMetrics(cfg.meterProvider())
	if err != nil {
		return nil, err
	}
	return &Consumer{
		cfg:       cfg,
		consumers: consumers,
		tracer:    cfg.tracerProvider().Tracer("pubsublite"),
		metrics:   metrics,
	}, nil
}

// Close closes the consumer. Once the consumer is closed, it can't be re-used.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopSubscriber != nil {
		c.cfg.Logger.Info("closing pubsublite consumer...")
		c.stopSubscriber()
	}
	return nil
}

// Run executes the consumer in a blocking manner. It should only be called once,
// any subsequent calls will return an error.
func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.stopSubscriber != nil {
		c.mu.Unlock()
		return apmqueue.ErrConsumerAlreadyRunning
	}
	ctx, c.stopSubscriber = context.WithCancel(ctx)
	c.mu.Unlock()

	g, ctx := errgroup.WithContext(ctx)
	for _, consumer := range c.consumers {
		consumer := consumer
		g.Go(func() error {
			receiveFunc := telemetry.Consumer(consumer.processMessage,
				c.tracer, c.metrics, string(consumer.topic),
				consumer.telemetryAttributes,
			)
			for {
				err := consumer.Receive(ctx, receiveFunc)
				// Keep attempting to receive until a fatal error is received.
				if errors.Is(err, pscompat.ErrBackendUnavailable) {
					c.cfg.Logger.Info("transient error found, re-starting consumer...", zap.Error(err))
					continue
				}
				return fmt.Errorf("cannot receive messages: %w", err)
			}
		})
	}
	return g.Wait()
}

// Healthy returns an error if the consumer isn't healthy.
func (c *Consumer) Healthy(ctx context.Context) error {
	return nil // TODO(marclop)
}

// consumer wraps a PubSub Lite SubscriberClient.
type consumer struct {
	*pscompat.SubscriberClient
	logger              *zap.Logger
	delivery            apmqueue.DeliveryType
	processor           apmqueue.Processor
	topic               apmqueue.Topic
	telemetryAttributes []attribute.KeyValue
	failed              sync.Map
}

func (c *consumer) processMessage(ctx context.Context, msg *pubsub.Message) {
	ctx = queuecontext.WithMetadata(ctx, msg.Attributes)
	var err error
	switch c.delivery {
	case apmqueue.AtMostOnceDeliveryType:
		msg.Ack() // (message may be lost on crash/error).
	case apmqueue.AtLeastOnceDeliveryType:
		defer func() {
			// If processing fails, the message will not be Nacked until the 3rd
			// delivery, otherwise, ack the message.
			if err != nil {
				attempt := int(1)
				if a, ok := c.failed.LoadOrStore(msg.ID, attempt); ok {
					attempt += a.(int)
				}
				if attempt > 2 {
					c.logger.Warn("re-processing limit exceeded, discarding event",
						zap.Int("attempt", attempt),
					)
					msg.Nack()
					c.failed.Delete(msg.ID)
					return
				}
				c.logger.Info("storing message id for re-processing",
					zap.Int("attempt", attempt),
				)
				c.failed.Store(msg.ID, attempt)
				return
			}
			msg.Ack()
			if _, found := c.failed.LoadAndDelete(msg.ID); found {
				partition, offset := partitionOffset(msg.ID)
				c.logger.Info("processed previously failed event",
					zap.Int64("offset", offset),
					zap.Int("partition", partition),
					zap.Any("headers", msg.Attributes),
				)
			}
		}()
	}
	record := apmqueue.Record{
		Topic:       c.topic,
		OrderingKey: []byte(msg.OrderingKey),
		Value:       msg.Data,
	}
	if err = c.processor.Process(ctx, record); err != nil {
		partition, offset := partitionOffset(msg.ID)
		c.logger.Error("unable to process event",
			zap.Error(err),
			zap.Int64("offset", offset),
			zap.Int("partition", partition),
			zap.Any("headers", msg.Attributes),
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
