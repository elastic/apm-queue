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
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/queuecontext"
)

// Decoder decodes a []byte into a model.APMEvent
type Decoder interface {
	// Decode decodes an encoded model.APM Event into its struct form.
	Decode([]byte, *model.APMEvent) error
}

// ConsumerConfig defines the configuration for the Kafka consumer.
type ConsumerConfig struct {
	// Brokers is the list of kafka brokers used to seed the Kafka client.
	Brokers []string
	// Topics that the consumer will consume messages from
	Topics []string
	// GroupID to join as part of the consumer group.
	GroupID string
	// ClientID to use when connecting to Kafka. This is used for logging
	// and client identification purposes.
	ClientID string
	// Version is the software version to use in the Kafka client. This is
	// useful since it shows up in Kafka metrics and logs.
	Version string
	// Decoder holds an encoding.Decoder for decoding events.
	Decoder Decoder
	// MaxPollRecords defines an upper bound to the number of records that can
	// be polled on a single fetch. If MaxPollRecords <= 0, defaults to 100.
	//
	// It is best to keep the number of polled records small or the consumer
	// risks being forced out of the group if it exceeds rebalance.timeout.ms.
	MaxPollRecords int
	// Delivery mechanism to use to acknowledge the messages.
	// AtMostOnceDeliveryType and AtLeastOnceDeliveryType are supported.
	// If not set, it defaults to apmqueue.AtMostOnceDeliveryType.
	Delivery apmqueue.DeliveryType
	// Logger to use for any errors.
	Logger *zap.Logger
	// Processor that will be used to process each event individually.
	// It is recommended to keep the synchronous processing fast and below the
	// rebalance.timeout.ms setting in Kafka.
	//
	// The processing time of each processing cycle can be calculated as:
	// record.process.time * MaxPollRecords.
	Processor model.BatchProcessor
}

// Validate ensures the configuration is valid, otherwise, returns an error.
func (cfg ConsumerConfig) Validate() error {
	var errs []error
	if len(cfg.Brokers) == 0 {
		errs = append(errs, errors.New("kafka: at least one broker must be set"))
	}
	if len(cfg.Topics) == 0 {
		errs = append(errs, errors.New("kafka: at least one topic must be set"))
	}
	if cfg.GroupID == "" {
		errs = append(errs, errors.New("kafka: consumer GroupID must be set"))
	}
	if cfg.Decoder == nil {
		errs = append(errs, errors.New("kafka: decoder must be set"))
	}
	if cfg.Logger == nil {
		errs = append(errs, errors.New("kafka: logger must be set"))
	}
	if cfg.Processor == nil {
		errs = append(errs, errors.New("kafka: processor must be set"))
	}
	return errors.Join(errs...)
}

// Consumer wraps a Kafka consumer and the consumption implementation details.
type Consumer struct {
	mu     sync.RWMutex
	client *kgo.Client
	cfg    ConsumerConfig
}

// NewConsumer creates a new instance of a Consumer.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka: invalid consumer config: %w", err)
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.WithLogger(kzap.New(cfg.Logger)),
		// If a rebalance happens while the client is polling, the consumed
		// records may belong to a partition which has been reassigned to a
		// different consumer int he group. To avoid this scenario, Polls will
		// block rebalances of partitions which would be lost, and the consumer
		// MUST manually call `AllowRebalance`.
		kgo.BlockRebalanceOnPoll(),
		kgo.DisableAutoCommit(),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
		if cfg.Version != "" {
			opts = append(opts, kgo.SoftwareNameAndVersion(
				cfg.ClientID, cfg.Version,
			))
		}
	}
	if cfg.MaxPollRecords <= 0 {
		cfg.MaxPollRecords = 100
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed creating kafka consumer: %w", err)
	}
	// Issue a metadata refresh request on construction, so the broker list is
	// populated.
	client.ForceMetadataRefresh()
	consumer := Consumer{
		cfg:    cfg,
		client: client,
	}
	return &consumer, nil
}

// Close closes the consumer.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Since we take the full lock when closing, there's no need to explicitly
	// allow rebalances since polls aren't concurrent with Close().
	c.client.Close()
	return nil
}

// Run executes the consumer in a blocking manner.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		if err := c.fetch(ctx); err != nil {
			return err
		}
	}
}

// fetch polls the Kafka broker for new records up to cfg.MaxPollRecords.
// Any errors returned by fetch should be considered fatal and
func (c *Consumer) fetch(ctx context.Context) error {
	// NOTE(marclop) this is pretty naive consuming, to maximize throughput,
	// it's best to use one goroutine per partition, but that requires more
	// state management and blocking when rebalances happen.
	c.mu.RLock()
	defer c.mu.RUnlock()

	fetches := c.client.PollRecords(ctx, c.cfg.MaxPollRecords)
	defer c.client.AllowRebalance()

	if fetches.IsClientClosed() {
		return fmt.Errorf("client is closed: %w", context.Canceled)
	}
	if errors.Is(fetches.Err0(), context.Canceled) {
		return fmt.Errorf("context canceled: %w", fetches.Err0())
	}
	switch c.cfg.Delivery {
	case apmqueue.AtLeastOnceDeliveryType:
		// Commit the fetched record offsets after we've processed them.
		defer c.client.CommitUncommittedOffsets(ctx)
	case apmqueue.AtMostOnceDeliveryType:
		// Commit the fetched record offsets as soon as we've polled them.
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			// If the commit fails, then return immediately and any uncommitted
			// records will be re-delivered in time. Otherwise, records may be
			// processed twice.
			return nil
		}
		// Allow rebalancing now that we have committed offsets, preventing
		// another consumer from reprocessing the records.
		c.client.AllowRebalance()
	}
	fetches.EachError(func(t string, p int32, err error) {
		c.cfg.Logger.Error("consumer fetches returned error",
			zap.Error(err), zap.String("topic", t), zap.Int32("partition", p),
		)
	})
	fetches.EachRecord(func(msg *kgo.Record) {
		var event model.APMEvent
		meta := make(map[string]string)
		for _, h := range msg.Headers {
			meta[h.Key] = string(h.Value)
		}
		if err := c.cfg.Decoder.Decode(msg.Value, &event); err != nil {
			// TODO(marclop) DLQ?
			c.cfg.Logger.Error("unable to decode message.Value into model.APMEvent",
				zap.Error(err),
				zap.String("topic", msg.Topic),
				zap.ByteString("message.value", msg.Value),
				zap.Int64("offset", msg.Offset),
				zap.Int32("partition", msg.Partition),
				zap.Any("headers", meta),
			)
			return
		}
		ctx = queuecontext.WithMetadata(context.Background(), meta)
		batch := model.Batch{event}
		if err := c.cfg.Processor.ProcessBatch(ctx, &batch); err != nil {
			c.cfg.Logger.Error("unable to process event",
				zap.Error(err),
				zap.String("topic", msg.Topic),
				zap.ByteString("message.value", msg.Value),
				zap.Int64("offset", msg.Offset),
				zap.Int32("partition", msg.Partition),
				zap.Any("headers", meta),
			)
			return
		}
	})
	return nil
}

// Healthy returns an error if the Kafka client fails to reach a discovered
// broker.
func (c *Consumer) Healthy() error {
	if err := c.client.Ping(context.Background()); err != nil {
		return fmt.Errorf("health probe: %w", err)
	}
	return nil
}
