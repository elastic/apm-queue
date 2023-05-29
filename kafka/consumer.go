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
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/queuecontext"
)

var (
	// ErrCommitFailed may be returned by `consumer.Run` when DeliveryType is
	// apmqueue.AtMostOnceDelivery.
	ErrCommitFailed = errors.New("kafka: failed to commit offsets")
)

// Decoder decodes a []byte into a model.APMEvent
type Decoder interface {
	// Decode decodes an encoded model.APM Event into its struct form.
	Decode([]byte, *model.APMEvent) error
}

// ConsumerConfig defines the configuration for the Kafka consumer.
type ConsumerConfig struct {
	CommonConfig
	// Topics that the consumer will consume messages from
	Topics []apmqueue.Topic
	// GroupID to join as part of the consumer group.
	GroupID string
	// Decoder holds an encoding.Decoder for decoding records.
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
	// Processor that will be used to process each event individually.
	// It is recommended to keep the synchronous processing fast and below the
	// rebalance.timeout.ms setting in Kafka.
	//
	// The processing time of each processing cycle can be calculated as:
	// record.process.time * MaxPollRecords.
	Processor model.BatchProcessor
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
		errs = append(errs, errors.New("kafka: at least one topic must be set"))
	}
	if cfg.GroupID == "" {
		errs = append(errs, errors.New("kafka: consumer GroupID must be set"))
	}
	if cfg.Decoder == nil {
		errs = append(errs, errors.New("kafka: decoder must be set"))
	}
	if cfg.Processor == nil {
		errs = append(errs, errors.New("kafka: processor must be set"))
	}
	return errors.Join(errs...)
}

// Consumer wraps a Kafka consumer and the consumption implementation details.
// Consumes each partition in a dedicated goroutine.
type Consumer struct {
	mu       sync.RWMutex
	client   *kgo.Client
	cfg      ConsumerConfig
	consumer *consumer
	running  atomic.Bool
	closed   atomic.Bool

	tracer trace.Tracer
}

// NewConsumer creates a new instance of a Consumer. The consumer will read from
// each partition concurrently by using a dedicated goroutine per partition.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if err := cfg.finalize(); err != nil {
		return nil, fmt.Errorf("kafka: invalid consumer config: %w", err)
	}
	consumer := &consumer{
		consumers: make(map[topicPartition]partitionConsumer),
		processor: cfg.Processor,
		logger:    cfg.Logger.Named("partition"),
		decoder:   cfg.Decoder,
		delivery:  cfg.Delivery,
	}
	topics := make([]string, 0, len(cfg.Topics))
	for _, t := range cfg.Topics {
		topics = append(topics, string(t))
	}
	opts := []kgo.Opt{
		// Injects the kgo.Client context as the record.Context.
		kgo.WithHooks(consumer),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(topics...),
		// If a rebalance happens while the client is polling, the consumed
		// records may belong to a partition which has been reassigned to a
		// different consumer int he group. To avoid this scenario, Polls will
		// block rebalances of partitions which would be lost, and the consumer
		// MUST manually call `AllowRebalance`.
		kgo.BlockRebalanceOnPoll(),
		kgo.DisableAutoCommit(),
		// Assign concurrent consumer callbacks to ensure consuming starts
		// for newly assigned partitions, and consuming ceases from lost or
		// revoked partitions.
		kgo.OnPartitionsAssigned(consumer.assigned),
		kgo.OnPartitionsLost(consumer.lost),
		kgo.OnPartitionsRevoked(consumer.lost),
	}
	client, err := cfg.newClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed creating kafka consumer: %w", err)
	}
	if cfg.MaxPollRecords <= 0 {
		cfg.MaxPollRecords = 100
	}
	return &Consumer{
		cfg:      cfg,
		client:   client,
		consumer: consumer,
		tracer:   cfg.tracerProvider().Tracer("kafka"),
	}, nil
}

// Close the consumer, blocking until all partition consumers are stopped.
func (c *Consumer) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.client.Close()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumer.wg.Wait() // Wait for all the goroutines to exit.
	return nil
}

// Run the consumer until a non recoverable error is found:
//   - ErrCommitFailed.
//
// To shut down the consumer, cancel the context, or call consumer.Close().
// If called more than once, returns `apmqueue.ErrConsumerAlreadyRunning`.
func (c *Consumer) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return apmqueue.ErrConsumerAlreadyRunning
	}
	for {
		if err := c.fetch(ctx); err != nil {
			// Return no error if the context is cancelled.
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}
}

// fetch polls the Kafka broker for new records up to cfg.MaxPollRecords.
// Any errors returned by fetch should be considered fatal.
func (c *Consumer) fetch(ctx context.Context) error {
	fetches := c.client.PollRecords(ctx, c.cfg.MaxPollRecords)
	defer c.client.AllowRebalance()

	if c.closed.Load() || fetches.IsClientClosed() ||
		errors.Is(fetches.Err0(), context.Canceled) ||
		errors.Is(fetches.Err0(), context.DeadlineExceeded) {
		return context.Canceled
	}
	// Acquire the lock after checking if the the context is cancelled and/or
	// the client is closed.
	c.mu.RLock()
	defer c.mu.RUnlock()
	switch c.cfg.Delivery {
	case apmqueue.AtLeastOnceDeliveryType:
		// Committing the processed records happens on each partition consumer.
	case apmqueue.AtMostOnceDeliveryType:
		// Commit the fetched record offsets as soon as we've polled them.
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			// NOTE(marclop): If the commit fails with an unrecoverable error,
			// return it and terminate the consumer. This will avoid potentially
			// processing records twice, and it's up to the consumer to re-start
			// the consumer.
			return ErrCommitFailed
		}
		// Allow re-balancing now that we have committed offsets, preventing
		// another consumer from reprocessing the records.
		c.client.AllowRebalance()
	}
	fetches.EachError(func(t string, p int32, err error) {
		c.cfg.Logger.Error("consumer fetches returned error",
			zap.Error(err), zap.String("topic", t), zap.Int32("partition", p),
		)
	})
	// Send partition records to be processed by its dedicated goroutine.
	fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
		if len(ftp.Records) == 0 {
			return
		}
		tp := topicPartition{topic: ftp.Topic, partition: ftp.Partition}
		select {
		case c.consumer.consumers[tp].records <- ftp.Records:
		// When using AtMostOnceDelivery, if the context is cancelled between
		// PollRecords and this line, records will be lost.
		// NOTE(marclop) Add a shutdown timer so that there's a grace period
		// to allow the records to be processed before they're lost.
		case <-ctx.Done():
			if c.cfg.Delivery == apmqueue.AtMostOnceDeliveryType {
				c.cfg.Logger.Warn(
					"data loss: context cancelled after records were committed",
					zap.String("topic", ftp.Topic),
					zap.Int32("partition", ftp.Partition),
					zap.Int64("offset", ftp.HighWatermark),
					zap.Int("records", len(ftp.Records)),
				)
			}
		}
	})
	return nil
}

// Healthy returns an error if the Kafka client fails to reach a discovered
// broker.
func (c *Consumer) Healthy(ctx context.Context) error {
	if err := c.client.Ping(ctx); err != nil {
		return fmt.Errorf("health probe: %w", err)
	}
	return nil
}

// consumer wraps partitionConsumers and exposes the necessary callbacks
// to use when partitions are reassigned.
type consumer struct {
	mu        sync.RWMutex
	wg        sync.WaitGroup
	consumers map[topicPartition]partitionConsumer
	processor model.BatchProcessor
	logger    *zap.Logger
	decoder   Decoder
	delivery  apmqueue.DeliveryType
	// ctx contains the `kgo.Client`'s context that is passed to the partition
	// callbacks. It is only set after partitions are assigned.
	ctx context.Context
}

type topicPartition struct {
	topic     string
	partition int32
}

// assigned must be set as a kgo.OnPartitionsAssigned callback. Ensuring all
// assigned partitions to this consumer process received records. The received
// context is only cancelled after the kgo.client is closed.
func (c *consumer) assigned(ctx context.Context, client *kgo.Client, assigned map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx == nil {
		c.ctx = ctx
	}
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			c.wg.Add(1)
			pc := partitionConsumer{
				records:   make(chan []*kgo.Record),
				processor: c.processor,
				logger:    c.logger,
				decoder:   c.decoder,
				client:    client,
				delivery:  c.delivery,
			}
			go func(topic string, partition int32) {
				defer c.wg.Done()
				pc.consume(ctx, topic, partition)
			}(topic, partition)
			tp := topicPartition{partition: partition, topic: topic}
			c.consumers[tp] = pc
		}
	}
}

// lost must be set as a kgo.OnPartitionsLost and kgo.OnPartitionsReassigned
// callbacks. Ensures that partitions that are lost (see kgo.OnPartitionsLost
// for more details) or reassigned (see kgo.OnPartitionsReassigned for more
// details) have their partition consumer stopped.
// This callback must finish within the re-balance timeout.
func (c *consumer) lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for topic, partitions := range lost {
		for _, partition := range partitions {
			tp := topicPartition{topic: topic, partition: partition}
			pc := c.consumers[tp]
			delete(c.consumers, tp)
			close(pc.records)
		}
	}
}

// Implement the kgo.Hook that allows injecting the kgo.Client context that is
// received on partition assignment.
func (c *consumer) OnFetchRecordBuffered(r *kgo.Record) {
	// Inject the context so records's context have cancellation.
	if r.Context == nil {
		c.mu.RLock()
		defer c.mu.RUnlock()
		// NOTE(marclop) Using the kgo.Client context may result in records
		// not being processed by the configured processor if the client is
		// closed while some records are processed.
		r.Context = c.ctx
	}
}

type partitionConsumer struct {
	client    *kgo.Client
	records   chan []*kgo.Record
	processor model.BatchProcessor
	logger    *zap.Logger
	decoder   Decoder
	delivery  apmqueue.DeliveryType
}

// consume processed the records from a topic and partition. Calling consume
// more than once will cause a panic.
// The received context which is only canceled when the kgo.Client is closed.
func (pc partitionConsumer) consume(ctx context.Context, topic string, partition int32) {
	logger := pc.logger.With(
		zap.String("topic", topic),
		zap.Int32("partition", partition),
	)
	for records := range pc.records {
		// Store the last processed record. Default to -1 for cases where
		// only the first record is received.
		last := -1
		for i, msg := range records {
			meta := make(map[string]string)
			for _, h := range msg.Headers {
				meta[h.Key] = string(h.Value)
			}
			var event model.APMEvent
			if err := pc.decoder.Decode(msg.Value, &event); err != nil {
				logger.Error("unable to decode message.Value into model.APMEvent",
					zap.Error(err),
					zap.ByteString("message.value", msg.Value),
					zap.Int64("offset", msg.Offset),
					zap.Any("headers", meta),
				)
				// NOTE(marclop) The decoding has failed, a DLQ may be helpful.
				continue
			}
			ctx := queuecontext.WithMetadata(msg.Context, meta)
			batch := model.Batch{event}
			// If a record can't be processed, no retries are attempted and it
			// may be lost. https://github.com/elastic/apm-queue/issues/118.
			if err := pc.processor.ProcessBatch(ctx, &batch); err != nil {
				logger.Error("data loss: unable to process event",
					zap.Error(err),
					zap.Int64("offset", msg.Offset),
					zap.Any("headers", meta),
				)
				switch pc.delivery {
				case apmqueue.AtLeastOnceDeliveryType:
					continue
				}
			}
			last = i
		}
		// Only commit the records when at least a record has been processed when
		// AtLeastOnceDeliveryType is set.
		if pc.delivery == apmqueue.AtLeastOnceDeliveryType && last >= 0 {
			lastRecord := records[last]
			if err := pc.client.CommitRecords(ctx, lastRecord); err != nil {
				logger.Error("unable to commit records",
					zap.Error(err),
					zap.Int64("offset", lastRecord.Offset),
				)
			} else if len(records) > 0 {
				logger.Info("committed",
					zap.Int64("offset", lastRecord.Offset),
				)
			}
		}
	}
}
