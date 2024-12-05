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
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	apmqueue "github.com/elastic/apm-queue/v2"
	"github.com/elastic/apm-queue/v2/queuecontext"
)

var (
	// ErrCommitFailed may be returned by `consumer.Run` when DeliveryType is
	// apmqueue.AtMostOnceDelivery.
	ErrCommitFailed = errors.New("kafka: failed to commit offsets")
)

// ConsumerConfig defines the configuration for the Kafka consumer.
type ConsumerConfig struct {
	CommonConfig
	// Topics that the consumer will consume messages from
	Topics []apmqueue.Topic
	// ConsumeRegex sets the client to parse all topics passed to ConsumeTopics
	// as regular expressions.
	ConsumeRegex bool
	// GroupID to join as part of the consumer group.
	GroupID string
	// MaxPollRecords defines an upper bound to the number of records that can
	// be polled on a single fetch. If MaxPollRecords <= 0, defaults to 500.
	// Note that this setting doesn't change how `franz-go` fetches and buffers
	// events from Kafka brokers, it merely affects the number of records that
	// are returned on `client.PollRecords`.
	// The higher this setting, the higher the general processing throughput
	// be. However, when Delivery is set to AtMostOnce, the higher this number,
	// the more events lost if the process crashes or terminates abruptly.
	//
	// It is best to keep the number of polled records small or the consumer
	// risks being forced out of the group if it exceeds rebalance.timeout.ms.
	// Default: 500
	// Kafka consumer setting: max.poll.records
	// Docs: https://kafka.apache.org/28/documentation.html#consumerconfigs_max.poll.records
	MaxPollRecords int
	// MaxPollWait defines the maximum amount of time a broker will wait for a
	// fetch response to hit the minimum number of required bytes before
	// returning
	// Default: 5s
	// Kafka consumer setting: fetch.max.wait.ms
	// Docs: https://kafka.apache.org/28/documentation.html#consumerconfigs_fetch.max.wait.ms
	MaxPollWait time.Duration
	// // MaxConcurrentFetches sets the maximum number of fetch requests to allow in
	// flight or buffered at once, overriding the unbounded (i.e. number of
	// brokers) default.
	// This setting, paired with FetchMaxBytes, can upper bound the maximum amount
	// of memory that the client can use for consuming.
	// Default: Unbounded, total number of brokers.
	// Docs: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#MaxConcurrentFetches
	MaxConcurrentFetches int
	// MaxPollBytes sets the maximum amount of bytes a broker will try to send
	// during a fetch
	// Default: 52428800 bytes (~52MB, 50MiB)
	// Kafka consumer setting: fetch.max.bytes
	// Docs: https://kafka.apache.org/28/documentation.html#brokerconfigs_fetch.max.bytes
	MaxPollBytes int32
	// MaxPollPartitionBytes sets the maximum amount of bytes that will be consumed for
	// a single partition in a fetch request
	// Default: 1048576 bytes (~1MB, 1MiB)
	// Kafka consumer setting: max.partition.fetch.bytes
	// Docs: https://kafka.apache.org/28/documentation.html#consumerconfigs_max.partition.fetch.bytes
	MaxPollPartitionBytes int32
	// ShutdownGracePeriod defines the maximum amount of time to wait for the
	// partition consumers to process events before the underlying kgo.Client
	// is closed, overriding the default 5s.
	ShutdownGracePeriod time.Duration
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
	Processor apmqueue.Processor
	// FetchMinBytes sets the minimum amount of bytes a broker will try to send
	// during a fetch, overriding the default 1 byte.
	// Default: 1
	// Kafka consumer setting: fetch.min.bytes
	// Docs: https://kafka.apache.org/28/documentation.html#consumerconfigs_fetch.min.bytes
	FetchMinBytes int32

	// BrokerMaxReadBytes sets the maximum response size that can be read from
	// Kafka, overriding the default 100MiB.
	BrokerMaxReadBytes int32

	// ConsumePreferringLagFn alters the order in which partitions are consumed.
	// Use with caution, as this can lead to uneven consumption of partitions,
	// and in the worst case scenario, in partitions starved out from being consumed.
	PreferLagFn kgo.PreferLagFn
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
	if cfg.Processor == nil {
		errs = append(errs, errors.New("kafka: processor must be set"))
	}
	if cfg.MaxPollBytes < 0 {
		errs = append(errs, errors.New("kafka: max poll bytes cannot be negative"))
	}
	if cfg.MaxPollPartitionBytes < 0 {
		errs = append(errs, errors.New("kafka: max poll partition bytes cannot be negative"))
	}
	if cfg.FetchMinBytes < 0 {
		errs = append(errs, errors.New("kafka: fetch min bytes cannot be negative"))
	}
	if cfg.MaxPollBytes > 0 {
		// math.MaxInt32 is 1<<31-1.
		if cfg.MaxPollBytes > 1<<30 {
			cfg.MaxPollBytes = 1 << 30
		}
		if cfg.BrokerMaxReadBytes == 0 {
			cfg.BrokerMaxReadBytes = cfg.MaxPollBytes * 2
		}
	}
	if cfg.BrokerMaxReadBytes < 0 || cfg.BrokerMaxReadBytes > 1<<30 {
		cfg.BrokerMaxReadBytes = 1 << 30
	}
	return errors.Join(errs...)
}

var _ apmqueue.Consumer = &Consumer{}

// Consumer wraps a Kafka consumer and the consumption implementation details.
// Consumes each partition in a dedicated goroutine.
type Consumer struct {
	mu       sync.RWMutex
	client   *kgo.Client
	cfg      ConsumerConfig
	consumer *consumer
	running  chan struct{}
	closed   chan struct{}

	forceClose context.CancelCauseFunc
	stopPoll   context.CancelFunc

	tracer trace.Tracer
}

// NewConsumer creates a new instance of a Consumer. The consumer will read from
// each partition concurrently by using a dedicated goroutine per partition.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if err := cfg.finalize(); err != nil {
		return nil, fmt.Errorf("kafka: invalid consumer config: %w", err)
	}
	// `forceClose` is called by `Consumer.Close()` if / when the
	// `cfg.ShutdownGracePeriod` is exceeded.
	processingCtx, forceClose := context.WithCancelCause(context.Background())
	namespacePrefix := cfg.namespacePrefix()
	consumer := &consumer{
		topicPrefix: namespacePrefix,
		logFieldFn:  cfg.TopicLogFieldFunc,
		assignments: make(map[topicPartition]*pc),
		processor:   cfg.Processor,
		logger:      cfg.Logger.Named("partition"),
		delivery:    cfg.Delivery,
		ctx:         processingCtx,
	}
	topics := make([]string, len(cfg.Topics))
	for i, topic := range cfg.Topics {
		topics[i] = fmt.Sprintf("%s%s", consumer.topicPrefix, topic)
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
	if cfg.ConsumeRegex {
		opts = append(opts, kgo.ConsumeRegex())
	}
	if cfg.MaxPollWait > 0 {
		opts = append(opts, kgo.FetchMaxWait(cfg.MaxPollWait))
	}
	if cfg.MaxPollBytes != 0 {
		opts = append(opts, kgo.FetchMaxBytes(cfg.MaxPollBytes))
	}
	if cfg.MaxPollPartitionBytes != 0 {
		opts = append(opts, kgo.FetchMaxPartitionBytes(cfg.MaxPollPartitionBytes))
	}
	if cfg.MaxConcurrentFetches > 0 {
		opts = append(opts, kgo.MaxConcurrentFetches(cfg.MaxConcurrentFetches))
	}
	if cfg.PreferLagFn != nil {
		opts = append(opts, kgo.ConsumePreferringLagFn(cfg.PreferLagFn))
	}
	if cfg.ShutdownGracePeriod <= 0 {
		cfg.ShutdownGracePeriod = 5 * time.Second
	}
	if cfg.FetchMinBytes > 0 {
		opts = append(opts, kgo.FetchMinBytes(cfg.FetchMinBytes))
	}
	if cfg.BrokerMaxReadBytes > 0 {
		opts = append(opts, kgo.BrokerMaxReadBytes(cfg.BrokerMaxReadBytes))
	}

	client, err := cfg.newClient(cfg.TopicAttributeFunc, opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed creating kafka consumer: %w", err)
	}
	if cfg.MaxPollRecords <= 0 {
		cfg.MaxPollRecords = 500
	}
	return &Consumer{
		cfg:        cfg,
		client:     client,
		consumer:   consumer,
		closed:     make(chan struct{}),
		running:    make(chan struct{}),
		forceClose: forceClose,
		stopPoll:   func() {},
		tracer:     cfg.tracerProvider().Tracer("kafka"),
	}, nil
}

// Close the consumer, blocking until all partition consumers are stopped.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closed:
	default:
		close(c.closed)
		defer c.client.CloseAllowingRebalance() // Last, close the `kgo.Client`
		// Cancel the context used in client.PollRecords, triggering graceful
		// cancellation.
		c.stopPoll()
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			// Close all partition consumers first to ensure there aren't any
			// records being processed while the kgo.Client is being closed.
			// Also ensures that commits can be issued after the records are
			// processed when AtLeastOnceDelivery is configured.
			c.consumer.close()
		}()
		// Wait for the consumers to process any in-flight records, or cancel
		// the underlying processing context if they aren't stopped in time.
		select {
		case <-time.After(c.cfg.ShutdownGracePeriod): // Timeout
			c.forceClose(fmt.Errorf(
				"consumer: close: timeout waiting for consumers to stop (%s)",
				c.cfg.ShutdownGracePeriod.String(),
			))
		case <-stopped: // Stopped within c.cfg.ShutdownGracePeriod.
		}
	}
	return nil
}

// Run the consumer until a non recoverable error is found:
//   - ErrCommitFailed.
//
// To shut down the consumer, call consumer.Close() or cancel the context.
// Calling `consumer.Close` is advisable to ensure graceful shutdown and
// avoid any records from being lost (AMOD), or processed twice (ALOD).
// To ensure that all polled records are processed. Close() must be called,
// even when the context is canceled.
//
// If called more than once, returns `apmqueue.ErrConsumerAlreadyRunning`.
func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	select {
	case <-c.running:
		c.mu.Unlock()
		return apmqueue.ErrConsumerAlreadyRunning
	default:
		close(c.running)
	}
	// Create a new context from the passed context, used exclusively for
	// kgo.Client.* calls. c.stopFetch is called by consumer.Close() to
	// cancel this context as part of the graceful shutdown sequence.
	var clientCtx context.Context
	clientCtx, c.stopPoll = context.WithCancel(ctx)
	c.mu.Unlock()
	for {
		if err := c.fetch(clientCtx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil // Return no error if err == context.Canceled.
			}
			return fmt.Errorf("cannot fetch records: %w", err)
		}
	}
}

// fetch polls the Kafka broker for new records up to cfg.MaxPollRecords.
// Any errors returned by fetch should be considered fatal.
func (c *Consumer) fetch(ctx context.Context) error {
	fetches := c.client.PollRecords(ctx, c.cfg.MaxPollRecords)
	defer c.client.AllowRebalance()

	if fetches.IsClientClosed() ||
		errors.Is(fetches.Err0(), context.Canceled) ||
		errors.Is(fetches.Err0(), context.DeadlineExceeded) {
		return context.Canceled
	}
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
		topicName := strings.TrimPrefix(t, c.consumer.topicPrefix)
		logger := c.cfg.Logger
		if c.cfg.TopicLogFieldFunc != nil {
			logger = logger.With(c.cfg.TopicLogFieldFunc(topicName))
		}

		logger.Error(
			"consumer fetches returned error",
			zap.Error(err),
			zap.String("topic", topicName),
			zap.Int32("partition", p),
		)
	})
	c.consumer.processFetch(fetches)
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
	mu          sync.RWMutex
	topicPrefix string
	assignments map[topicPartition]*pc
	processor   apmqueue.Processor
	logger      *zap.Logger
	delivery    apmqueue.DeliveryType
	logFieldFn  TopicLogFieldFunc
	// ctx contains the graceful cancellation context that is passed to the
	// partition consumers.
	ctx context.Context
}

type topicPartition struct {
	topic     string
	partition int32
}

// assigned must be set as a kgo.OnPartitionsAssigned callback. Ensuring all
// assigned partitions to this consumer process received records.
func (c *consumer) assigned(_ context.Context, client *kgo.Client, assigned map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			t := strings.TrimPrefix(topic, c.topicPrefix)
			logger := c.logger.With(
				zap.String("topic", t),
				zap.Int32("partition", partition),
			)
			if c.logFieldFn != nil {
				logger = logger.With(c.logFieldFn(t))
			}

			pc := newPartitionConsumer(c.ctx, client, c.processor,
				c.delivery, t, logger,
			)
			c.assignments[topicPartition{topic: topic, partition: partition}] = pc
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
	var wg sync.WaitGroup
	for topic, partitions := range lost {
		for _, partition := range partitions {
			tp := topicPartition{topic: topic, partition: partition}
			if consumer, ok := c.assignments[tp]; ok {
				wg.Add(1)
				go func() {
					defer wg.Done()
					consumer.wait()
				}()
			}
			delete(c.assignments, tp)
		}
	}
	wg.Wait()
}

// close is used on initiate clean shutdown. This call blocks until all the
// partition consumers have processed their records and stopped.
//
// It holds the write lock, which cannot be acquired until the last fetch of
// records has been sent to all the partition consumers.
func (c *consumer) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	var wg sync.WaitGroup
	for tp, consumer := range c.assignments {
		delete(c.assignments, tp)
		wg.Add(1)
		go func(c *pc) {
			defer wg.Done()
			c.wait()
		}(consumer)
	}
	wg.Wait()
}

// processFetch sends the received records for a partition to the corresponding
// partition consumer. If topic/partition combination can't be found in the
// consumer map, the consumer has been closed.
//
// It holds the consumer read lock, which cannot be acquired if the consumer is
// closing.
func (c *consumer) processFetch(fetches kgo.Fetches) {
	if fetches.NumRecords() == 0 {
		return
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
		if len(ftp.Records) == 0 {
			return
		}
		consumer, ok := c.assignments[topicPartition{topic: ftp.Topic, partition: ftp.Partition}]
		if ok {
			consumer.consumeRecords(ftp)
			return
		}
		// NOTE(marclop) While possible, this is unlikely to happen given the
		// locking that's in place in the caller.
		if c.delivery == apmqueue.AtMostOnceDeliveryType {
			topicName := strings.TrimPrefix(ftp.Topic, c.topicPrefix)
			logger := c.logger
			if c.logFieldFn != nil {
				logger = logger.With(c.logFieldFn(topicName))
			}
			logger.Warn(
				"data loss: failed to send records to process after commit",
				zap.Error(errors.New(
					"attempted to process records for revoked partition",
				)),
				zap.String("topic", topicName),
				zap.Int32("partition", ftp.Partition),
				zap.Int64("offset", ftp.HighWatermark),
				zap.Int("records", len(ftp.Records)),
			)
		}
	})
}

// OnFetchRecordBuffered Implements the kgo.Hook that injects the processCtx
// context that is canceled by `Consumer.Close()`.
func (c *consumer) OnFetchRecordBuffered(r *kgo.Record) {
	if r.Context == nil {
		r.Context = c.ctx
	}
}

type pc struct {
	topic     apmqueue.Topic
	g         errgroup.Group
	logger    *zap.Logger
	delivery  apmqueue.DeliveryType
	processor apmqueue.Processor
	client    *kgo.Client
	ctx       context.Context
}

func newPartitionConsumer(ctx context.Context,
	client *kgo.Client,
	processor apmqueue.Processor,
	delivery apmqueue.DeliveryType,
	topic string,
	logger *zap.Logger,
) *pc {
	c := pc{
		topic:     apmqueue.Topic(topic),
		ctx:       ctx,
		client:    client,
		processor: processor,
		delivery:  delivery,
		logger:    logger,
	}
	// Only allow calls to processor.Process to happen serially.
	c.g.SetLimit(1)
	return &c
}

// consumeTopicPartition processes the records for a topic and partition. The
// records will be processed asynchronously.
func (c *pc) consumeRecords(ftp kgo.FetchTopicPartition) {
	c.g.Go(func() error {
		// Stores the last processed record. Default to -1 for cases where
		// only the first record is received.
		last := -1
		for i, msg := range ftp.Records {
			meta := make(map[string]string, len(msg.Headers))
			for _, h := range msg.Headers {
				meta[h.Key] = string(h.Value)
			}

			processCtx := queuecontext.WithMetadata(msg.Context, meta)
			record := apmqueue.Record{
				Topic:       c.topic,
				Partition:   msg.Partition,
				OrderingKey: msg.Key,
				Value:       msg.Value,
			}
			// If a record can't be processed, no retries are attempted and it
			// may be lost. https://github.com/elastic/apm-queue/issues/118.
			if err := c.processor.Process(processCtx, record); err != nil {
				c.logger.Error("data loss: unable to process event",
					zap.Error(err),
					zap.Int64("offset", msg.Offset),
					zap.Any("headers", meta),
				)
				switch c.delivery {
				case apmqueue.AtLeastOnceDeliveryType:
					continue
				}
			}
			last = i
		}
		// Commit the last record offset when one or more records are processed
		// and the delivery guarantee is set to AtLeastOnceDeliveryType.
		if c.delivery == apmqueue.AtLeastOnceDeliveryType && last >= 0 {
			lastRecord := ftp.Records[last]
			if err := c.client.CommitRecords(c.ctx, lastRecord); err != nil {
				c.logger.Error("unable to commit records",
					zap.Error(err),
					zap.Int64("offset", lastRecord.Offset),
				)
			} else if len(ftp.Records) > 0 {
				c.logger.Info("committed",
					zap.Int64("offset", lastRecord.Offset),
				)
			}
		}
		return nil
	})
}

// wait blocks until all the records have been processed.
func (c *pc) wait() error { return c.g.Wait() }
