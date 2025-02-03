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
	"math"
	"strings"
	"sync"
	"sync/atomic"
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
	// Also note that there are potentially undesirable side effects, of keeping
	// this setting on the lower end to keep up with the rate of records:
	// - For AtMostOnceDelivery, a higher commit frequency, which can lead to
	// higher Kafka load and lower throughput. For AtLeastOnceDelivery, the
	// commits are issued at most every `CommitInterval` (or every rebalance).
	// - If incredibly low compared to the rate of records, the consumer may
	// use a more CPU, since there is more overhead per loop iteration.
	//
	// It is best to keep the number of polled records small or the consumer
	// risks being forced out of the group if it exceeds rebalance.timeout.ms.
	//
	// Default: 500 (not recommended for high throughput scenarios)
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
	//
	// If set to AtMostOnceDeliveryType, the consumer will commit the offsets
	// as soon as the records are polled from the broker.
	//
	// If set to AtLeastOnceDeliveryType, the consumer will commit the offsets
	// that are safe to commit after the records are marked as Done() by the
	// processor. The consumer uses `apmqueue.OffsetTracker` to track the offsets
	// that have been processed, and ONLY commits the offsets that are safe to
	// commit. See the `apmqueue.OffsetTracker` documentation for more details.
	//
	// For AtLeastOnceDeliveryType, consider the following scenarios:
	// - If the processor is slow, the consumer may not commit the offsets in
	//  time, records may be reprocessed by another consumer in the group after
	//  a rebalance.
	// - When implementing the processor, if the processor implementation
	// doesn't re-process failures itself, the consumer may not commit offsets
	// at all until the partition is reassigned to another consumer in the group.
	Delivery apmqueue.DeliveryType
	// Processor that will be used to process each event individually.
	// It is recommended to keep the synchronous processing fast and below the
	// rebalance.timeout.ms setting in Kafka.
	//
	// The processing time of each processing cycle can be calculated as:
	// record.process.time * MaxPollRecords.
	// Additionally, the processor MUST be safe to call concurrently.
	Processor apmqueue.Processor
	// CommitInterval defines the interval at which the consumer will commit
	// the offsets when Delivery is set to AtLeastOnceDeliveryType.
	//
	// Note that lowering this setting can lead to higher Kafka load and lower
	// throughput. It is recommended to keep this setting high enough to avoid
	// excessive commits, but low enough to avoid reprocessing records in case
	// of a crash.
	//
	// Default: time.Second
	CommitInterval time.Duration
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
	if cfg.CommitInterval <= 0 {
		cfg.CommitInterval = time.Second
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
	if cfg.BrokerMaxReadBytes < 0 {
		errs = append(errs, errors.New("kafka: broker max read bytes cannot be negative"))
	}
	if cfg.MaxPollPartitionBytes > 1<<30 {
		cfg.Logger.Info("kafka: MaxPollPartitionBytes exceeds 1GiB, setting to 1GiB")
		cfg.MaxPollPartitionBytes = 1 << 30
	}
	if cfg.BrokerMaxReadBytes > 1<<30 {
		cfg.Logger.Info("kafka: BrokerMaxReadBytes exceeds 1GiB, setting to 1GiB")
		cfg.BrokerMaxReadBytes = 1 << 30
	}
	if cfg.MaxPollBytes > 0 {
		// math.MaxInt32 is 1<<31-1.
		if cfg.MaxPollBytes > 1<<30 {
			cfg.Logger.Info("kafka: MaxPollBytes exceeds 1GiB, setting to 1GiB")
			cfg.MaxPollBytes = 1 << 30
		}
		if cfg.BrokerMaxReadBytes == 0 {
			cfg.Logger.Info("kafka: BrokerMaxReadBytes unset, setting to MaxPollBytes * 2 or 1GiB, whichever is smallest")
			cfg.BrokerMaxReadBytes = int32(math.Min(float64(cfg.MaxPollBytes)*2, 1<<30))
		}
		if cfg.BrokerMaxReadBytes > 0 && cfg.BrokerMaxReadBytes < cfg.MaxPollBytes {
			errs = append(errs, fmt.Errorf(
				"kafka: BrokerMaxReadBytes (%d) cannot be less than MaxPollBytes (%d)",
				cfg.BrokerMaxReadBytes, cfg.MaxPollBytes,
			))
		}
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
	pollCtx    context.Context
	stopPoll   context.CancelFunc

	tracer trace.Tracer
}

// NewConsumer creates a new instance of a Consumer. The consumer will call the
// processor from each partition concurrently by using a dedicated goroutine per
// topic/partition combination.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if err := cfg.finalize(); err != nil {
		return nil, fmt.Errorf("kafka: invalid consumer config: %w", err)
	}
	// `forceClose` is called by `Consumer.Close()` if / when the
	// `cfg.ShutdownGracePeriod` is exceeded.
	processingCtx, forceClose := context.WithCancelCause(context.Background())
	// Create a new context from the passed context, used exclusively for
	// kgo.Client.* calls. c.stopFetch is called by consumer.Close() to
	// cancel this context as part of the graceful shutdown sequence.
	pollCtx, stopPoll := context.WithCancel(context.Background())
	namespacePrefix := cfg.namespacePrefix()
	consumer := &consumer{
		topicPrefix:  namespacePrefix,
		logFieldFn:   cfg.TopicLogFieldFunc,
		assignments:  make(map[topicPartition]*pc),
		processor:    cfg.Processor,
		logger:       cfg.Logger.Named("partition"),
		delivery:     cfg.Delivery,
		ctx:          processingCtx,
		pollCtx:      pollCtx,
		commitOffset: make(map[topicPartition]int64),
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
		// Skip commits when partitions are lost.
		kgo.OnPartitionsLost(consumer.lost(true)),
		// Commit the last safe offset for revoked partitions on rebalance.
		kgo.OnPartitionsRevoked(consumer.lost(false)),
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
	ret := &Consumer{
		cfg:        cfg,
		client:     client,
		consumer:   consumer,
		closed:     make(chan struct{}),
		running:    make(chan struct{}),
		forceClose: forceClose,
		pollCtx:    pollCtx,
		stopPoll:   stopPoll,
		tracer:     cfg.tracerProvider().Tracer("kafka"),
	}
	if cfg.Delivery == apmqueue.AtLeastOnceDeliveryType {
		// Start the commit loop for AtLeastOnceDeliveryType.
		// Use the pollCtx explicitly to ensure that the commit loop is
		// canceled when the consumer is closed (no more fetches) coming
		// through.
		// The final commits will be handled by the `consumer.lost()` callback
		// and/or by `consumer.close()`.
		go ret.consumer.commitLoop(ret.pollCtx, client, cfg.CommitInterval)
	}
	return ret, nil
}

// Close the consumer, blocking until all partition consumers are stopped.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closed:
	default:
		ctx, cancel := context.WithTimeout(context.Background(),
			c.cfg.ShutdownGracePeriod,
		)
		defer cancel()
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
			c.consumer.close(ctx, c.client)
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
	c.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if errors.Is(err, context.Canceled) {
				return nil // Return no error if err == context.Canceled.
			}
			return fmt.Errorf("run: %w", err)
		default:
		}
		if err := c.fetch(c.pollCtx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil // Return no error if err == context.Canceled.
			}
			return fmt.Errorf("run: cannot fetch records: %w", err)
		}
	}
}

// fetch polls the Kafka broker for new records up to cfg.MaxPollRecords.
// Any errors returned by fetch should be considered fatal, except for
// context.Canceled and context.DeadlineExceeded.
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
		// Record commits are handled by the in consumer.commitLoop. Which
		// happens periodically, when the partition is reassigned or the
		// consumer is closed.
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

		logger.Error("consumer fetches returned error",
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
	// pollCtx is the context used to poll records from Kafka. It is canceled
	// when the consumer is closed.
	pollCtx context.Context
	// commitOffset stores the last committed offset for each partition.
	commitOffset map[topicPartition]int64
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
			pc.pollCtx = c.pollCtx
			c.assignments[topicPartition{topic: topic, partition: partition}] = pc
			c.commitOffset[topicPartition{topic: topic, partition: partition}] = -1
		}
	}
}

// lost must be set as a kgo.OnPartitionsLost and kgo.OnPartitionsReassigned
// callbacks. Ensures that partitions that are lost (see kgo.OnPartitionsLost
// for more details) or reassigned (see kgo.OnPartitionsReassigned for more
// details) have their partition consumer stopped.
// This callback must finish within the re-balance timeout.
// It commits the last safe offset for the lost partition on rebalance, but
// if the partition has been lost (since the consumer doesn't own it anymore).
func (c *consumer) lost(skipCommit bool) func(
	ctx context.Context, client *kgo.Client, lost map[string][]int32,
) {
	return func(ctx context.Context, client *kgo.Client, lost map[string][]int32) {
		c.mu.Lock()
		defer c.mu.Unlock()
		var wg sync.WaitGroup
		var offsets atomic.Int64
		recordChan := make(chan kgo.Record, len(lost))
		for topic, partitions := range lost {
			for _, partition := range partitions {
				tp := topicPartition{topic: topic, partition: partition}
				delete(c.assignments, tp)
				delete(c.commitOffset, tp)
				if consumer, ok := c.assignments[tp]; ok {
					last := c.commitOffset[tp]
					wg.Add(1)
					go func(pc *pc, last int64) {
						defer wg.Done()
						consumer.wait()
						if c.delivery == apmqueue.AtMostOnceDeliveryType ||
							skipCommit {
							return
						}
						if r, n := needsCommit(pc, last, tp); n > 0 {
							offsets.Add(n)
							recordChan <- r
						}
					}(consumer, last)
				}
			}
		}
		wg.Wait()
		close(recordChan)
		records := make([]*kgo.Record, 0, len(recordChan))
		for record := range recordChan {
			records = append(records, &record)
		}
		if len(records) == 0 || c.delivery == apmqueue.AtMostOnceDeliveryType {
			return
		}
		// Commit the last safe offset for the rebalanced partitions.
		c.issueCommit(ctx, client, records, zap.String("reason", "rebalance"))
	}
}

// needsCommit checks if the partition consumer safe offset is greater than the
// last committed offset for the partition. If it is, it returns that record
// and the number of offsets that will be committed with that offset.
func needsCommit(consumer *pc, last int64, tp topicPartition) (kgo.Record, int64) {
	if i, safe := consumer.tracker.SafeOffset(); safe > last {
		return kgo.Record{
			Topic:       tp.topic,
			Partition:   tp.partition,
			Offset:      safe,
			LeaderEpoch: i.Epoch,
		}, safe - last
	}
	return kgo.Record{}, 0
}

// close is used on initiate clean shutdown. This call blocks until all the
// partition consumers have processed their records and stopped.
//
// It holds the write lock, which cannot be acquired until the last fetch of
// records has been sent to all the partition consumers.
func (c *consumer) close(ctx context.Context, client *kgo.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var wg sync.WaitGroup
	var offsets atomic.Int64
	recordChan := make(chan kgo.Record, len(c.assignments))
	for tp, consumer := range c.assignments {
		last := c.commitOffset[tp]
		delete(c.assignments, tp)
		delete(c.commitOffset, tp)
		wg.Add(1)
		go func(pc *pc, last int64) {
			defer wg.Done()
			pc.wait()
			if c.delivery == apmqueue.AtMostOnceDeliveryType {
				return
			}
			if r, n := needsCommit(consumer, last, tp); n > 0 {
				offsets.Add(n)
				recordChan <- r
			}
		}(consumer, last)
	}
	wg.Wait()
	close(recordChan)
	records := make([]*kgo.Record, 0, len(recordChan))
	for record := range recordChan {
		records = append(records, &record)
	}
	// Commit the last safe offset for all partitions
	c.issueCommit(ctx, client, records, zap.String("reason", "close"))
}

func (c *consumer) issueCommit(ctx context.Context,
	client *kgo.Client,
	records []*kgo.Record,
	reason zap.Field,
) {
	if err := client.CommitRecords(ctx, records...); err != nil {
		c.logger.Error("commit failed on close",
			zap.Error(ErrCommitFailed),
			zap.Int("unique_count", len(records)),
			zap.NamedError("error.reason", err),
			reason,
		)
	} else {
		c.logger.Info("committed offsets",
			zap.Int("unique.count", len(records)),
			reason,
		)
	}
}

// processFetch sends the received records for a partition to the corresponding
// partition consumer. If topic/partition combination can't be found in the
// consumer map, the consumer has been closed.
//
// It holds the consumer read lock, which cannot be acquired if the consumer is
// closing or reassigning partitions.
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

// commitOffsets iterates over all partition consumers, collects safe offsets from each
// partition's OffsetTracker, and if the safe offset has advanced compared to commitTracker,
// issues a batched commit using client.CommitRecords.
func (c *consumer) commitOffsets(ctx context.Context, client *kgo.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	records := make([]*kgo.Record, 0, len(c.assignments))
	var offsets int64
	for tp, pc := range c.assignments {
		info, safe := pc.tracker.SafeOffset()
		if last := c.commitOffset[tp]; safe > last {
			offsets += safe - last
			records = append(records, &kgo.Record{
				Topic:       tp.topic,
				Partition:   tp.partition,
				Offset:      safe,
				LeaderEpoch: info.Epoch,
			})
		}
	}
	if len(records) == 0 {
		return
	}
	// Commit the records.
	if err := client.CommitRecords(ctx, records...); err != nil {
		c.logger.Error("commit failed on interval commit",
			zap.Error(ErrCommitFailed),
			zap.NamedError("error.reason", err),
			zap.Int("unique_count", len(records)),
			zap.Int64("offset.count", offsets),
			zap.String("reason", "interval"),
		)
	} else {
		c.logger.Info("committed offsets",
			zap.Int("unique.count", len(records)),
			zap.Int64("offset.count", offsets),
			zap.String("reason", "interval"),
		)
	}

	// Update commitTracker with new safe offsets.
	for _, rec := range records {
		tp := topicPartition{topic: rec.Topic, partition: rec.Partition}
		// Since we're using a read lock to inspect the state, we need to check
		// if the partition is still assigned. If it's not, we can't update the
		// commit offset, since the entry is stale.
		if _, ok := c.assignments[tp]; ok {
			c.commitOffset[tp] = rec.Offset
		}
	}
}

// commitLoop runs periodically at the given interval, calling commitOffsets.
func (c *consumer) commitLoop(ctx context.Context, cl *kgo.Client, interval time.Duration) {
	t := time.NewTimer(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.commitOffsets(ctx, cl)
			t.Reset(interval)
		}
	}
}

// partitionConsumer now manages offsets with offsetTracker for asynchronous commits.
type pc struct {
	topic     apmqueue.Topic
	g         errgroup.Group
	logger    *zap.Logger
	delivery  apmqueue.DeliveryType
	processor apmqueue.Processor
	client    *kgo.Client
	pollCtx   context.Context
	ctx       context.Context
	tracker   *apmqueue.WindowOffsetTracker[kgo.EpochOffset]
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
		tracker:   apmqueue.NewWindowOffsetTracker[kgo.EpochOffset](), // track offsets
	}
	// Only allow calls to processor.Process to happen serially.
	c.g.SetLimit(1)
	return &c
}

// consumeRecords registers the offset in the offset tracker and processes the
// record with the processor. If the processor returns an error, the record is
// logged as lost, since there aren't any retries.
func (c *pc) consumeRecords(ftp kgo.FetchTopicPartition) {
	c.g.Go(func() error {
		// If the context is canceled, stop processing records, since
		// the consumer is closing and the records will be reprocessed
		// by another consumer in the group on rebalance.
		select {
		case <-c.pollCtx.Done():
			return nil
		default:
		}
		for _, msg := range ftp.Records {
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
				Offset:      msg.Offset,
			}
			switch c.delivery {
			case apmqueue.AtMostOnceDeliveryType:
				record.Done = func() {}
			case apmqueue.AtLeastOnceDeliveryType:
				// Register the offset to be tracked.
				c.tracker.RegisterOffset(msg.Offset, kgo.EpochOffset{
					Offset: msg.Offset, Epoch: msg.LeaderEpoch,
				})
				// Mark the record as done when the processor calls this back.
				record.Done = func() { c.tracker.MarkDone(msg.Offset) }
			}
			if err := c.processor.Process(processCtx, record); err != nil {
				c.logger.Error("data loss: unable to process event",
					zap.Error(err),
					zap.Int64("offset", msg.Offset),
					zap.Any("headers", meta),
				)
			}
		}
		return nil
	})
}

// wait blocks until all the records have been processed by the processor.
func (c *pc) wait() error { return c.g.Wait() }
