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
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue/v2"
)

var _ apmqueue.Consumer = (*LogCompactedConsumer)(nil)

// LogCompactedConfig holds the configuration for a log compacted consumer.
type LogCompactedConfig struct {
	CommonConfig
	// Topic is the log compacted topic to consume.
	// Required.
	Topic string
	// Processor is the function that processes the fetched records. Any errors
	// are treated as non-fatal and will not stop the consumer. The rationale
	// is that log compacted topics are designed to retain only the latest record
	// for each key, so processing errors should not prevent the consumer from
	// continuing to fetch and process new records.
	Processor func(context.Context, *kgo.FetchesRecordIter) error
	// FetchMaxWait is the maximum time to wait for records to be fetched.
	FetchMaxWait time.Duration
	// MinFetchSize is the minimum number of bytes to fetch in a single request.
	// This can be used to ensure that the consumer fetches a reasonable amount
	// of data in each request.
	MinFetchSize int32
}

func (cfg *LogCompactedConfig) finalize() error {
	var errs []error
	if err := cfg.CommonConfig.finalize(); err != nil {
		errs = append(errs, err)
	}
	if cfg.Topic == "" {
		errs = append(errs, errors.New("topic must be set for log compacted consumer"))
	}
	if cfg.Processor == nil {
		errs = append(errs, errors.New("processor function must be set for log compacted consumer"))
	}
	return errors.Join(errs...)
}

// LogCompactedConsumer is a consumer for log compacted topics in Kafka.
// It continuously fetches records from the specified topic and processes them
// using the provided processor function.
//
// Log compacted topics are special Kafka topics that retain only the latest
// record for each key, allowing for efficient storage and retrieval of the most
// recent state of each key.
type LogCompactedConsumer struct {
	client  *kgo.Client
	logger  *zap.Logger
	process func(context.Context, *kgo.FetchesRecordIter) error
	topic   string
	ctx     context.Context
	cancel  context.CancelFunc

	mu      sync.RWMutex
	started chan struct{}
	stopped chan struct{}
	// delta contains the number of records pending to process to get to a
	// full sync.
	syncDelta atomic.Int64
	synced    chan struct{}
}

// NewLogCompactedConsumer creates a new log compacted consumer.
func NewLogCompactedConsumer(cfg LogCompactedConfig,
) (*LogCompactedConsumer, error) {
	if err := cfg.finalize(); err != nil {
		return nil, err
	}
	opts := []kgo.Opt{
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ConsumeTopics(cfg.Topic),
	}
	if cfg.FetchMaxWait > 0 {
		opts = append(opts, kgo.FetchMaxWait(cfg.FetchMaxWait))
	}
	if cfg.MinFetchSize > 0 {
		opts = append(opts, kgo.FetchMinBytes(cfg.MinFetchSize))
	}

	client, err := cfg.newClientWithOpts(
		[]clientOptsFn{WithTopicMultipleAttributeFunc(cfg.TopicAttributesFunc)},
		opts...,
	)
	if err != nil {
		return nil, err
	}

	lcc := LogCompactedConsumer{
		topic:   cfg.Topic,
		process: cfg.Processor,
		logger:  cfg.Logger,
		client:  client,
		started: make(chan struct{}),
		stopped: make(chan struct{}),
		synced:  make(chan struct{}),
	}
	lcc.ctx, lcc.cancel = context.WithCancel(context.Background())
	return &lcc, nil
}

// Run starts the log compacted consumer. It will run in a goroutine and
// continuously fetch records from the Kafka topic until the context the
// consumer is closed.
func (lcc *LogCompactedConsumer) Run(ctx context.Context) error {
	lcc.mu.Lock()
	defer lcc.mu.Unlock()
	select {
	case <-lcc.stopped:
		return errors.New("consumer already stopped")
	default:
	}
	select {
	case <-lcc.started:
		return nil
	default:
	}

	go func() {
		defer close(lcc.stopped)
		close(lcc.started)
		for {
			select {
			case <-lcc.ctx.Done():
				return // Exit the goroutine if the context is done.
			default:
				lcc.consume(lcc.ctx)
			}
		}
	}()
	select {
	case <-lcc.started: // Wait for the consumer goroutine to start.
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (lcc *LogCompactedConsumer) consume(ctx context.Context) {
	fetches := lcc.client.PollRecords(lcc.ctx, -1) // This means all buffered.
	if fetches.IsClientClosed() {
		lcc.logger.Info("kafka client closed, stopping fetch")
		return
	}
	if err := fetches.Err0(); errors.Is(err, context.Canceled) {
		lcc.logger.Info("context canceled, stopping fetch")
		return
	}
	fetches.EachError(func(topic string, partition int32, err error) {
		lcc.logger.Error("kafka client fetch returned errors",
			zap.Error(err),
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.Int("num_records", fetches.NumRecords()),
		)
	})
	if fetches.Empty() {
		return
	}
	lcc.mu.RLock()
	defer lcc.mu.RUnlock()

	// If the consumer hasn't yet had the first full sync, record both the
	// last offset and the high watermark for each partition.
	var hwm, lastRecord map[int32]int64
	select {
	case <-lcc.synced:
	default:
		hwm = make(map[int32]int64)
		lastRecord = make(map[int32]int64)
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			length := len(p.Records)
			if length == 0 {
				return // Skip empty partitions.
			}
			hwm[p.Partition] = p.HighWatermark
			lastRecord[p.Partition] = p.Records[length-1].Offset
		})
	}

	// NOTE(marclop): Introduce retries?
	if err := lcc.process(ctx, fetches.RecordIter()); err != nil {
		lcc.logger.Error("error processing records",
			zap.Error(err),
			zap.String("topic", lcc.topic),
			zap.Int("partitions", fetches.NumRecords()),
		)
	}
	select {
	case <-lcc.synced:
		return
	default:
	}
	// If the consumer is not yet synced, check if the current fetch would
	// complete the sync for all partitions.
	var delta int64
	allPartitionsSynced := true
	for partition, offset := range lastRecord {
		offset = offset + 1 // HWM always points to the next offset.
		if hwm[partition] != offset {
			allPartitionsSynced = false
			delta += hwm[partition] - offset
		}
	}
	lcc.syncDelta.Store(delta)
	if allPartitionsSynced {
		close(lcc.synced)
	}
}

// Close stops the consumer and closes the underlying Kafka client.
func (lcc *LogCompactedConsumer) Close() error {
	lcc.mu.Lock()
	defer lcc.mu.Unlock()
	select {
	case <-lcc.started:
	default:
		return nil
	}
	select {
	case <-lcc.stopped:
		return nil
	default:
	}
	lcc.cancel()
	lcc.client.Close()
	<-lcc.stopped
	return nil
}

// Healthy checks if the consumer is healthy by ensuring that it has had a full
// sync and that the underlying Kafka client can ping a broker in the cluster.
//
// This function can be used as a readiness probe.
func (lcc *LogCompactedConsumer) Healthy(ctx context.Context) error {
	select {
	case <-lcc.started:
	default:
		return errors.New("consumer not started yet")
	}

	select {
	case <-lcc.synced:
		return lcc.client.Ping(ctx)
	case <-ctx.Done():
		return fmt.Errorf(
			"healthy: consumer not fully synced yet: %d remaining: %w",
			lcc.syncDelta.Load(), ctx.Err(),
		)
	}
}
