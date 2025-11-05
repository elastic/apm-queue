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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewLogCompactedConsumer(t *testing.T) {
	_, commonCfg := newFakeCluster(t)

	testCases := map[string]struct {
		cfg LogCompactedConfig
		err error
	}{
		"empty config": {
			cfg: LogCompactedConfig{},
			err: errors.New("topic must be set for log compacted consumer"),
		},
		"missing topic": {
			cfg: LogCompactedConfig{
				CommonConfig: CommonConfig{
					Brokers: commonCfg.Brokers,
					Logger:  commonCfg.Logger,
				},
				Processor: func(context.Context, *kgo.FetchesRecordIter) error { return nil },
			},
			err: errors.New("topic must be set for log compacted consumer"),
		},
		"missing processor": {
			cfg: LogCompactedConfig{
				CommonConfig: CommonConfig{
					Brokers: commonCfg.Brokers,
					Logger:  commonCfg.Logger,
				},
				Topic: "test-topic",
			},
			err: errors.New("processor function must be set for log compacted consumer"),
		},
		"invalid brokers": {
			cfg: LogCompactedConfig{
				CommonConfig: CommonConfig{
					Brokers: []string{"localhost:invalidport"},
					Logger:  commonCfg.Logger,
				},
				Topic:     "test-topic",
				Processor: func(context.Context, *kgo.FetchesRecordIter) error { return nil },
			},
			err: errors.New("failed to create Kafka client: dial tcp: lookup localhost on"),
		},
		"valid config minimal": {
			cfg: LogCompactedConfig{
				CommonConfig: CommonConfig{
					Brokers:  commonCfg.Brokers,
					Logger:   commonCfg.Logger,
					ClientID: "test-consumer",
				},
				Topic:        "test-topic",
				FetchMaxWait: 100 * time.Millisecond,
				Processor:    func(context.Context, *kgo.FetchesRecordIter) error { return nil },
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			consumer, err := NewLogCompactedConsumer(tc.cfg)
			if tc.err != nil {
				require.Error(t, err)
				require.Nil(t, consumer)
			} else {
				require.NoError(t, err)
				require.NotNil(t, consumer)
				assert.NoError(t, consumer.Close())
			}
		})
	}
}

func newLogCompactedFakeCluster(tb testing.TB, topic string, partitions int32) *kfake.Cluster {
	tb.Helper()

	cluster, err := kfake.NewCluster()
	require.NoError(tb, err)
	tb.Cleanup(cluster.Close)

	c, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	require.NoError(tb, err)

	kadmClient := kadm.NewClient(c)
	tresp, err := kadmClient.CreateTopic(context.Background(), partitions, 1, map[string]*string{
		"cleanup.policy": kadm.StringPtr("compact"),
	}, topic)
	require.NoError(tb, err)
	require.NoError(tb, tresp.Err, tresp.ErrMessage)

	return cluster
}

func TestLogCompactedConsumer_MultipleConsumersReceiveSameMessages(t *testing.T) {
	// This is the core test: multiple consumers should see the same messages
	// from a log compacted topic, ensuring the primary purpose is working.

	topicName := "log-compacted-topic"

	// Create a cluster with a log compacted topic
	cluster := newLogCompactedFakeCluster(t, topicName, 1)

	addrs := cluster.ListenAddrs()

	// Create producer to populate the topic with test data
	producer, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	require.NoError(t, err)
	defer producer.Close()

	// Produce test records - many updates per key to test log compaction behavior
	finalValues := map[string]string{
		"key1": "value1-abc",
		"key2": "value2-edf",
		"key3": "value3-xyz",
	}
	testRecords := []*kgo.Record{
		{Topic: topicName, Key: []byte("key1"), Value: []byte("value1-v1")},
		{Topic: topicName, Key: []byte("key2"), Value: []byte("value2-v1")},
		{Topic: topicName, Key: []byte("key1"), Value: []byte("value1-v2")},
		{Topic: topicName, Key: []byte("key3"), Value: []byte("value3-v1")},
		{Topic: topicName, Key: []byte("key2"), Value: []byte("value2-v2")},
		// More updates for key1
		{Topic: topicName, Key: []byte("key1"), Value: []byte("value1-v3")},
		{Topic: topicName, Key: []byte("key1"), Value: []byte("value1-v4")},
		{Topic: topicName, Key: []byte("key1"), Value: []byte("value1-v5")},
		{Topic: topicName, Key: []byte("key1"), Value: []byte("value1-v6")},
		{Topic: topicName, Key: []byte("key1"), Value: []byte("value1-v7")},
		{Topic: topicName, Key: []byte("key1"), Value: []byte(finalValues["key1"])},
		// More updates for key2
		{Topic: topicName, Key: []byte("key2"), Value: []byte("value2-v3")},
		{Topic: topicName, Key: []byte("key2"), Value: []byte("value2-v4")},
		{Topic: topicName, Key: []byte("key2"), Value: []byte("value2-v5")},
		{Topic: topicName, Key: []byte("key2"), Value: []byte("value2-v6")},
		{Topic: topicName, Key: []byte("key2"), Value: []byte(finalValues["key2"])},
		// More updates for key3
		{Topic: topicName, Key: []byte("key3"), Value: []byte("value3-v2")},
		{Topic: topicName, Key: []byte("key3"), Value: []byte("value3-v3")},
		{Topic: topicName, Key: []byte("key3"), Value: []byte("value3-v4")},
		{Topic: topicName, Key: []byte("key3"), Value: []byte(finalValues["key3"])},
	}

	for _, record := range testRecords {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := producer.ProduceSync(ctx, record).FirstErr()
		cancel()
		require.NoError(t, err)
	}

	// Create multiple consumers
	numConsumers := 10
	consumers := make([]*LogCompactedConsumer, numConsumers)
	consumerMessages := make([]map[string]string, numConsumers)
	consumerMutexes := make([]*sync.Mutex, numConsumers)

	var processing sync.WaitGroup
	processing.Add(numConsumers)
	for i := 0; i < numConsumers; i++ {
		consumerMessages[i] = make(map[string]string)
		consumerMutexes[i] = &sync.Mutex{}

		cfg := LogCompactedConfig{
			CommonConfig: CommonConfig{
				Brokers:  addrs,
				Logger:   zapTest(t),
				ClientID: fmt.Sprintf("test-consumer-%d", i),
			},
			Topic:        topicName,
			FetchMaxWait: 500 * time.Millisecond,
			MinFetchSize: 1 << 16, // 64 KiB
			Processor: func(consumerIdx int) func(_ context.Context, iter *kgo.FetchesRecordIter) error {
				var once sync.Once
				return func(_ context.Context, iter *kgo.FetchesRecordIter) error {
					for !iter.Done() {
						record := iter.Next()
						consumerMutexes[consumerIdx].Lock()
						consumerMessages[consumerIdx][string(record.Key)] = string(record.Value)
						consumerMutexes[consumerIdx].Unlock()
					}
					once.Do(processing.Done)
					return nil
				}
			}(i),
		}

		consumer, err := NewLogCompactedConsumer(cfg)
		require.NoError(t, err)
		consumers[i] = consumer
	}

	// Start all consumers
	for i, consumer := range consumers {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, consumer.Run(ctx), "Consumer %d should start successfully", i)
	}
	processing.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i, c := range consumers {
		require.NoError(t, c.Healthy(ctx), "Consumer %v should be synced after starting", i)
		c.Close() // Stop each consumer as it's been fully synced.
	}

	// Verify all consumers received the same messages
	for i, messages := range consumerMessages {
		t.Logf("Consumer %d received messages: %+v", i, messages)
		// Verify this consumer has all expected messages
		for key, expectedValue := range finalValues {
			actualValue, exists := messages[key]
			assert.True(t, exists, "Consumer %d missing key %s", i, key)
			assert.Equal(t, expectedValue, actualValue, "Consumer %d has wrong value for key %s", i, key)
		}
		// Verify no unexpected messages
		assert.Len(t, messages, len(finalValues), "Consumer %d has unexpected messages", i)
	}
}

func TestLogCompactedConsumerSyncBehavior(t *testing.T) {
	topicName := "sync-test-topic"

	cluster := newLogCompactedFakeCluster(t, topicName, 1)

	addrs := cluster.ListenAddrs()

	// Create consumer
	var processedRecords int32
	cfg := LogCompactedConfig{
		CommonConfig: CommonConfig{
			Brokers:  addrs,
			Logger:   zapTest(t),
			ClientID: "sync-test-consumer",
		},
		Topic:        topicName,
		FetchMaxWait: time.Second,
		Processor: func(_ context.Context, iter *kgo.FetchesRecordIter) error {
			for !iter.Done() {
				_ = iter.Next() // We just count records, don't need content
				atomic.AddInt32(&processedRecords, 1)
			}
			return nil
		},
	}

	consumer, err := NewLogCompactedConsumer(cfg)
	require.NoError(t, err)
	defer consumer.Close()

	// Start consumer
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, consumer.Run(ctx))

	// Initially, health check should fail (not synced)
	healthCtx, healthCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer healthCancel()
	assert.EqualError(t, consumer.Healthy(healthCtx),
		"healthy: consumer not fully synced yet: 0 remaining: context deadline exceeded",
	)

	// Produce some messages
	producer, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	require.NoError(t, err)
	defer producer.Close()

	record := &kgo.Record{Topic: topicName, Key: []byte("sync-key"), Value: []byte("sync-value")}
	prodCtx, prodCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err = producer.ProduceSync(prodCtx, record).FirstErr()
	prodCancel()
	require.NoError(t, err)

	// Wait for consumer to process and sync
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&processedRecords) > 0
	}, time.Second, 50*time.Millisecond, "Consumer should process records")

	// Health check should now pass (synced)
	healthCtx, healthCancel = context.WithTimeout(context.Background(), time.Second)
	defer healthCancel()
	assert.NoError(t, consumer.Healthy(healthCtx), "Health check should pass after syncing")
}

func TestLogCompactedConsumerProcessorError(t *testing.T) {
	topicName := "error-test-topic"

	cluster := newLogCompactedFakeCluster(t, topicName, 1)

	addrs := cluster.ListenAddrs()

	// Create consumer with processor that returns error and capture logs
	expectedErr := errors.New("processor error")
	core, logs := observer.New(zapcore.ErrorLevel)
	logger := zap.New(core)

	cfg := LogCompactedConfig{
		CommonConfig: CommonConfig{
			Brokers:  addrs,
			Logger:   logger,
			ClientID: "error-test-consumer",
		},
		Topic:        topicName,
		FetchMaxWait: 100 * time.Millisecond,
		Processor: func(context.Context, *kgo.FetchesRecordIter) error {
			return expectedErr
		},
	}

	consumer, err := NewLogCompactedConsumer(cfg)
	require.NoError(t, err)
	defer consumer.Close()

	// Start consumer
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { require.NoError(t, consumer.Run(ctx)) }()

	// Produce a message to trigger processor error
	producer, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	require.NoError(t, err)
	defer producer.Close()

	record := &kgo.Record{Topic: topicName, Key: []byte("error-key"), Value: []byte("error-value")}
	prodCtx, prodCancel := context.WithTimeout(context.Background(), time.Second)
	defer prodCancel()
	err = producer.ProduceSync(prodCtx, record).FirstErr()
	require.NoError(t, err)

	// Wait for the error to be logged and verify it contains the expected error message
	require.Eventually(t, func() bool {
		observedLogs := logs.FilterMessage("error processing records").TakeAll()
		if len(observedLogs) == 0 {
			return false
		}

		// Verify the error message is logged correctly
		logEntry := observedLogs[0]
		assert.Equal(t, zapcore.ErrorLevel, logEntry.Level)
		assert.Equal(t, "error processing records", logEntry.Message)
		assert.Equal(t, expectedErr.Error(), logEntry.ContextMap()["error"])
		assert.Equal(t, topicName, logEntry.ContextMap()["topic"])
		assert.Contains(t, logEntry.ContextMap(), "partitions")

		return true
	}, time.Second, 100*time.Millisecond, "Expected error log entry not found")
}

func TestLogCompactedConsumerStartStop(t *testing.T) {
	topicName := "lifecycle-test-topic"

	cluster := newLogCompactedFakeCluster(t, topicName, 1)

	addrs := cluster.ListenAddrs()

	cfg := LogCompactedConfig{
		CommonConfig: CommonConfig{
			Brokers:  addrs,
			Logger:   zapTest(t),
			ClientID: "lifecycle-test-consumer",
		},
		Topic:        topicName,
		FetchMaxWait: 100 * time.Millisecond,
		Processor:    func(context.Context, *kgo.FetchesRecordIter) error { return nil },
	}

	consumer, err := NewLogCompactedConsumer(cfg)
	require.NoError(t, err)

	// Test running the consumer
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, consumer.Run(ctx))
	assert.NoError(t, consumer.Run(ctx), "Second Run should not error")

	// Test closing
	require.NoError(t, consumer.Close(), "Close should succeed")
	assert.NoError(t, consumer.Close(), "Second Close should not error")

	assert.Error(t, consumer.Run(ctx), "Run should error after Close")
}

func TestLogCompactedConsumerHealthyOnEmptyFirstFetch(t *testing.T) {
	topicName := "empty-first-fetch-topic"
	cluster := newLogCompactedFakeCluster(t, topicName, 1)
	var processedRecords int32
	cfg := LogCompactedConfig{
		CommonConfig: CommonConfig{
			Brokers:  cluster.ListenAddrs(),
			Logger:   zapTest(t),
			ClientID: "empty-first-fetch-consumer",
		},
		Topic:        topicName,
		FetchMaxWait: 100 * time.Millisecond,
		MinFetchSize: 0,
		Processor: func(_ context.Context, iter *kgo.FetchesRecordIter) error {
			for !iter.Done() {
				_ = iter.Next()
				atomic.AddInt32(&processedRecords, 1)
			}
			return nil
		},
	}

	consumer, err := NewLogCompactedConsumer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { consumer.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, consumer.Run(ctx))

	// With an empty topic, the first fetch should return no records and the
	// consumer should be considered synced (healthy) without producing anything.
	require.Eventually(t, func() bool {
		return consumer.Healthy(ctx) == nil
	}, time.Second, 50*time.Millisecond)

	// Sanity check: no records were processed.
	assert.Equal(t, int32(0), atomic.LoadInt32(&processedRecords))
}
