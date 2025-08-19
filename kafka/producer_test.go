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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	apmqueue "github.com/elastic/apm-queue/v2"
	"github.com/elastic/apm-queue/v2/queuecontext"
)

func TestNewProducer(t *testing.T) {
	t.Run("invalid", func(t *testing.T) {
		_, err := NewProducer(ProducerConfig{})
		require.Error(t, err)
		assert.EqualError(t, err, "kafka: invalid producer config: "+strings.Join([]string{
			"kafka: logger must be set",
			"kafka: at least one broker must be set",
		}, "\n"))
	})

	validConfig := ProducerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"broker"},
			Logger:  zap.NewNop(),
		},
	}

	t.Run("valid", func(t *testing.T) {
		p, err := NewProducer(validConfig)
		require.NoError(t, err)
		require.NotNil(t, p)
		require.NoError(t, p.Close())
	})

	t.Run("compression_from_environment", func(t *testing.T) {
		t.Setenv("KAFKA_PRODUCER_COMPRESSION_CODEC", "zstd,gzip,none")
		p, err := NewProducer(validConfig)
		require.NoError(t, err)
		require.NotNil(t, p)
		assert.Equal(t, []CompressionCodec{
			ZstdCompression(),
			GzipCompression(),
			NoCompression(),
		}, p.cfg.CompressionCodec)
		require.NoError(t, p.Close())
	})

	t.Run("invalid_compression_from_environment", func(t *testing.T) {
		t.Setenv("KAFKA_PRODUCER_COMPRESSION_CODEC", "huffman,bson")
		_, err := NewProducer(validConfig)
		require.Error(t, err)
		assert.EqualError(t, err, "kafka: invalid producer config: "+strings.Join([]string{
			`kafka: unknown codec "huffman"`,
			`kafka: unknown codec "bson"`,
		}, "\n"))
	})
}

func TestNewProducerBasic(t *testing.T) {
	// This test ensures that basic producing is working, it tests:
	// * Producing to a single topic
	// * Producing a set number of records
	// * Content contains headers from arbitrary metadata.
	test := func(t *testing.T, isSync bool) {
		t.Run(fmt.Sprintf("sync_%t", isSync), func(t *testing.T) {
			topic := apmqueue.Topic("default-topic")
			namespacedTopic := "name_space-default-topic"
			partitionCount := 10
			exp := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
			defer tp.Shutdown(context.Background())

			topicBytesWritten := map[string]*atomic.Int64{
				namespacedTopic: &atomic.Int64{},
			}

			client, brokers := newClusterWithTopics(t, int32(partitionCount), namespacedTopic)
			producer := newProducer(t, ProducerConfig{
				CommonConfig: CommonConfig{
					Brokers:        brokers,
					Logger:         zap.NewNop(),
					Namespace:      "name_space",
					TracerProvider: tp,
				},
				Sync:               isSync,
				MaxBufferedRecords: 0,
				BatchListener: func(topic string, bytesWritten int) {
					topicBytesWritten[topic].Add(int64(bytesWritten))
				},
			})

			ctx := queuecontext.WithMetadata(context.Background(), map[string]string{
				"a": "b", "c": "d",
			})

			batch := []apmqueue.Record{
				{Topic: topic, OrderingKey: nil, Value: []byte("1")},
				{Topic: topic, OrderingKey: nil, Value: []byte("2")},
				{Topic: topic, OrderingKey: []byte("key_2"), Value: []byte("3")},
				{Topic: topic, OrderingKey: []byte("key_3"), Value: []byte("4")},
				{Topic: topic, OrderingKey: []byte("key_1"), Value: []byte("5")},
				{Topic: topic, OrderingKey: []byte("key_3"), Value: []byte("6")},
				{Topic: topic, OrderingKey: []byte("key_1"), Value: []byte("7")},
				{Topic: topic, OrderingKey: []byte("key_2"), Value: []byte("8")},
			}
			if !isSync {
				// Cancel the context before calling Produce
				ctxCancelled, cancelProduce := context.WithCancel(ctx)
				cancelProduce()
				require.NoError(t, producer.Produce(ctxCancelled, batch...))
			} else {
				produceCtx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				require.NoError(t, producer.Produce(produceCtx, batch...))
			}

			var actual []apmqueue.Record
			orderingKeyToPartitionM := make(map[string]int32)
			client.AddConsumeTopics(namespacedTopic)

			for len(actual) < len(batch) {
				ctx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()

				fetches := client.PollRecords(ctx, 1)
				require.NoError(t, fetches.Err())

				// Assert length.
				records := fetches.Records()
				if len(records) == 0 {
					continue
				}
				require.Len(t, records, 1)
				record := records[0]

				require.Equal(t, namespacedTopic, record.Topic)
				actual = append(actual, apmqueue.Record{
					Topic:       topic,
					OrderingKey: record.Key,
					Value:       record.Value,
				})
				if record.Key != nil {
					// Assert that specific ordering key maps to same partition.
					// If ordering key is unexpectedly nil then it will be caught
					// in the assertion with expected batch of apmqueue.Record.
					if p, ok := orderingKeyToPartitionM[string(record.Key)]; ok {
						assert.Equal(t, p, record.Partition, "each ordering key must map to same partition")
					} else {
						orderingKeyToPartitionM[string(record.Key)] = record.Partition
					}
				}
				// Sort headers and assert their existence.
				sort.Slice(record.Headers, func(i, j int) bool {
					return record.Headers[i].Key < record.Headers[j].Key
				})
				assert.Equal(t, []kgo.RecordHeader{
					{Key: "a", Value: []byte("b")},
					{Key: "c", Value: []byte("d")},
				}, record.Headers)
			}
			assert.Empty(t, cmp.Diff(
				actual, batch,
				cmpopts.SortSlices(func(a, b apmqueue.Record) bool {
					return bytes.Compare(a.Value, b.Value) < 0
				}),
			))
			// require.NoError(t, producer.Close())

			// Assert no more records have been produced. A nil context is used to
			// cause PollRecords to return immediately.
			//lint:ignore SA1012 passing a nil context is a valid use for this call.
			fetches := client.PollRecords(nil, 1)
			assert.Len(t, fetches.Records(), 0)

			// Ensure that we recorded bytes written to the topic.
			require.Len(t, topicBytesWritten, 1)
			require.Contains(t, topicBytesWritten, namespacedTopic)
			// We can't test equality because the producer batching & compression behavior is not
			// deterministic.
			require.GreaterOrEqual(t,
				topicBytesWritten[namespacedTopic].Load(),
				int64(100),
			)
		})
	}
	test(t, true)
	test(t, false)
}

func TestProduceSyncFailed(t *testing.T) {
	test := func(t *testing.T, ns, topic string) {
		t.Run(ns+topic, func(t *testing.T) {
			correctNamespace := "name_space"
			correctTopic := "default-topic"
			namespacedTopic := correctNamespace + "-" + correctTopic

			_, brokers := newClusterWithTopics(t, int32(1), namespacedTopic)
			producer := newProducer(t, ProducerConfig{
				CommonConfig: CommonConfig{
					Brokers:   brokers,
					Logger:    zap.NewNop(),
					Namespace: ns,
				},
				Sync:               true,
				MaxBufferedRecords: 0,
			})

			batch := []apmqueue.Record{
				{
					Topic:       apmqueue.Topic(topic),
					OrderingKey: []byte("key"),
					Value:       []byte("value"),
				},
			}

			ctx := context.Background()

			// Cancel the context before calling Produce
			if ns == correctNamespace && topic == correctTopic {
				ctxCancelled, cancelProduce := context.WithCancel(ctx)
				cancelProduce()
				err := producer.Produce(ctxCancelled, batch...)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "context canceled")
			}

			if ns != correctNamespace || topic != correctTopic {
				err := producer.Produce(ctx, batch...)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), kerr.UnknownTopicOrPartition.Message)
			}
		})
	}
	test(t, "name_space", "default-topic")
	test(t, "incorrect-namespace", "default-topic")
	test(t, "name_space", "incorrect-topic")
}

func testVerboseLogger(t testing.TB) *zap.Logger {
	t.Helper()
	if testing.Verbose() {
		return zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
	}
	return zap.NewNop()
}

func TestProducerGracefulShutdown(t *testing.T) {
	test := func(t testing.TB, dt apmqueue.DeliveryType, syncProducer bool) {
		brokers := newClusterAddrWithTopics(t, 1, "topic")
		var processed atomic.Int64
		wait := make(chan struct{})
		l := testVerboseLogger(t)
		producer := newProducer(t, ProducerConfig{
			CommonConfig: CommonConfig{
				Brokers: brokers,
				Logger:  l,
			},
			Sync: syncProducer,
		})
		consumer := newConsumer(t, ConsumerConfig{
			CommonConfig: CommonConfig{
				Brokers: brokers,
				Logger:  l,
			},
			GroupID:  "group",
			Topics:   []apmqueue.Topic{"topic"},
			Delivery: dt,
			Processor: apmqueue.ProcessorFunc(func(_ context.Context, r apmqueue.Record) error {
				<-wait
				processed.Add(1)
				return nil
			}),
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// This is a workaround to hook into the processing code using json marshalling.
		// The goal is try to close the producer before the batch is sent to kafka but after the Produce
		// method is called.
		// Brief walkthrough:
		// - Call the producer and start processing the batch
		// - json encoding will block and a signal will be sent so that we know processing started
		// - wait for the signal and then try to close the producer in a separate goroutine.
		// - closing will block until processing finished to avoid losing events
		producer.Produce(ctx, apmqueue.Record{Topic: "topic"})
		close(wait)
		assert.NoError(t, producer.Close())

		// Run a consumer that fetches from kafka to verify that the events are there.
		go func() { consumer.Run(ctx) }()
		assert.Eventually(t, func() bool {
			return processed.Load() == 1
		}, 6*time.Second, time.Millisecond, "must process 1 event")
	}

	// use a variable for readability
	sync := true

	t.Run("AtLeastOnceDelivery", func(t *testing.T) {
		t.Run("sync", func(t *testing.T) {
			test(t, apmqueue.AtLeastOnceDeliveryType, sync)
		})
		t.Run("async", func(t *testing.T) {
			test(t, apmqueue.AtLeastOnceDeliveryType, !sync)
		})
	})
	t.Run("AtMostOnceDelivery", func(t *testing.T) {
		t.Run("sync", func(t *testing.T) {
			test(t, apmqueue.AtMostOnceDeliveryType, sync)
		})
		t.Run("async", func(t *testing.T) {
			test(t, apmqueue.AtMostOnceDeliveryType, !sync)
		})
	})
}

func TestProducerConcurrentClose(t *testing.T) {
	brokers := newClusterAddrWithTopics(t, 1, "topic")
	producer := newProducer(t, ProducerConfig{
		CommonConfig: CommonConfig{
			Brokers: brokers,
			Logger:  zap.NewNop(),
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, producer.Close())
		}()
	}
	wg.Wait()
}

func newProducer(t testing.TB, cfg ProducerConfig) *Producer {
	t.Helper()
	producer, err := NewProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, producer.Close())
	})
	return producer
}
