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
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/queuecontext"
)

func TestNewProducer(t *testing.T) {
	t.Run("invalid", func(t *testing.T) {
		_, err := NewProducer(ProducerConfig{})
		require.Error(t, err)
		assert.EqualError(t, err, "kafka: invalid producer config: "+strings.Join([]string{
			"kafka: at least one broker must be set",
			"kafka: logger must be set",
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
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	defer tp.Shutdown(context.Background())

	// This test ensures that basic producing is working, it tests:
	// * Producing to a single topic
	// * Producing a set number of records
	// * Content contains headers from arbitrary metadata.
	test := func(t *testing.T, sync bool) {
		t.Run(fmt.Sprintf("sync_%t", sync), func(t *testing.T) {
			topic := apmqueue.Topic("default-topic")
			client, brokers := newClusterWithTopics(t, 1, topic)
			producer, err := NewProducer(ProducerConfig{
				CommonConfig: CommonConfig{
					Brokers:        brokers,
					Logger:         zap.NewNop(),
					TracerProvider: tp,
				},
				Sync: sync,
			})
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			ctx = queuecontext.WithMetadata(ctx, map[string]string{"a": "b", "c": "d"})
			batch := []apmqueue.Record{
				{Topic: topic, Value: []byte("1")},
				{Topic: topic, Value: []byte("2")},
			}
			spanCount := len(exp.GetSpans())
			if !sync {
				// Cancel the context before calling Produce
				ctxCancelled, cancelProduce := context.WithCancel(ctx)
				cancelProduce()
				producer.Produce(ctxCancelled, batch...)
			} else {
				producer.Produce(ctx, batch...)
			}

			client.AddConsumeTopics(string(topic))
			for i := 0; i < len(batch); i++ {
				fetches := client.PollRecords(ctx, 1)
				require.NoError(t, fetches.Err())

				// Assert contents.
				assert.Equal(t,
					apmqueue.Record{Topic: topic, Value: []byte(fmt.Sprint(i + 1))},
					batch[i],
				)

				// Assert length.
				records := fetches.Records()
				assert.Len(t, records, 1)
				record := records[0]
				// Sort headers and assert their existence.
				sort.Slice(record.Headers, func(i, j int) bool {
					return record.Headers[i].Key < record.Headers[j].Key
				})
				assert.Equal(t, []kgo.RecordHeader{
					{Key: "a", Value: []byte("b")},
					{Key: "c", Value: []byte("d")},
				}, record.Headers)
			}

			// Assert no more records have been produced. A nil context is used to
			// cause PollRecords to return immediately.
			//lint:ignore SA1012 passing a nil context is a valid use for this call.
			fetches := client.PollRecords(nil, 1)
			assert.Len(t, fetches.Records(), 0)

			// Assert tracing happened properly
			assert.Eventually(t, func() bool {
				return len(exp.GetSpans()) == spanCount+3
			}, time.Second, 10*time.Millisecond)

			var span tracetest.SpanStub
			for _, s := range exp.GetSpans() {
				if s.Name == "producer.Produce" {
					span = s
				}
			}

			assert.Equal(t, "producer.Produce", span.Name)
			assert.Equal(t, []attribute.KeyValue{
				attribute.Bool("sync", sync),
				attribute.Int("record.count", 2),
			}, span.Attributes)

			exp.Reset()
		})
	}
	test(t, true)
	test(t, false)
}

func TestProducerGracefulShutdown(t *testing.T) {
	test := func(t testing.TB, dt apmqueue.DeliveryType, syncProducer bool) {
		_, brokers := newClusterWithTopics(t, 1, "topic")
		var processed atomic.Int64
		wait := make(chan struct{})
		// signal := make(chan struct{})
		producer := newProducer(t, ProducerConfig{
			CommonConfig: CommonConfig{
				Brokers: brokers,
				Logger:  zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel)),
			},
			Sync: syncProducer,
		})
		consumer := newConsumer(t, ConsumerConfig{
			CommonConfig: CommonConfig{
				Brokers: brokers,
				Logger:  zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel)),
			},
			GroupID:  "group",
			Topics:   []apmqueue.Topic{"topic"},
			Delivery: dt,
			Processor: apmqueue.ProcessorFunc(func(_ context.Context, r ...apmqueue.Record) error {
				<-wait
				processed.Add(int64(len(r)))
				return nil
			}),
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// This is a workaround to hook into the processing code using json marshalling.
		// The goal is try to close the producer before the batch is sent to kafka but after the Produce
		// method is called.
		// We are using two goroutines because both Produce and Close will block waiting on each other.
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
		}, 6*time.Second, time.Millisecond, processed)
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
	_, brokers := newClusterWithTopics(t, 1, "topic")
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

func newClusterWithTopics(t testing.TB, partitions int32, topics ...apmqueue.Topic) (*kgo.Client, []string) {
	t.Helper()
	cluster, err := kfake.NewCluster()
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	addrs := cluster.ListenAddrs()

	client, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	require.NoError(t, err)

	kadmClient := kadm.NewClient(client)
	t.Cleanup(kadmClient.Close)

	strTopic := make([]string, 0, len(topics))
	for _, t := range topics {
		strTopic = append(strTopic, string(t))
	}
	_, err = kadmClient.CreateTopics(context.Background(), partitions, 1, nil, strTopic...)
	require.NoError(t, err)
	return client, addrs
}

func newProducer(t testing.TB, cfg ProducerConfig) *Producer {
	producer, err := NewProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, producer.Close())
	})
	return producer
}
