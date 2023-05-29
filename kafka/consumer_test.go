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
	"crypto/tls"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
	saslplain "github.com/elastic/apm-queue/kafka/sasl/plain"
)

func TestNewConsumer(t *testing.T) {
	testCases := map[string]struct {
		expectErr bool
		cfg       ConsumerConfig
	}{
		"empty": {
			expectErr: true,
		},
		"invalid client consumer options": {
			cfg: ConsumerConfig{
				CommonConfig: CommonConfig{
					Brokers: []string{"localhost:invalidport"},
					Logger:  zap.NewNop(),
				},
				Topics:    []apmqueue.Topic{"topic"},
				GroupID:   "groupid",
				Decoder:   json.JSON{},
				Processor: model.ProcessBatchFunc(func(context.Context, *model.Batch) error { return nil }),
			},
			expectErr: true,
		},
		"valid": {
			cfg: ConsumerConfig{
				CommonConfig: CommonConfig{
					Brokers:  []string{"localhost:9092"},
					Logger:   zap.NewNop(),
					ClientID: "clientid",
					Version:  "1.0",
					SASL:     saslplain.New(saslplain.Plain{}),
					TLS:      &tls.Config{},
				},
				GroupID:   "groupid",
				Topics:    []apmqueue.Topic{"topic"},
				Decoder:   json.JSON{},
				Processor: model.ProcessBatchFunc(func(context.Context, *model.Batch) error { return nil }),
			},
			expectErr: false,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			consumer, err := NewConsumer(tc.cfg)
			if err == nil {
				defer assert.NoError(t, consumer.Close())
			}
			if tc.expectErr {
				assert.Error(t, err)
				assert.Nil(t, consumer)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, consumer)
			}
		})
	}
}

func TestConsumerHealth(t *testing.T) {
	testCases := map[string]struct {
		expectErr  bool
		closeEarly bool
	}{
		"success": {
			expectErr:  false,
			closeEarly: false,
		},
		"failure": {
			expectErr:  true,
			closeEarly: true,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cluster, err := kfake.NewCluster()
			require.NoError(t, err)

			addrs := cluster.ListenAddrs()
			consumer := newConsumer(t, ConsumerConfig{
				CommonConfig: CommonConfig{
					Brokers: addrs,
					Logger:  zap.NewNop(),
				},
				Topics:    []apmqueue.Topic{"topic"},
				GroupID:   "groupid",
				Decoder:   json.JSON{},
				Processor: model.ProcessBatchFunc(func(context.Context, *model.Batch) error { return nil }),
			})

			if tc.closeEarly {
				cluster.Close()
			} else {
				defer cluster.Close()
			}

			if tc.expectErr {
				assert.Error(t, consumer.Healthy(context.Background()))
			} else {
				assert.NoError(t, consumer.Healthy(context.Background()))
			}
		})
	}
}

func TestConsumerFetch(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	defer tp.Shutdown(context.Background())

	event := model.APMEvent{Transaction: &model.Transaction{ID: "1"}}
	codec := json.JSON{}
	topics := []apmqueue.Topic{"topic"}
	client, addrs := newClusterWithTopics(t, topics...)

	cfg := ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:        addrs,
			Logger:         zap.NewNop(),
			TracerProvider: tp,
		},
		Topics:  topics,
		GroupID: "groupid",
		Decoder: codec,
		Processor: model.ProcessBatchFunc(func(_ context.Context, b *model.Batch) error {
			assert.Len(t, *b, 1)
			assert.Equal(t, event, (*b)[0])
			return nil
		}),
	}

	b, err := codec.Encode(event)
	require.NoError(t, err)

	produceRecord(context.Background(), t, client,
		&kgo.Record{Topic: string(topics[0]), Value: b},
	)
	consumer := newConsumer(t, cfg)

	spanCount := len(exp.GetSpans())
	assert.NoError(t, consumer.fetch(context.Background()))
	assert.Len(t, exp.GetSpans(), spanCount+1)
}

func TestConsumerDelivery(t *testing.T) {
	// ALOD = at least once delivery
	// AMOD = at most once delivery
	event := model.APMEvent{Transaction: &model.Transaction{ID: "1"}}
	codec := json.JSON{}
	topics := []apmqueue.Topic{"topic"}

	// Produces `initialRecords` + `lastRecords` in total. Asserting the
	// "lossy" behavior of the consumer implementation, depending on the
	// chosen DeliveryType.
	// `initialRecords` are produced, then, the concurrent consumer is
	// started, and the first receive will always fail to process the first
	// records in the received poll (`maxPollRecords`).
	// Next, the context is cancelled and the consumer is stopped.
	// A new consumer is created which takes over from the last committed
	// offset.
	// Depending on the DeliveryType some or none records should be lost.
	cases := map[string]struct {
		// Test setup.
		deliveryType   apmqueue.DeliveryType
		initialRecords int
		maxPollRecords int
		lastRecords    int
		// Number of successfully processed events. The assertion is GE due to
		// variable guarantee factors.
		processed int32
		// Number of unsuccessfully processed events.
		errored int32
	}{
		"12_produced_10_poll_AMOD": {
			deliveryType:   apmqueue.AtMostOnceDeliveryType,
			initialRecords: 10,
			maxPollRecords: 10,
			lastRecords:    2,

			processed: 2,  // The last produced records are processed.
			errored:   10, // The initial produced records are lost.
		},
		"30_produced_2_poll_AMOD": {
			deliveryType:   apmqueue.AtMostOnceDeliveryType,
			initialRecords: 20,
			maxPollRecords: 2,
			lastRecords:    10,

			// 30 total - 2 errored - 2 lost before they can be processed.
			processed: 26,
			errored:   2, // The first two fetch fails.
		},
		"12_produced_1_poll_AMOD": {
			deliveryType:   apmqueue.AtMostOnceDeliveryType,
			initialRecords: 1,
			maxPollRecords: 1,
			lastRecords:    11,

			processed: 11, // The last produced records are processed.
			errored:   1,  // The initial produced records are lost.
		},
		"12_produced_10_poll_ALOD": {
			deliveryType:   apmqueue.AtLeastOnceDeliveryType,
			initialRecords: 10,
			maxPollRecords: 10,
			lastRecords:    2,

			processed: 12, // All records are re-processed.
			errored:   10, // The initial batch errors.
		},
		"30_produced_2_poll_ALOD": {
			deliveryType:   apmqueue.AtLeastOnceDeliveryType,
			initialRecords: 20,
			maxPollRecords: 2,
			lastRecords:    10,

			processed: 30, // All records are processed.
			errored:   2,  // The initial batch errors.
		},
		"12_produced_1_poll_ALOD": {
			deliveryType:   apmqueue.AtLeastOnceDeliveryType,
			initialRecords: 1,
			maxPollRecords: 1,
			lastRecords:    11,

			processed: 11,
			errored:   1,
		},
		"1_produced_1_poll_ALOD": {
			deliveryType:   apmqueue.AtLeastOnceDeliveryType,
			initialRecords: 1,
			maxPollRecords: 1,
			lastRecords:    0,

			processed: 1,
			errored:   1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			client, addrs := newClusterWithTopics(t, topics...)
			failRecord := make(chan struct{})
			processRecord := make(chan struct{})
			defer close(failRecord)

			baseLogger := zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))

			var processed atomic.Int32
			var errored atomic.Int32
			cfg := ConsumerConfig{
				CommonConfig: CommonConfig{
					Brokers: addrs,
					Logger:  baseLogger,
				},
				Delivery:       tc.deliveryType,
				Decoder:        codec,
				Topics:         topics,
				GroupID:        "groupid",
				MaxPollRecords: tc.maxPollRecords,
				Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
					select {
					// Records are marked as processed on receive processRecord.
					case <-processRecord:
						processed.Add(int32(len(*b)))
					// Records are marked as failed when ctx is canceled, or
					// on receive failRecord.
					case <-failRecord:
						errored.Add(int32(len(*b)))
						return errors.New("failed processing record")
					case <-ctx.Done():
						errored.Add(int32(len(*b)))
						return ctx.Err()
					}
					return nil
				}),
			}

			b, err := codec.Encode(event)
			require.NoError(t, err)
			record := &kgo.Record{
				Topic: string(topics[0]),
				Value: b,
			}

			// Context used for the consumer
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			for i := 0; i < int(tc.initialRecords); i++ {
				produceRecord(ctx, t, client, record)
			}

			cfg.Logger = baseLogger.Named("1")
			consumer := newConsumer(t, cfg)
			go func() {
				err := consumer.Run(ctx)
				if err != nil {
					assert.Equal(t, ErrCommitFailed, err)
				}
			}()

			// Wait until the batch processor function is called.
			// The first event is processed and after the context is canceled,
			// the partition consumers are also stopped. Fetching and record
			// processing is decoupled. The consumer may have fetched more
			// records while waiting for `failRecord`.
			// For AMOD, the offsets are committed after being fetched, which
			// means that records may be lost before they reach the Processor.
			select {
			case failRecord <- struct{}{}:
				cancel()
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for consumer to process event")
			}
			consumer.Close()

			assert.Eventually(t, func() bool {
				return int(errored.Load()) == tc.maxPollRecords
			}, time.Second, time.Millisecond)

			// Start a new consumer in the background and then produce
			ctx, cancel = context.WithCancel(context.Background())
			defer cancel()
			// Produce tc.lastRecords.
			for i := 0; i < tc.lastRecords; i++ {
				produceRecord(ctx, t, client, record)
			}
			cfg.MaxPollRecords = tc.lastRecords
			cfg.Logger = baseLogger.Named("2")
			consumer = newConsumer(t, cfg)
			go func() {
				assert.NoError(t, consumer.Run(ctx))
			}()
			// Wait for the first record to be consumed before running any assertion.
			select {
			case processRecord <- struct{}{}:
				close(processRecord) // Allow records to be processed
			case <-time.After(6 * time.Second):
				t.Fatal("timed out waiting for consumer to process event")
			}

			assert.Eventually(t, func() bool {
				// Some events may or may not be processed. Assert GE.
				return processed.Load() >= tc.processed &&
					errored.Load() == tc.errored
			}, 2*time.Second, time.Millisecond)
			t.Logf("got: %d events errored, %d processed, want: %d errored, %d processed",
				errored.Load(), processed.Load(), tc.errored, tc.processed,
			)
		})
	}
}

func TestConsumerGracefulShutdown(t *testing.T) {
	test := func(t testing.TB, dt apmqueue.DeliveryType) {
		client, brokers := newClusterWithTopics(t, "topic")
		var codec json.JSON
		var processed atomic.Int32
		var errored atomic.Int32
		process := make(chan struct{})
		records := 2
		consumer := newConsumer(t, ConsumerConfig{
			CommonConfig: CommonConfig{
				Brokers: brokers,
				Logger:  zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel)),
			},
			GroupID:        "group",
			Topics:         []apmqueue.Topic{"topic"},
			Decoder:        codec,
			MaxPollRecords: records,
			Delivery:       dt,
			Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
				select {
				case <-ctx.Done():
					errored.Add(int32(len(*b)))
					return ctx.Err()
				case <-process:
					processed.Add(int32(len(*b)))
				}
				return nil
			}),
		})

		b, err := codec.Encode(model.APMEvent{Transaction: &model.Transaction{ID: "1"}})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		for i := 0; i < records; i++ {
			produceRecord(ctx, t, client, &kgo.Record{Topic: "topic", Value: b})
		}

		go func() { consumer.Run(ctx) }()
		select {
		case process <- struct{}{}:
			close(process) // Allow records to be processed
			cancel()       // Stop the consumer.
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for consumer to process event")
		}
		assert.Eventually(t, func() bool {
			return processed.Load() == int32(records) && errored.Load() == 0
		}, 6*time.Second, time.Millisecond)
		t.Logf("got: %d events processed, %d errored, want: %d processed",
			processed.Load(), errored.Load(), records,
		)
	}

	t.Run("AtLeastOnceDelivery", func(t *testing.T) {
		test(t, apmqueue.AtLeastOnceDeliveryType)
	})
	t.Run("AtMostOnceDelivery", func(t *testing.T) {
		test(t, apmqueue.AtMostOnceDeliveryType)
	})
}

func TestMultipleConsumers(t *testing.T) {
	event := model.APMEvent{Transaction: &model.Transaction{ID: "1"}}
	codec := json.JSON{}
	topics := []apmqueue.Topic{"topic"}
	client, addrs := newClusterWithTopics(t, topics...)

	var count atomic.Int32
	cfg := ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: addrs,
			Logger:  zap.NewNop(),
		},
		Topics:  topics,
		GroupID: "groupid",
		Decoder: codec,
		Processor: model.ProcessBatchFunc(func(_ context.Context, b *model.Batch) error {
			count.Add(1)
			assert.Len(t, *b, 1)
			return nil
		}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumers := 2
	for i := 0; i < consumers; i++ {
		go func() {
			consumer := newConsumer(t, cfg)
			assert.NoError(t, consumer.Run(ctx))
		}()
	}

	b, err := codec.Encode(event)
	require.NoError(t, err)
	record := kgo.Record{
		Topic: string(topics[0]),
		Value: b,
	}
	produced := 1000
	for i := 0; i < produced; i++ {
		produceRecord(ctx, t, client, &record)
	}
	assert.Eventually(t, func() bool {
		return count.Load() == int32(produced)
	}, time.Second, 50*time.Millisecond)
}

func TestMultipleConsumerGroups(t *testing.T) {
	event := model.APMEvent{Transaction: &model.Transaction{ID: "1"}}
	codec := json.JSON{}
	topics := []apmqueue.Topic{"topic"}
	client, addrs := newClusterWithTopics(t, topics...)
	cfg := ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers: addrs,
			Logger:  zap.NewNop(),
		},
		Topics:  topics,
		Decoder: codec,
	}

	b, err := codec.Encode(event)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := make(map[string]*atomic.Int64)
	groups := 10
	for i := 0; i < groups; i++ {
		gid := "groupid" + strconv.Itoa(i)
		cfg.GroupID = gid
		var counter atomic.Int64
		m[gid] = &counter
		cfg.Processor = model.ProcessBatchFunc(func(_ context.Context, b *model.Batch) error {
			counter.Add(1)
			assert.Len(t, *b, 1)
			assert.Equal(t, event, (*b)[0])
			return nil
		})
		consumer := newConsumer(t, cfg)
		go func() {
			assert.NoError(t, consumer.Run(ctx))
		}()
	}

	produceRecords := 100
	for i := 0; i < produceRecords; i++ {
		client.Produce(ctx, &kgo.Record{
			Topic: string(topics[0]),
			Value: b,
		}, func(r *kgo.Record, err error) {
			assert.NoError(t, err)
			assert.NotNil(t, r)
		})
	}

	for k, i := range m {
		assert.Eventually(t, func() bool {
			return i.Load() == int64(produceRecords)
		}, time.Second, 50*time.Millisecond, k)
	}
}

func TestConsumerRunError(t *testing.T) {
	newConsumer := func(t testing.TB, ready chan struct{}) (*Consumer, *kgo.Client) {
		client, addr := newClusterWithTopics(t, "topic")
		consumer := newConsumer(t, ConsumerConfig{
			CommonConfig: CommonConfig{
				Brokers: addr,
				Logger:  zaptest.NewLogger(t),
			},
			Topics:  []apmqueue.Topic{"topic"},
			GroupID: "groupid",
			Decoder: json.JSON{},
			Processor: model.ProcessBatchFunc(func(context.Context, *model.Batch) error {
				select {
				case <-ready:
				default:
					close(ready)
				}
				return nil
			}),
		})
		return consumer, client
	}
	t.Run("context.Cancelled", func(t *testing.T) {
		consumer, _ := newConsumer(t, nil)
		// ctx canceled
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, consumer.Run(ctx))
		// Returns an error after the consumer has been run.
		require.Error(t, consumer.Run(context.Background()))
	})
	t.Run("consumer.Close()", func(t *testing.T) {
		ready := make(chan struct{})
		consumer, client := newConsumer(t, ready)
		go func() {
			require.NoError(t, consumer.Run(context.Background()))
		}()
		produceRecord(context.Background(), t, client, &kgo.Record{
			Topic: "topic", Value: []byte("{}"),
		})
		select {
		case <-ready:
			consumer.Close()
		case <-time.After(time.Second):
		}
		// Returns an error after the consumer has been run.
		require.Error(t, consumer.Run(context.Background()))
	})
}

func newConsumer(t testing.TB, cfg ConsumerConfig) *Consumer {
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, consumer.Close())
	})
	return consumer
}

func produceRecord(ctx context.Context, t testing.TB, c *kgo.Client, r *kgo.Record) {
	t.Helper()
	results := c.ProduceSync(ctx, r)
	assert.NoError(t, results.FirstErr())
	r, err := results.First()
	assert.NoError(t, err)
	assert.NotNil(t, r)
}
