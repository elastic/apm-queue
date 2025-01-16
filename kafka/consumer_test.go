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
	"math"
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

	apmqueue "github.com/elastic/apm-queue/v2"
	"github.com/elastic/apm-queue/v2/queuecontext"
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
					Logger:  zapTest(t),
				},
				Topics:    []apmqueue.Topic{"topic"},
				GroupID:   "groupid",
				Processor: apmqueue.ProcessorFunc(func(context.Context, apmqueue.Record) error { return nil }),
			},
			expectErr: true,
		},
		"valid": {
			cfg: ConsumerConfig{
				CommonConfig: CommonConfig{
					Brokers:  []string{"localhost:9092"},
					Logger:   zapTest(t),
					ClientID: "clientid",
					Version:  "1.0",
					TLS:      &tls.Config{},
				},
				GroupID:   "groupid",
				Topics:    []apmqueue.Topic{"topic"},
				Processor: apmqueue.ProcessorFunc(func(context.Context, apmqueue.Record) error { return nil }),
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
					Logger:  zapTest(t),
				},
				Topics:    []apmqueue.Topic{"topic"},
				GroupID:   "groupid",
				Processor: apmqueue.ProcessorFunc(func(context.Context, apmqueue.Record) error { return nil }),
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

func TestConsumerInstrumentation(t *testing.T) {
	const partitions = 4
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	defer tp.Shutdown(context.Background())

	namespace := "name_space"
	topic := apmqueue.Topic("topic")
	event := apmqueue.Record{Topic: topic, Value: []byte("1")}
	client, addrs := newClusterWithTopics(t, partitions, "name_space-topic")
	processed := make(chan struct{})
	cfg := ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:        addrs,
			Logger:         zapTest(t),
			Namespace:      namespace,
			TracerProvider: tp,
		},
		Topics:         []apmqueue.Topic{topic},
		GroupID:        "groupid",
		MaxPollRecords: 1, // Consume a single record for this test.
		Processor: apmqueue.ProcessorFunc(func(_ context.Context, r apmqueue.Record) error {
			defer close(processed)
			assert.Equal(t, event.Topic, r.Topic)
			assert.Equal(t, event.Value, r.Value)
			// We cannot know which partition the record will be polled from,
			// but we can assert it's in the range of number of partitions.
			assert.True(t, event.Partition < partitions-1)
			return nil
		}),
	}

	produceRecord(context.Background(), t, client,
		&kgo.Record{Topic: "name_space-topic", Value: event.Value},
	)
	consumer := newConsumer(t, cfg)
	go consumer.Run(context.Background())

	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatal("timed out while waiting for record to be processed.")
	}
}

func TestConsumerDelivery(t *testing.T) {
	// ALOD = at least once delivery
	// AMOD = at most once delivery

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

			// 30 total - 2 errored - 2 lost.
			// 30 total - 2 errored - 2 ignored.
			// 30 total - 2 errored
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

			processed: 12,
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
			client, addrs := newClusterWithTopics(t, 2, "name_space-topic")
			baseLogger := zapTest(t)

			var expected atomic.Int32
			var processed atomic.Int32
			var errored atomic.Int32
			newProcessor := func(processRecord, failRecord <-chan struct{}, fn func()) apmqueue.Processor {
				return apmqueue.ProcessorFunc(func(_ context.Context, r apmqueue.Record) error {
					if expected.Add(-1) < 0 {
						// processing happens in a separate goroutine so even if
						// we cancel the context as soon as we start processing the first
						// batch of records a second fetch might have completed already.
						// Hence ignore any 'extra' record.
						return errors.New("extra records are lost")
					}

					defer fn()
					select {
					// Records are marked as processed on receive processRecord.
					case <-processRecord:
						processed.Add(1)
					// Records are marked as failed on receive failRecord.
					case <-failRecord:
						errored.Add(1)
						return errors.New("failed processing record")
					}
					return nil
				})
			}
			failRecord := make(chan struct{})
			// Context used for the consumer
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cfg := ConsumerConfig{
				CommonConfig: CommonConfig{
					Brokers:   addrs,
					Logger:    baseLogger,
					Namespace: "name_space",
				},
				Delivery:             tc.deliveryType,
				Topics:               []apmqueue.Topic{"topic"},
				GroupID:              "groupid",
				MaxPollRecords:       tc.maxPollRecords,
				MaxConcurrentFetches: 1,
				Processor:            newProcessor(nil, failRecord, cancel),
			}

			record := &kgo.Record{
				Topic: "name_space-topic",
				Value: []byte("content"),
			}

			for i := 0; i < int(tc.initialRecords); i++ {
				produceRecord(ctx, t, client, record)
			}

			// expect up to tc.maxPollRecords
			expected.Store(int32(tc.maxPollRecords))

			cfg.Logger = baseLogger.Named("1")
			consumer := newConsumer(t, cfg)
			consumerDone := make(chan struct{})
			go func() {
				defer close(consumerDone)
				err := consumer.Run(ctx)
				if err != nil {
					assert.ErrorIs(t, err, ErrCommitFailed)
				}
			}()

			// Wait until the processor function is called.
			// The first event is processed and after the context is canceled,
			// the partition consumers are also stopped. Fetching and record
			// processing is decoupled. The consumer may have fetched more
			// records while waiting for `failRecord`.
			// For AMOD, the offsets are committed after being fetched, which
			// means that records may be lost before they reach the Processor.
			select {
			case failRecord <- struct{}{}:
				close(failRecord)
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for consumer to process event")
			}
			require.NoError(t, consumer.Close())

			select {
			case <-consumerDone:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for consumer to close")
			}

			assert.Eventually(t, func() bool {
				return processed.Load() == 0 && errored.Load() == tc.errored
			}, time.Second, time.Millisecond)
			t.Logf("got: %d events errored, %d processed, want: %d errored, %d processed",
				errored.Load(), processed.Load(), tc.errored, tc.processed,
			)

			// Start a new consumer in the background and then produce
			ctx, cancel = context.WithCancel(context.Background())
			defer cancel()
			// Produce tc.lastRecords.
			for i := 0; i < tc.lastRecords; i++ {
				produceRecord(ctx, t, client, record)
			}
			cfg.MaxPollRecords = tc.lastRecords
			cfg.Logger = baseLogger.Named("2")
			processRecord := make(chan struct{})
			cfg.Processor = newProcessor(processRecord, nil, func() {})
			consumer = newConsumer(t, cfg)
			// there should be no remaining records once this is over so just
			// set a 'unlimited' value to avoid ignoring records.
			expected.Store(math.MaxInt32)
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
				if tc.deliveryType == apmqueue.AtMostOnceDeliveryType {
					// For AMOD, we need to account for the following situations:
					// - A second fetch started and the events were processed before the context was canceled
					// so the events got ignored.
					// - A second fetch started before the context was canceled but they were never processed
					// so the events got lost.
					// - The context was canceled before the second fetch could start
					//
					// This is only an issue if tc.initialRecords != tc.maxPollRecords and the tests seems to
					// use the first two cases as the expected scenario.
					// For the third case it means we receive an extra 'tc.maxPollRecords' records.
					return errored.Load() == tc.errored && (processed.Load() == tc.processed || processed.Load() == tc.processed+int32(tc.maxPollRecords))
				}
				// For ALOD, the extra/ignored records in the first fetch were not committed so they
				// didn't affect the record count.
				return processed.Load() == tc.processed && errored.Load() == tc.errored
			}, 2*time.Second, time.Millisecond)
			t.Logf("got: %d events errored, %d processed, want: %d errored, %d processed",
				errored.Load(), processed.Load(), tc.errored, tc.processed,
			)
		})
	}
}

func TestConsumerGracefulShutdown(t *testing.T) {
	// The objective of the test is to ensure that if 2 records are fetched
	// (MaxPollRecords setting) (and committed especially for the AMOD strategy),
	// they aren't lost, and are processed even when consumer.Close() is called.
	// Since fetch will read the records from Kafka (and commit them right away
	// for AMOD), then hand off the records to the partition consumer.
	// The idea was to avoid having a "leaky" consumer and have a test for it
	// that ensures that when the first record is read and the consumer closed,
	// the second record is read as well and not lost.
	test := func(t testing.TB, dt apmqueue.DeliveryType) {
		client, brokers := newClusterWithTopics(t, 2, "topic")
		var processed atomic.Int32
		var errored atomic.Int32
		process := make(chan struct{})
		processedRecord := make(chan struct{})
		records := 2
		consumer := newConsumer(t, ConsumerConfig{
			CommonConfig: CommonConfig{
				Brokers: brokers,
				Logger:  zapTest(t),
			},
			GroupID:        t.Name(),
			Topics:         []apmqueue.Topic{"topic"},
			MaxPollRecords: records,
			Delivery:       dt,
			Processor: apmqueue.ProcessorFunc(func(ctx context.Context, r apmqueue.Record) error {
				select {
				case <-ctx.Done():
					errored.Add(1)
					return ctx.Err()
				case <-process:
					processed.Add(1)
					processedRecord <- struct{}{}
				}
				return nil
			}),
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var consumerErr error
		consumerDone := make(chan struct{})
		go func() {
			defer close(consumerDone)
			consumerErr = consumer.Run(ctx)
		}()

		for i := 0; i < records; i++ {
			produceRecord(ctx, t, client, &kgo.Record{Topic: "topic", Value: []byte("content")})
		}
		for i := 0; i < records; i++ {
			select {
			case process <- struct{}{}:
				<-processedRecord
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for consumer to process event")
			}
		}
		assert.NoError(t, consumer.Close())

		select {
		case <-consumerDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for consumer to exit")
		}

		assert.NoError(t, consumerErr) // Assert the consumer error.
		//  Ensure all records are processed.
		assert.Equal(t, int32(records), processed.Load())
		assert.Zero(t, errored.Load())
	}

	t.Run("AtLeastOnceDelivery", func(t *testing.T) {
		test(t, apmqueue.AtLeastOnceDeliveryType)
	})
	t.Run("AtMostOnceDelivery", func(t *testing.T) {
		test(t, apmqueue.AtMostOnceDeliveryType)
	})
}

func TestConsumerContextPropagation(t *testing.T) {
	addrs := newClusterAddrWithTopics(t, 2, "topic")
	commonCfg := CommonConfig{
		Brokers: addrs,
		Logger:  zapTest(t),
	}
	processed := make(chan struct{})
	expectedMeta := map[string]string{
		"key":       "value",
		"timestamp": time.Now().Format(time.RFC3339),
	}
	consumer := newConsumer(t, ConsumerConfig{
		CommonConfig: commonCfg,
		GroupID:      "ctx_prop",
		Topics:       []apmqueue.Topic{"topic"},
		Processor: apmqueue.ProcessorFunc(func(ctx context.Context, _ apmqueue.Record) error {
			select {
			case <-processed:
				t.Fatal("should only be called once")
			default:
				close(processed)
			}
			meta, ok := queuecontext.MetadataFromContext(ctx)
			assert.True(t, ok)
			assert.Equal(t, expectedMeta, meta)
			return nil
		}),
	})
	// Pass an empty context to the consumer.
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()

	go func() { consumer.Run(consumerCtx) }()
	producer := newProducer(t, ProducerConfig{
		CommonConfig: commonCfg,
		Sync:         true,
	})
	// Enrich the producer's context with metadata.
	ctx, cancel := context.WithCancel(queuecontext.WithMetadata(
		context.Background(), expectedMeta,
	))
	defer cancel()

	producer.Produce(ctx, apmqueue.Record{
		Topic: apmqueue.Topic("topic"),
		Value: []byte("1"),
	})
	assert.NoError(t, producer.Close())

	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatal("timed out while waiting for record to be consumed")
	}
}

func TestMultipleConsumers(t *testing.T) {
	client, addrs := newClusterWithTopics(t, 2, "name_space-topic")

	var count atomic.Int32
	cfg := ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:   addrs,
			Logger:    zapTest(t),
			Namespace: "name_space",
		},
		Topics:  []apmqueue.Topic{"topic"},
		GroupID: "groupid",
		Processor: apmqueue.ProcessorFunc(func(_ context.Context, r apmqueue.Record) error {
			count.Add(1)
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

	record := kgo.Record{
		Topic: "name_space-topic",
		Value: []byte("content"),
	}
	produced := 100
	for i := 0; i < produced; i++ {
		produceRecord(ctx, t, client, &record)
	}
	assert.Eventually(t, func() bool {
		return count.Load() == int32(produced)
	}, time.Second, 50*time.Millisecond)
}

func TestMultipleConsumerGroups(t *testing.T) {
	const partitions = 4
	event := apmqueue.Record{Topic: "topic", Value: []byte("x")}
	client, addrs := newClusterWithTopics(t, partitions, "name_space-topic")
	cfg := ConsumerConfig{
		CommonConfig: CommonConfig{
			Brokers:   addrs,
			Logger:    zapTest(t),
			Namespace: "name_space",
		},
		Topics: []apmqueue.Topic{"topic"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := make(map[string]*atomic.Int64)
	groups := 10
	for i := 0; i < groups; i++ {
		gid := "groupid" + strconv.Itoa(i)
		cfg.GroupID = gid
		var counter atomic.Int64
		m[gid] = &counter
		cfg.Processor = apmqueue.ProcessorFunc(func(_ context.Context, r apmqueue.Record) error {
			counter.Add(1)
			assert.Equal(t, event.Topic, r.Topic)
			assert.Equal(t, event.Value, r.Value)
			// We cannot know which partition the record will be polled from,
			// but we can assert it's in the range of number of partitions.
			assert.True(t, event.Partition < partitions-1)
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
			Topic: "name_space-topic",
			Value: event.Value,
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
		client, addr := newClusterWithTopics(t, 2, "topic")
		consumer := newConsumer(t, ConsumerConfig{
			CommonConfig: CommonConfig{
				Brokers: addr,
				Logger:  zapTest(t),
			},
			Topics:  []apmqueue.Topic{"topic"},
			GroupID: "groupid",
			Processor: apmqueue.ProcessorFunc(func(_ context.Context, _ apmqueue.Record) error {
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

func TestConsumerTopicLogFieldFunc(t *testing.T) {
	t.Run("empty field", func(t *testing.T) {
		const partitions = 2
		topic := apmqueue.Topic("topic")
		event := apmqueue.Record{Topic: topic, Value: []byte("x")}
		var counter atomic.Int64
		client, addrs := newClusterWithTopics(t, partitions, string(topic))
		cfg := ConsumerConfig{
			CommonConfig: CommonConfig{
				Brokers: addrs,
				Logger:  zapTest(t),
				TopicLogFieldFunc: func(topic string) zapcore.Field {
					return zap.Field{}
				},
			},
			GroupID: t.Name(),
			Topics:  []apmqueue.Topic{topic},
			Processor: apmqueue.ProcessorFunc(func(_ context.Context, r apmqueue.Record) error {
				counter.Add(1)
				assert.Equal(t, event.Topic, r.Topic)
				assert.Equal(t, event.Value, r.Value)
				// We cannot know which partition the record will be polled from,
				// but we can assert it's in the range of number of partitions.
				assert.True(t, event.Partition < partitions-1)
				return nil
			}),
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		consumer := newConsumer(t, cfg)
		go func() {
			assert.NoError(t, consumer.Run(ctx))
		}()

		produceRecords := 100
		for i := 0; i < produceRecords; i++ {
			client.Produce(ctx, &kgo.Record{
				Topic: string(topic),
				Value: event.Value,
			}, func(r *kgo.Record, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, r)
			})
		}

		assert.Eventually(t, func() bool {
			return counter.Load() == int64(produceRecords)
		}, time.Second, 50*time.Millisecond)
	})
}

func TestConsumerConfigFinalizer(t *testing.T) {
	proc := apmqueue.ProcessorFunc(func(context.Context, apmqueue.Record) error { return nil })
	ccfg := CommonConfig{
		Brokers: []string{"localhost:9092"},
		Logger:  zapTest(t),
	}
	t.Run("MaxPollBytes set to 1 << 20", func(t *testing.T) {
		cfg := ConsumerConfig{
			CommonConfig:          ccfg,
			Processor:             proc,
			Topics:                []apmqueue.Topic{"topic"},
			GroupID:               "groupid",
			MaxPollBytes:          1 << 20,
			MaxPollPartitionBytes: 1 << 20,
		}
		err := cfg.finalize()
		require.NoError(t, err)
		assertNotNilOptions(t, &cfg)

		assert.Equal(t, ConsumerConfig{
			CommonConfig:          CommonConfig{Brokers: []string{"localhost:9092"}},
			Topics:                []apmqueue.Topic{"topic"},
			GroupID:               "groupid",
			MaxPollBytes:          1 << 20,
			MaxPollPartitionBytes: 1 << 20,
			BrokerMaxReadBytes:    1 << 21,
		}, cfg)
	})
	t.Run("MaxPollBytes set to 1 << 28", func(t *testing.T) {
		cfg := ConsumerConfig{
			CommonConfig:          ccfg,
			Processor:             proc,
			Topics:                []apmqueue.Topic{"topic"},
			GroupID:               "groupid",
			MaxPollBytes:          1 << 28,
			MaxPollPartitionBytes: 1 << 28,
		}
		err := cfg.finalize()
		require.NoError(t, err)
		assertNotNilOptions(t, &cfg)

		assert.Equal(t, ConsumerConfig{
			CommonConfig:          CommonConfig{Brokers: []string{"localhost:9092"}},
			Topics:                []apmqueue.Topic{"topic"},
			GroupID:               "groupid",
			MaxPollBytes:          1 << 28,
			MaxPollPartitionBytes: 1 << 28,
			BrokerMaxReadBytes:    1 << 29,
		}, cfg)
	})
	t.Run("MaxPollBytes set to 1 << 29", func(t *testing.T) {
		cfg := ConsumerConfig{
			CommonConfig:          ccfg,
			Processor:             proc,
			Topics:                []apmqueue.Topic{"topic"},
			GroupID:               "groupid",
			MaxPollBytes:          1 << 29,
			MaxPollPartitionBytes: 1 << 29,
		}
		err := cfg.finalize()
		require.NoError(t, err)
		assertNotNilOptions(t, &cfg)

		assert.Equal(t, ConsumerConfig{
			CommonConfig:          CommonConfig{Brokers: []string{"localhost:9092"}},
			Topics:                []apmqueue.Topic{"topic"},
			GroupID:               "groupid",
			MaxPollBytes:          1 << 29,
			MaxPollPartitionBytes: 1 << 29,
			BrokerMaxReadBytes:    1 << 30,
		}, cfg)
	})
	t.Run("MaxPollBytes set to 1 << 30", func(t *testing.T) {
		cfg := ConsumerConfig{
			CommonConfig:          ccfg,
			Processor:             proc,
			Topics:                []apmqueue.Topic{"topic"},
			GroupID:               "groupid",
			MaxPollBytes:          1 << 30,
			MaxPollPartitionBytes: 1 << 30,
		}
		err := cfg.finalize()
		require.NoError(t, err)
		assertNotNilOptions(t, &cfg)

		assert.Equal(t, ConsumerConfig{
			CommonConfig:          CommonConfig{Brokers: []string{"localhost:9092"}},
			Topics:                []apmqueue.Topic{"topic"},
			GroupID:               "groupid",
			MaxPollBytes:          1 << 30,
			MaxPollPartitionBytes: 1 << 30,
			BrokerMaxReadBytes:    1 << 30,
		}, cfg)
	})
	t.Run("MaxPollBytes set to 1 << 31-1", func(t *testing.T) {
		cfg := ConsumerConfig{
			CommonConfig:          ccfg,
			Processor:             proc,
			Topics:                []apmqueue.Topic{"topic"},
			GroupID:               "groupid",
			MaxPollBytes:          1<<31 - 1,
			MaxPollPartitionBytes: 1<<31 - 1,
		}
		err := cfg.finalize()
		require.NoError(t, err)
		assertNotNilOptions(t, &cfg)

		assert.Equal(t, ConsumerConfig{
			CommonConfig:          CommonConfig{Brokers: []string{"localhost:9092"}},
			Topics:                []apmqueue.Topic{"topic"},
			GroupID:               "groupid",
			MaxPollBytes:          1 << 30,
			MaxPollPartitionBytes: 1 << 30,
			BrokerMaxReadBytes:    1 << 30,
		}, cfg)
	})
	t.Run("BrokerMaxReadBytes is less than MaxPollBytes", func(t *testing.T) {
		cfg := ConsumerConfig{
			CommonConfig:          ccfg,
			Processor:             proc,
			Topics:                []apmqueue.Topic{"topic"},
			GroupID:               "groupid",
			BrokerMaxReadBytes:    1,
			MaxPollBytes:          1<<31 - 1,
			MaxPollPartitionBytes: 1<<31 - 1,
		}
		err := cfg.finalize()
		assert.EqualError(t, err, "kafka: BrokerMaxReadBytes (1) cannot be less than MaxPollBytes (1073741824)")
	})
}

func assertNotNilOptions(t testing.TB, cfg *ConsumerConfig) {
	t.Helper()

	assert.NotNil(t, cfg.Processor)
	cfg.Processor = nil
	assert.NotNil(t, cfg.Logger)
	cfg.Logger = nil
	assert.NotNil(t, cfg.TopicAttributeFunc)
	cfg.TopicAttributeFunc = nil
}

func newConsumer(t testing.TB, cfg ConsumerConfig) *Consumer {
	if cfg.MaxPollWait <= 0 {
		// Lower MaxPollWait, ShutdownGracePeriod to speed up execution.
		cfg.MaxPollWait = 50 * time.Millisecond
		cfg.ShutdownGracePeriod = time.Second
	}
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

func zapTest(t testing.TB) *zap.Logger {
	return zaptest.NewLogger(t, zaptest.Level(zapcore.InfoLevel))
}
