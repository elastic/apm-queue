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

package systemtest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
	"github.com/elastic/apm-queue/kafka"
	"github.com/elastic/apm-queue/pubsublite"
)

type stub struct {
	wait   chan struct{}
	signal chan struct{}
}

func (s *stub) MarshalJSON() ([]byte, error) {
	close(s.signal)
	<-s.wait
	return []byte("null"), nil
}

func TestProducerGracefulShutdown(t *testing.T) {
	testCases := map[string]struct {
		sync    bool
		timeout time.Duration
	}{
		"async": {
			sync:    false,
			timeout: 90 * time.Second,
		},
		"sync": {
			sync:    true,
			timeout: 90 * time.Second,
		},
	}

	for name, tc := range testCases {
		t.Run("Kafka/"+name, func(t *testing.T) {
			logger := zaptest.NewLogger(t, zaptest.Level(zapcore.InfoLevel))
			codec := json.JSON{}
			var processed atomic.Int64
			topics := SuffixTopics(apmqueue.Topic(t.Name()))
			topicRouter := func(event model.APMEvent) apmqueue.Topic {
				return apmqueue.Topic(topics[0])
			}

			err := ProvisionKafka(context.Background(),
				newLocalKafkaConfig(topics...),
			)
			require.NoError(t, err)

			producer := newKafkaProducer(t, kafka.ProducerConfig{
				CommonConfig: kafka.CommonConfig{Logger: logger},
				Encoder:      codec,
				TopicRouter:  topicRouter,
				Sync:         tc.sync,
			})
			consumer := newKafkaConsumer(t, kafka.ConsumerConfig{
				CommonConfig: kafka.CommonConfig{Logger: logger},
				GroupID:      "group",
				Topics:       topics,
				Decoder:      codec,
				Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
					processed.Add(1)
					return nil
				}),
			})

			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()

			var wg sync.WaitGroup
			wg.Add(2)
			wait := make(chan struct{})
			signal := make(chan struct{})
			go func() {
				defer wg.Done()
				assert.NoError(t, producer.ProcessBatch(ctx, &model.Batch{
					model.APMEvent{Transaction: &model.Transaction{ID: "1", Custom: map[string]any{"foo": &stub{wait: wait, signal: signal}}}},
				}))
			}()
			<-signal
			go func() {
				defer wg.Done()
				close(wait)
				assert.NoError(t, producer.Close())
			}()

			wg.Wait()

			go func() { consumer.Run(ctx) }()
			assert.Eventually(t, func() bool {
				return processed.Load() == 1
			}, 20*time.Second, time.Millisecond, processed)
		})
		t.Run("PubSubLite/"+name, func(t *testing.T) {
			codec := json.JSON{}
			logger := zaptest.NewLogger(t, zaptest.Level(zapcore.InfoLevel))
			var processed atomic.Int64
			topics := SuffixTopics(apmqueue.Topic(t.Name()))
			topicRouter := func(event model.APMEvent) apmqueue.Topic {
				return apmqueue.Topic(topics[0])
			}

			err := ProvisionPubSubLite(context.Background(),
				newPubSubLiteConfig(topics...),
			)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()

			producer := newPubSubLiteProducer(t, pubsublite.ProducerConfig{
				CommonConfig: pubsublite.CommonConfig{Logger: logger},
				Encoder:      codec,
				TopicRouter:  topicRouter,
				Sync:         tc.sync,
			})
			consumer := newPubSubLiteConsumer(ctx, t, pubsublite.ConsumerConfig{
				CommonConfig: pubsublite.CommonConfig{Logger: logger},
				Topics:       topics,
				Decoder:      codec,
				Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
					processed.Add(1)
					return nil
				}),
			})

			var wg sync.WaitGroup
			wg.Add(2)
			wait := make(chan struct{})
			signal := make(chan struct{})
			go func() {
				defer wg.Done()
				assert.NoError(t, producer.ProcessBatch(ctx, &model.Batch{
					model.APMEvent{Transaction: &model.Transaction{ID: "1", Custom: map[string]any{"foo": &stub{wait: wait, signal: signal}}}},
				}))
			}()
			<-signal
			go func() {
				defer wg.Done()
				close(wait)
				assert.NoError(t, producer.Close())
			}()

			wg.Wait()

			go func() { consumer.Run(ctx) }()
			assert.Eventually(t, func() bool {
				return processed.Load() == 1
			}, 60*time.Second, time.Millisecond, processed)
		})
	}
}

func TestConsumerGracefulShutdown(t *testing.T) {
	testCases := map[string]struct {
		deliveryType apmqueue.DeliveryType
		timeout      time.Duration
	}{
		"at most once": {
			deliveryType: apmqueue.AtMostOnceDeliveryType,
			timeout:      90 * time.Second,
		},
		"at least once": {
			deliveryType: apmqueue.AtLeastOnceDeliveryType,
			timeout:      90 * time.Second,
		},
	}

	for name, tc := range testCases {
		t.Run("Kafka/"+name, func(t *testing.T) {
			logger := zaptest.NewLogger(t, zaptest.Level(zapcore.InfoLevel))
			codec := json.JSON{}
			var processed atomic.Int32
			var errored atomic.Int32
			process := make(chan struct{})
			records := 2
			topics := SuffixTopics(apmqueue.Topic(t.Name()))
			topicRouter := func(event model.APMEvent) apmqueue.Topic {
				return apmqueue.Topic(topics[0])
			}

			err := ProvisionKafka(context.Background(),
				newLocalKafkaConfig(topics...),
			)
			require.NoError(t, err)

			producer := newKafkaProducer(t, kafka.ProducerConfig{
				CommonConfig: kafka.CommonConfig{Logger: logger},
				Encoder:      codec,
				TopicRouter:  topicRouter,
			})
			consumer := newKafkaConsumer(t, kafka.ConsumerConfig{
				CommonConfig:   kafka.CommonConfig{Logger: logger},
				GroupID:        "group",
				Delivery:       tc.deliveryType,
				Topics:         topics,
				Decoder:        codec,
				MaxPollRecords: records,
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

			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()

			assert.NoError(t, producer.ProcessBatch(ctx, &model.Batch{
				model.APMEvent{Transaction: &model.Transaction{ID: "1"}},
				model.APMEvent{Transaction: &model.Transaction{ID: "2"}},
			}))
			assert.NoError(t, producer.Close())

			// Run a consumer that fetches from kafka to verify that the events are there.
			go func() { consumer.Run(ctx) }()
			select {
			case process <- struct{}{}:
				close(process) // Allow records to be processed
				cancel()       // Stop the consumer.
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for consumer to process event")
			}
			assert.Eventually(t, func() bool {
				return processed.Load() == int32(records) && errored.Load() == 0
			}, 20*time.Second, time.Millisecond, processed)
			t.Logf("got: %d events processed, %d errored, want: %d processed",
				processed.Load(), errored.Load(), records,
			)

		})
		t.Run("PubSubLite/"+name, func(t *testing.T) {
			codec := json.JSON{}
			logger := zaptest.NewLogger(t, zaptest.Level(zapcore.InfoLevel))
			var processed atomic.Int32
			var errored atomic.Int32
			process := make(chan struct{})
			records := 2
			topics := SuffixTopics(apmqueue.Topic(t.Name()))
			topicRouter := func(event model.APMEvent) apmqueue.Topic {
				return apmqueue.Topic(topics[0])
			}

			err := ProvisionPubSubLite(context.Background(),
				newPubSubLiteConfig(topics...),
			)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()

			producer := newPubSubLiteProducer(t, pubsublite.ProducerConfig{
				CommonConfig: pubsublite.CommonConfig{Logger: logger},
				Encoder:      codec,
				TopicRouter:  topicRouter,
				Sync:         true,
			})
			consumer := newPubSubLiteConsumer(ctx, t, pubsublite.ConsumerConfig{
				CommonConfig: pubsublite.CommonConfig{Logger: logger},
				Topics:       topics,
				Delivery:     tc.deliveryType,
				Decoder:      codec,
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

			assert.NoError(t, producer.ProcessBatch(ctx, &model.Batch{
				model.APMEvent{Transaction: &model.Transaction{ID: "1"}},
				model.APMEvent{Transaction: &model.Transaction{ID: "2"}},
			}))
			assert.NoError(t, producer.Close())

			// Run a consumer that fetches from kafka to verify that the events are there.
			go func() { consumer.Run(ctx) }()
			select {
			case process <- struct{}{}:
				close(process) // Allow records to be processed
				cancel()       // Stop the consumer.
			case <-time.After(60 * time.Second):
				t.Fatal("timed out waiting for consumer to process event")
			}
			assert.Eventually(t, func() bool {
				return processed.Load() == int32(records) && errored.Load() == 0
			}, 60*time.Second, time.Millisecond, processed)
			t.Logf("got: %d events processed, %d errored, want: %d processed",
				processed.Load(), errored.Load(), records,
			)
		})
	}
}
