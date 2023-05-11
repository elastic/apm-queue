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
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
	"github.com/elastic/apm-queue/kafka"
	"github.com/elastic/apm-queue/pubsublite"
)

// TestProduceConsumeDelivery verifies that a failure to process an event won't affect the
// other events in the same batch.
func TestProduceConsumeDelivery(t *testing.T) {
	testCases := map[string]struct {
		deliveryType         apmqueue.DeliveryType
		events               int
		replay               int
		expectedRecordsCount int
		timeout              time.Duration
	}{
		"at most once": {
			deliveryType:         apmqueue.AtMostOnceDeliveryType,
			events:               100,
			replay:               1,
			expectedRecordsCount: 199,
			timeout:              60 * time.Second,
		},
		"at least once": {
			deliveryType:         apmqueue.AtLeastOnceDeliveryType,
			events:               100,
			replay:               1,
			expectedRecordsCount: 199,
			timeout:              60 * time.Second,
		},
	}

	for name, tc := range testCases {
		t.Run("Kafka/"+name, func(t *testing.T) {
			topics := SuffixTopics(apmqueue.Topic(t.Name()))
			topicRouter := func(event model.APMEvent) apmqueue.Topic {
				return apmqueue.Topic(topics[0])
			}

			err := ProvisionKafka(context.Background(),
				newLocalKafkaConfig(topics...),
			)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			var records atomic.Int64
			var once sync.Once
			testProduceConsume(ctx, t, produceConsumeCfg{
				events:               tc.events,
				replay:               tc.replay,
				expectedRecordsCount: tc.expectedRecordsCount,
				records:              &records,
				producer: newKafkaProducer(t, kafka.ProducerConfig{
					Logger:      zap.NewNop(),
					Encoder:     json.JSON{},
					TopicRouter: topicRouter,
				}),
				consumer: newKafkaConsumer(t, kafka.ConsumerConfig{
					Logger:   zap.NewNop(),
					Decoder:  json.JSON{},
					Topics:   topics,
					GroupID:  t.Name(),
					Delivery: tc.deliveryType,
					Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
						var err error
						once.Do(func() {
							err = errors.New("first event error")
						})
						if err != nil {
							return err
						}
						return assertBatchFunc(t, consumerAssertions{
							records:   &records,
							processor: model.TransactionProcessor,
						}).ProcessBatch(ctx, b)
					}),
				}),
				timeout: tc.timeout,
			})
		})
		t.Run("PubSubLite/"+name, func(t *testing.T) {
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
			var records atomic.Int64
			var once sync.Once
			testProduceConsume(ctx, t, produceConsumeCfg{
				events:               tc.events,
				replay:               tc.replay,
				expectedRecordsCount: tc.expectedRecordsCount,
				records:              &records,
				producer: newPubSubLiteProducer(t, pubsublite.ProducerConfig{
					Logger:      zap.NewNop(),
					Encoder:     json.JSON{},
					TopicRouter: topicRouter,
				}),
				consumer: newPubSubLiteConsumer(ctx, t, pubsublite.ConsumerConfig{
					Logger:   zap.NewNop(),
					Decoder:  json.JSON{},
					Topics:   topics,
					Delivery: tc.deliveryType,
					Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
						var err error
						once.Do(func() {
							err = errors.New("first event error")
						})
						if err != nil {
							return err
						}
						return assertBatchFunc(t, consumerAssertions{
							records:   &records,
							processor: model.TransactionProcessor,
						}).ProcessBatch(ctx, b)
					}),
				}),
				timeout: tc.timeout,
			})
		})
	}
}

func TestProduceConsumeDeliveryGuarantees(t *testing.T) {
	codec := json.JSON{}
	testCases := map[string]struct {
		deliveryType         apmqueue.DeliveryType
		events               int
		timeout              time.Duration
		expectedRecordsCount int
	}{
		"at most once": {
			deliveryType:         apmqueue.AtMostOnceDeliveryType,
			events:               100,
			timeout:              120 * time.Second,
			expectedRecordsCount: 0,
		},
		"at least once": {
			deliveryType:         apmqueue.AtLeastOnceDeliveryType,
			events:               100,
			timeout:              120 * time.Second,
			expectedRecordsCount: 1,
		},
	}

	test := func(t *testing.T, ctx context.Context, producer apmqueue.Producer, records *atomic.Int64, errorConsumer apmqueue.Consumer, records2 *atomic.Int64, successConsumer apmqueue.Consumer, expectedRecordsCount int) {
		batch := model.Batch{
			model.APMEvent{
				Timestamp: time.Now(),
				Processor: model.TransactionProcessor,
				Trace:     model.Trace{ID: "trace"},
				Event: model.Event{
					Duration: time.Millisecond * (time.Duration(rand.Int63n(999)) + 1),
				},
				Transaction: &model.Transaction{
					ID: "transaction",
				},
			},
		}

		go errorConsumer.Run(ctx)
		assert.NoError(t, producer.ProcessBatch(ctx, &batch))

		assert.Eventually(t, func() bool {
			return records.Load() == 1 // Assertion
		},
			60*time.Second,       // Timeout
			100*time.Millisecond, // Poll
			"expected records (%d) records do not match consumed records (%v)", // ErrMessage
			1,
			records,
		)
		assert.NoError(t, errorConsumer.Close())

		go successConsumer.Run(ctx)

		assert.Eventually(t, func() bool {
			return records2.Load() == int64(expectedRecordsCount) // Assertion
		},
			60*time.Second,       // Timeout
			100*time.Millisecond, // Poll
			"expected records (%d) records do not match consumed records (%v)", // ErrMessage
			expectedRecordsCount,
			records2,
		)
	}

	for name, tc := range testCases {
		t.Run("Kafka/"+name, func(t *testing.T) {
			topics := SuffixTopics(apmqueue.Topic(t.Name()))
			topicRouter := func(event model.APMEvent) apmqueue.Topic {
				return apmqueue.Topic(topics[0])
			}

			err := ProvisionKafka(context.Background(),
				newLocalKafkaConfig(topics...),
			)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()

			producer := newKafkaProducer(t, kafka.ProducerConfig{
				Logger:      zap.NewNop(),
				Encoder:     codec,
				TopicRouter: topicRouter,
			})

			var records atomic.Int64
			errorConsumer := newKafkaConsumer(t, kafka.ConsumerConfig{
				Logger:   zap.NewNop(),
				Decoder:  codec,
				Topics:   topics,
				GroupID:  t.Name(),
				Delivery: tc.deliveryType,
				Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
					records.Add(1)
					return errors.New("first consumer processor error")
				}),
			})

			var records2 atomic.Int64
			successConsumer := newKafkaConsumer(t, kafka.ConsumerConfig{
				Logger:   zap.NewNop(),
				Decoder:  codec,
				Topics:   topics,
				GroupID:  t.Name(),
				Delivery: tc.deliveryType,
				Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
					records2.Add(1)
					return nil
				}),
			})

			test(t, ctx, producer, &records, errorConsumer, &records2, successConsumer, tc.expectedRecordsCount)
		})
		t.Run("PubSubLite/"+name, func(t *testing.T) {
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
				Logger:      zap.NewNop(),
				Encoder:     codec,
				TopicRouter: topicRouter,
			})

			var records atomic.Int64
			errorConsumer := newPubSubLiteConsumer(ctx, t, pubsublite.ConsumerConfig{
				Logger:   zap.NewNop(),
				Decoder:  codec,
				Topics:   topics,
				Delivery: tc.deliveryType,
				Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
					records.Add(1)
					return errors.New("first consumer processor error")
				}),
			})

			var records2 atomic.Int64
			successConsumer := newPubSubLiteConsumer(ctx, t, pubsublite.ConsumerConfig{
				Logger:   zap.NewNop(),
				Decoder:  codec,
				Topics:   topics,
				Delivery: tc.deliveryType,
				Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
					records2.Add(1)
					return nil
				}),
			})

			test(t, ctx, producer, &records, errorConsumer, &records2, successConsumer, tc.expectedRecordsCount)
		})
	}
}
