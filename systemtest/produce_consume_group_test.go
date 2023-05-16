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
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

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
			logger := zap.NewNop()
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
					Logger:      logger,
					Encoder:     json.JSON{},
					TopicRouter: topicRouter,
				}),
				consumer: newKafkaConsumer(t, kafka.ConsumerConfig{
					Logger:   logger,
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
			logger := zap.NewNop()
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
					Logger:      logger,
					Encoder:     json.JSON{},
					TopicRouter: topicRouter,
				}),
				consumer: newPubSubLiteConsumer(ctx, t, pubsublite.ConsumerConfig{
					Logger:   logger,
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
			timeout:              90 * time.Second,
			expectedRecordsCount: 0,
		},
		"at least once": {
			deliveryType:         apmqueue.AtLeastOnceDeliveryType,
			events:               100,
			timeout:              90 * time.Second,
			expectedRecordsCount: 1,
		},
	}

	type consumerF func(testing.TB, string, model.BatchProcessor) apmqueue.Consumer
	test := func(t *testing.T, ctx context.Context, producer apmqueue.Producer, newConsumer consumerF, expectedRecordsCount int) {
		var errRecords atomic.Int64
		errConsumer := newConsumer(t, "err_consumer",
			model.ProcessBatchFunc(func(_ context.Context, b *model.Batch) error {
				errRecords.Add(int64(len(*b)))
				return errors.New("first consumer processor error")
			}),
		)
		go errConsumer.Run(ctx)

		batch := model.Batch{model.APMEvent{
			Timestamp:   time.Now(),
			Processor:   model.TransactionProcessor,
			Trace:       model.Trace{ID: "trace"},
			Transaction: &model.Transaction{ID: "transaction"},
			Event: model.Event{
				Duration: time.Millisecond * (time.Duration(rand.Int63n(999)) + 1),
			},
		}}
		// Produce record and close the producer.
		assert.NoError(t, producer.ProcessBatch(ctx, &batch))
		assert.NoError(t, producer.Close())

		assert.Eventually(t, func() bool {
			return errRecords.Load() == 1 // Expect to receive 1 error.
		},
			60*time.Second,       // Timeout
			100*time.Millisecond, // Poll
			"expected records (%d) records do not match consumed records (%v)", // ErrMessage
			1,
			errRecords,
		)
		assert.NoError(t, errConsumer.Close())

		var successRecords atomic.Int64
		successConsumer := newConsumer(t, "ok_consumer",
			model.ProcessBatchFunc(func(_ context.Context, b *model.Batch) error {
				successRecords.Add(int64(len(*b)))
				return nil
			}),
		)
		go successConsumer.Run(ctx)

		assert.Eventually(t, func() bool {
			return successRecords.Load() == int64(expectedRecordsCount) // Assertion
		},
			60*time.Second,       // Timeout
			100*time.Millisecond, // Poll
			"expected records (%d) records do not match consumed records (%v)", // ErrMessage
			expectedRecordsCount,
			successRecords,
		)
	}

	for name, tc := range testCases {
		t.Run("Kafka/"+name, func(t *testing.T) {
			logger := zaptest.NewLogger(t, zaptest.Level(zapcore.InfoLevel))
			// logger := zap.NewNop()
			topics := SuffixTopics(apmqueue.Topic(t.Name()))
			topicRouter := func(event model.APMEvent) apmqueue.Topic {
				return apmqueue.Topic(topics[0])
			}
			require.NoError(t, ProvisionKafka(context.Background(),
				newLocalKafkaConfig(topics...),
			))

			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			producer := newKafkaProducer(t, kafka.ProducerConfig{
				Logger:      logger.Named("producer"),
				Encoder:     codec,
				TopicRouter: topicRouter,
			})
			consumerFunc := func(t testing.TB, name string, bp model.BatchProcessor) apmqueue.Consumer {
				return newKafkaConsumer(t, kafka.ConsumerConfig{
					Logger:         logger.Named(name),
					Decoder:        codec,
					Topics:         topics,
					GroupID:        t.Name(),
					Delivery:       tc.deliveryType,
					MaxPollRecords: 1, // Wait for 1 record to be fetched.
					Processor:      bp,
				})
			}
			test(t, ctx, producer, consumerFunc, tc.expectedRecordsCount)
		})
		t.Run("PubSubLite/"+name, func(t *testing.T) {
			// logger := zap.NewNop()
			logger := zaptest.NewLogger(t, zaptest.Level(zapcore.InfoLevel))
			topics := SuffixTopics(apmqueue.Topic(t.Name()))
			topicRouter := func(event model.APMEvent) apmqueue.Topic {
				return apmqueue.Topic(topics[0])
			}
			require.NoError(t, ProvisionPubSubLite(context.Background(),
				newPubSubLiteConfig(topics...),
			))

			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			producer := newPubSubLiteProducer(t, pubsublite.ProducerConfig{
				Logger:      logger,
				Encoder:     codec,
				TopicRouter: topicRouter,
				Sync:        true,
			})
			consumerFunc := func(t testing.TB, name string, bp model.BatchProcessor) apmqueue.Consumer {
				return newPubSubLiteConsumer(ctx, t, pubsublite.ConsumerConfig{
					Logger:    logger.Named(name),
					Decoder:   codec,
					Topics:    topics,
					Delivery:  tc.deliveryType,
					Processor: bp,
				})
			}
			test(t, ctx, producer, consumerFunc, tc.expectedRecordsCount)
		})
	}
}
