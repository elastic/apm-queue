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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
)

// TestProduceConsumeDelivery verifies that a failure to process an event won't affect the
// other events in the same batch.
func TestProduceConsumeDelivery(t *testing.T) {
	forEachProvider(t, func(t *testing.T, pf providerF) {
		forEachDeliveryType(t, func(t *testing.T, dt apmqueue.DeliveryType) {
			timeout := 60 * time.Second
			events := 100
			replay := 1
			expectedRecordsCount := 199

			var records atomic.Int64
			var once sync.Once
			processor := model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
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
			})

			producer, consumer := pf(t,
				withProcessor(processor),
				withDeliveryType(dt),
				withLogger(func(t testing.TB) *zap.Logger {
					return zaptest.NewLogger(t, zaptest.Level(zapcore.InfoLevel))
				}),
			)

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			testProduceConsume(ctx, t, produceConsumeCfg{
				events:               events,
				replay:               replay,
				expectedRecordsCount: expectedRecordsCount,
				records:              &records,
				producer:             producer,
				consumer:             consumer,
				timeout:              timeout,
			})
		})
	})
}

func TestProduceConsumeDeliveryGuarantees(t *testing.T) {
	forEachProvider(t, func(t *testing.T, pf providerF) {
		forEachDeliveryType(t, func(t *testing.T, dt apmqueue.DeliveryType) {
			timeout := 90 * time.Second
			expectedRecordsCount := 1
			topic := SuffixTopics(apmqueue.Topic(t.Name()))[0]

			var errRecords atomic.Int64
			errProcessor := model.ProcessBatchFunc(func(_ context.Context, b *model.Batch) error {
				errRecords.Add(int64(len(*b)))
				return errors.New("first consumer processor error")
			})

			producer, errConsumer := pf(t,
				withProcessor(errProcessor),
				withDeliveryType(dt),
				withTopic(func(testing.TB) apmqueue.Topic {
					return topic
				}),
			)

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

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
				defaultConsumerWaitTimeout, // Timeout
				1*time.Second,              // Poll
				"expected records (%d) records do not match consumed records (%v)", // ErrMessage
				1,
				errRecords,
			)
			cancel()
			assert.NoError(t, errConsumer.Close())

			var successRecords atomic.Int64
			successProcessor := model.ProcessBatchFunc(func(_ context.Context, b *model.Batch) error {
				successRecords.Add(int64(len(*b)))
				return nil
			})

			producer, successConsumer := pf(t,
				withProcessor(successProcessor),
				withDeliveryType(dt),
				withTopic(func(testing.TB) apmqueue.Topic {
					return topic
				}),
				withLogger(func(t testing.TB) *zap.Logger {
					return zaptest.NewLogger(t, zaptest.Level(zapcore.InfoLevel))
				}),
			)
			assert.NoError(t, producer.Close())

			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			defer cancel()

			go successConsumer.Run(ctx)

			assert.Eventually(t, func() bool {
				return successRecords.Load() == int64(expectedRecordsCount) // Assertion
			},
				defaultConsumerWaitTimeout, // Timeout
				1*time.Second,              // Poll
				"expected records (%d) records do not match consumed records (%v)", // ErrMessage
				expectedRecordsCount,
				successRecords,
			)
		})
	})
}
