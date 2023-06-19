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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	apmqueue "github.com/elastic/apm-queue"
)

// TestProduceConsumeDelivery verifies that a failure to process an event won't affect the
// other events in the same batch.
func TestProduceConsumeDelivery(t *testing.T) {
	forEachProvider(t, func(t *testing.T, pf providerF) {
		forEachDeliveryType(t, func(t *testing.T, dt apmqueue.DeliveryType) {
			timeout := 60 * time.Second
			topic := SuffixTopics(apmqueue.Topic(t.Name()))[0]
			recordCount := 200
			expectedRecordsCount := recordCount - 1 // Expect 1 record to be dropped.

			var records atomic.Int64
			var once sync.Once
			proc := assertProcessor(t, consumerAssertions{records: &records})
			processor := apmqueue.ProcessorFunc(func(ctx context.Context, r ...apmqueue.Record) error {
				var err error
				once.Do(func() {
					err = errors.New("first event error")
				})
				if err != nil {
					return err
				}
				return proc.Process(ctx, r...)
			})

			producer, consumer := pf(t,
				withProcessor(processor),
				withDeliveryType(dt),
				withTopic(func(t testing.TB) apmqueue.Topic {
					return topic
				}),
			)

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			testProduceConsume(ctx, t, produceConsumeCfg{
				events:               map[apmqueue.Topic]int{topic: recordCount},
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
			topic := SuffixTopics(apmqueue.Topic(t.Name()))[0]

			var errRecords atomic.Int64
			errProcessor := apmqueue.ProcessorFunc(func(_ context.Context, r ...apmqueue.Record) error {
				errRecords.Add(int64(len(r)))
				return errors.New("first consumer processor error")
			})

			producer, errConsumer := pf(t,
				withProcessor(errProcessor),
				withDeliveryType(dt),
				withTopic(func(testing.TB) apmqueue.Topic {
					return topic
				}),
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go errConsumer.Run(ctx)

			record := apmqueue.Record{Topic: topic, Value: []byte("content")}
			// Produce record and close the producer.
			assert.NoError(t, producer.Produce(context.Background(), record))
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
			successProcessor := apmqueue.ProcessorFunc(func(_ context.Context, r ...apmqueue.Record) error {
				successRecords.Add(int64(len(r)))
				return nil
			})

			producer, successConsumer := pf(t,
				withProcessor(successProcessor),
				withDeliveryType(dt),
				withTopic(func(testing.TB) apmqueue.Topic {
					return topic
				}),
			)
			assert.NoError(t, producer.Close())

			ctx, cancel = context.WithCancel(context.Background())
			defer cancel()

			go successConsumer.Run(ctx)

			var expectedRecordsCount int64
			if dt == apmqueue.AtMostOnceDeliveryType {
				expectedRecordsCount = 0
			} else {
				expectedRecordsCount = 1
			}

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
