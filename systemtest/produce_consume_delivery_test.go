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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
)

func TestConsumerDelivery(t *testing.T) {
	t.Skip()
	forEachProvider(t, func(t *testing.T, pf providerF) {
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
				failRecord := make(chan struct{})
				processRecord := make(chan struct{})
				defer close(failRecord)
				topic := SuffixTopics(apmqueue.Topic(t.Name()))[0]
				var processed atomic.Int32
				var errored atomic.Int32

				processor := model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
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
				})

				producer, consumer := pf(t,
					withProcessor(processor),
					withDeliveryType(tc.deliveryType),
					withLogger(func(t testing.TB) *zap.Logger {
						return zap.NewNop()
					}),
					withTopic(func(testing.TB) apmqueue.Topic {
						return topic
					}),
					withMaxPollRecords(tc.maxPollRecords),
				)

				// Context used for the consumer
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				batch := make(model.Batch, 0, tc.initialRecords)
				for i := 0; i < int(tc.initialRecords); i++ {
					batch = append(batch, model.APMEvent{Transaction: &model.Transaction{ID: strconv.Itoa(i)}})
				}
				require.NoError(t, producer.ProcessBatch(ctx, &batch))
				require.NoError(t, producer.Close())

				waitCh := make(chan struct{})
				go func() {
					consumer.Run(ctx)
					close(waitCh)
				}()
				defer func() {
					select {
					case <-waitCh:
					case <-time.After(defaultConsumerExitTimeout):
						t.Error("timed out waiting for consumer to exit")
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
				case <-time.After(defaultConsumerWaitTimeout):
					t.Fatal("timed out waiting for consumer to process event")
				}
				cancel()
				assert.NoError(t, consumer.Close())

				assert.Eventually(t, func() bool {
					return int(errored.Load()) == tc.maxPollRecords
				}, defaultConsumerWaitTimeout, time.Second)

				// Start a new consumer in the background and then produce
				ctx, cancel = context.WithCancel(context.Background())
				defer cancel()
				// Produce tc.lastRecords.
				producer, consumer = pf(t,
					withProcessor(processor),
					withDeliveryType(tc.deliveryType),
					withLogger(func(t testing.TB) *zap.Logger {
						return zap.NewNop()
					}),
					withTopic(func(t testing.TB) apmqueue.Topic {
						return topic
					}),
					withMaxPollRecords(tc.lastRecords),
				)

				batch = make(model.Batch, 0, tc.lastRecords)
				for i := 0; i < int(tc.lastRecords); i++ {
					batch = append(batch, model.APMEvent{Transaction: &model.Transaction{ID: strconv.Itoa(i)}})
				}
				producer.ProcessBatch(ctx, &batch)
				require.NoError(t, producer.Close())

				waitCh2 := make(chan struct{})
				go func() {
					consumer.Run(ctx)
					close(waitCh2)
				}()
				defer func() {
					cancel()
					consumer.Close()
					select {
					case <-waitCh2:
					case <-time.After(defaultConsumerExitTimeout):
						t.Error("timed out waiting for consumer to edit")
					}
				}()

				// Wait for the first record to be consumed before running any assertion.
				select {
				case processRecord <- struct{}{}:
					close(processRecord) // Allow records to be processed
				case <-time.After(defaultConsumerWaitTimeout):
					t.Fatal("timed out waiting for consumer to process event")
				}

				assert.Eventually(t, func() bool {
					// Some events may or may not be processed. Assert GE.
					return processed.Load() >= tc.processed &&
						errored.Load() == tc.errored
				}, defaultConsumerWaitTimeout, time.Second)
				t.Logf("got: %d events errored, %d processed, want: %d errored, %d processed",
					errored.Load(), processed.Load(), tc.errored, tc.processed,
				)
			})
		}
	})
}
