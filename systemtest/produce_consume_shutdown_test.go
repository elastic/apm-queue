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

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
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
	timeout := 90 * time.Second
	forEachProvider(t, func(t *testing.T, pf providerF) {
		forEachDeliveryType(t, func(t *testing.T, dt apmqueue.DeliveryType) {
			runAsyncAndSync(t, func(t *testing.T, isSync bool) {
				var processed atomic.Int64
				processor := model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
					processed.Add(1)
					return nil
				})

				producer, consumer := pf(t, withProcessor(processor), withSync(isSync), withDeliveryType(dt))

				var wg sync.WaitGroup
				wg.Add(2)
				wait := make(chan struct{})
				signal := make(chan struct{})
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()

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
				}, 60*time.Second, time.Second, processed)
			})
		})
	})
}

func TestConsumerGracefulShutdown(t *testing.T) {
	timeout := 90 * time.Second
	forEachProvider(t, func(t *testing.T, pf providerF) {
		forEachDeliveryType(t, func(t *testing.T, dt apmqueue.DeliveryType) {
			runAsyncAndSync(t, func(t *testing.T, isSync bool) {
				records := 2
				var processed atomic.Int32
				var errored atomic.Int32
				process := make(chan struct{})
				processor := model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
					select {
					case <-ctx.Done():
						errored.Add(int32(len(*b)))
						return ctx.Err()
					case <-process:
						processed.Add(int32(len(*b)))
					}
					return nil
				})

				producer, consumer := pf(t, withProcessor(processor), withSync(isSync), withDeliveryType(dt))

				ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
				case <-time.After(60 * time.Second):
					t.Fatal("timed out waiting for consumer to process event")
				}
				assert.Eventually(t, func() bool {
					return processed.Load() == int32(records) && errored.Load() == 0
				}, 90*time.Second, time.Second, processed)
				t.Logf("got: %d events processed, %d errored, want: %d processed",
					processed.Load(), errored.Load(), records,
				)
			})
		})
	})
}
