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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	apmqueue "github.com/elastic/apm-queue/v2"
)

func TestGracefulShutdownProducer(t *testing.T) {
	forEachProvider(t, func(t *testing.T, pf providerF) {
		runAsyncAndSync(t, func(t *testing.T, isSync bool) {
			var processed atomic.Int64
			topic := SuffixTopics(apmqueue.Topic(t.Name()))[0]
			processor := apmqueue.ProcessorFunc(func(_ context.Context, r apmqueue.Record) error {
				processed.Add(1)
				return nil
			})
			producer, consumer := pf(t,
				withProcessor(processor),
				withSync(isSync),
				withTopic(func(t testing.TB) apmqueue.Topic { return topic }),
			)

			assert.NoError(t, producer.Produce(context.Background(), apmqueue.Record{
				Topic: topic,
				Value: []byte("content"),
			}))
			assert.NoError(t, producer.Close())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { consumer.Run(ctx) }()

			assert.Eventually(t, func() bool {
				return processed.Load() == 1
			}, defaultConsumerWaitTimeout, time.Second)
			if t.Failed() {
				t.Log(processed.Load())
			}
		})
	})
}

func TestGracefulShutdownConsumer(t *testing.T) {
	forEachProvider(t, func(t *testing.T, pf providerF) {
		forEachDeliveryType(t, func(t *testing.T, dt apmqueue.DeliveryType) {
			var processed atomic.Int64
			signal := make(chan struct{})
			processor := apmqueue.ProcessorFunc(func(ctx context.Context, r apmqueue.Record) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case signal <- struct{}{}:
				}
				processed.Add(1)
				return nil
			})
			topic := SuffixTopics(apmqueue.Topic(t.Name()))[0]
			producer, consumer := pf(t,
				withProcessor(processor),
				withDeliveryType(dt),
				withTopic(func(t testing.TB) apmqueue.Topic { return topic }),
			)

			assert.NoError(t, producer.Produce(context.Background(), apmqueue.Record{
				Topic: topic,
				Value: []byte("content"),
			}))
			assert.NoError(t, producer.Close())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { consumer.Run(ctx) }()

			// wait for the event to be received, cancel the context and close the consumer
			<-signal
			cancel()
			consumer.Close()

			assert.Eventually(t, func() bool {
				return processed.Load() == 1
			}, defaultConsumerWaitTimeout, time.Second, processed)
		})
	})
}
