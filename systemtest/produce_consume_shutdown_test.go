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

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
)

type hookedCodec struct {
	u             universalEncoderDecoder
	blockEncoding bool
	blockDecoding bool
	signal        chan struct{}
}

func (s *hookedCodec) Encode(e model.APMEvent) ([]byte, error) {
	if s.blockEncoding {
		s.signal <- struct{}{}
	}
	return s.u.Encode(e)
}

func (s *hookedCodec) Decode(b []byte, e *model.APMEvent) error {
	if s.blockDecoding {
		s.signal <- struct{}{}
	}
	return s.u.Decode(b, e)
}

func newHookedCodec(t *testing.T, blockEncoding bool, blockDecoding bool) (*hookedCodec, chan struct{}) {
	signal := make(chan struct{})
	codec := &hookedCodec{
		u:             json.JSON{},
		blockDecoding: blockDecoding,
		blockEncoding: blockEncoding,
		signal:        signal,
	}
	t.Cleanup(func() {
		close(codec.signal)
	})
	return codec, signal
}

func TestGracefulShutdownProducer(t *testing.T) {
	forEachProvider(t, func(t *testing.T, pf providerF) {
		runAsyncAndSync(t, func(t *testing.T, isSync bool) {
			var processed atomic.Int64
			processor := model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
				processed.Add(1)
				return nil
			})

			codec, signal := newHookedCodec(t, true, false)
			producer, consumer := pf(t, withProcessor(processor), withSync(isSync), withEncoderDecoder(codec))

			go func() {
				assert.NoError(t, producer.ProcessBatch(context.Background(), &model.Batch{
					model.APMEvent{Transaction: &model.Transaction{ID: "1"}},
				}))
			}()

			// wait for events to be encoded and then try to close the producer
			<-signal
			assert.NoError(t, producer.Close())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() { consumer.Run(ctx) }()
			assert.Eventually(t, func() bool {
				return processed.Load() == 1
			}, defaultConsumerWaitTimeout, time.Second, processed)
		})
	})
}

func TestGracefulShutdownConsumer(t *testing.T) {
	forEachProvider(t, func(t *testing.T, pf providerF) {
		forEachDeliveryType(t, func(t *testing.T, dt apmqueue.DeliveryType) {
			var processed atomic.Int32
			processor := model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
				processed.Add(1)
				return nil
			})

			codec, signal := newHookedCodec(t, false, true)
			producer, consumer := pf(t, withProcessor(processor), withDeliveryType(dt), withEncoderDecoder(codec))

			assert.NoError(t, producer.ProcessBatch(context.Background(), &model.Batch{
				model.APMEvent{Transaction: &model.Transaction{ID: "1"}},
			}))
			assert.NoError(t, producer.Close())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() { consumer.Run(ctx) }()

			// wait for the event to be decoded, cancel the context and close the consumer
			<-signal
			cancel()
			consumer.Close()

			assert.Eventually(t, func() bool {
				return processed.Load() == 1
			}, defaultConsumerWaitTimeout, time.Second, processed)
		})
	})
}
