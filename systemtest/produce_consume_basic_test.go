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

	apmqueue "github.com/elastic/apm-queue"
)

func TestProduceConsumeMultipleTopics(t *testing.T) {
	// This test covers:
	// - TopicRouter publishes to different topics.
	// - Consumer can consume from more than one topic.
	// - No errors are logged.
	events := 50
	timeout := 60 * time.Second

	forEachProvider(t, func(t *testing.T, pf providerF) {
		runAsyncAndSync(t, func(t *testing.T, isSync bool) {
			var records atomic.Int64
			processor := assertProcessor(t, consumerAssertions{
				records: &records,
			})
			topics := SuffixTopics(
				apmqueue.Topic(t.Name()+"Even"),
				apmqueue.Topic(t.Name()+"Odd"),
			)
			producer, consumer := pf(t,
				withProcessor(processor),
				withSync(isSync),
				withTopicsGenerator(func(t testing.TB) []apmqueue.Topic {
					return topics
				}),
			)

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			events := map[apmqueue.Topic]int{
				topics[0]: events,
				topics[1]: events,
			}
			var expectedCount int
			for _, c := range events {
				expectedCount += c
			}
			testProduceConsume(ctx, t, produceConsumeCfg{
				events:               events,
				expectedRecordsCount: expectedCount,
				records:              &records,
				producer:             producer,
				consumer:             consumer,
				timeout:              timeout,
			})
		})
	})
}

type produceConsumeCfg struct {
	events               map[apmqueue.Topic]int
	expectedRecordsCount int
	producer             apmqueue.Producer
	consumer             apmqueue.Consumer
	records              *atomic.Int64
	timeout              time.Duration
}

func testProduceConsume(ctx context.Context, t testing.TB, cfg produceConsumeCfg) {
	// Run consumer and assert that the events are eventually set.
	go cfg.consumer.Run(ctx)
	var records []apmqueue.Record
	for topic, events := range cfg.events {
		for i := 0; i < events; i++ {
			records = append(records, apmqueue.Record{
				Topic: topic,
				Value: []byte("content"),
			})
		}
	}
	// Produce the records to queue.
	assert.NoError(t, cfg.producer.Produce(ctx, records...))
	assert.NoError(t, cfg.producer.Close())
	if cfg.records == nil {
		return
	}
	assert.Eventually(t, func() bool {
		return cfg.records.Load() == int64(cfg.expectedRecordsCount) // Assertion
	},
		cfg.timeout,          // Timeout
		100*time.Millisecond, // Poll
		"expected records (%d) records do not match consumed records (%v)", // ErrMessage
		cfg.expectedRecordsCount,
		cfg.records,
	)
}

type consumerAssertions struct {
	records *atomic.Int64
}

func assertProcessor(t testing.TB, assertions consumerAssertions) apmqueue.Processor {
	return apmqueue.ProcessorFunc(func(_ context.Context, r ...apmqueue.Record) error {
		assert.Greater(t, len(r), 0)
		assertions.records.Add(int64(len(r)))
		return nil
	})
}

func TestShutdown(t *testing.T) {
	forEachProvider(t, func(t *testing.T, pf providerF) {
		type stopFunc func(context.CancelFunc, apmqueue.Consumer)
		for name, stop := range map[string]stopFunc{
			"ctx":   func(cancel context.CancelFunc, _ apmqueue.Consumer) { cancel() },
			"close": func(_ context.CancelFunc, c apmqueue.Consumer) { assert.NoError(t, c.Close()) },
		} {
			t.Run(name, func(t *testing.T) {
				received := make(chan struct{})
				processor := apmqueue.ProcessorFunc(func(context.Context, ...apmqueue.Record) error {
					close(received)
					return nil
				})
				topics := SuffixTopics(apmqueue.Topic(t.Name()))
				producer, consumer := pf(t,
					withProcessor(processor),
					withTopicsGenerator(func(t testing.TB) []apmqueue.Topic {
						return topics
					}),
				)

				assert.NoError(t, producer.Produce(context.Background(), apmqueue.Record{
					Topic: topics[0],
				}))
				assert.NoError(t, producer.Close())

				closeCh := make(chan struct{})
				ctx, cancel := context.WithCancel(context.Background())

				// cleanup
				defer func() {
					cancel()
					consumer.Close()
				}()

				go func() {
					// TODO this is failing
					//assert.Equal(t, expectedErr, consumer.Run(ctx))
					consumer.Run(ctx)
					close(closeCh)
				}()
				select {
				case <-received:
				case <-time.After(defaultConsumerWaitTimeout):
					t.Error("timed out while waiting to receive an event")
				}

				stopCh := make(chan struct{})
				go func() {
					stop(cancel, consumer)
					close(stopCh)
				}()
				select {
				case <-stopCh:
				case <-time.After(10 * time.Second):
					t.Error("timed out while stopping consumer")
				}

				select {
				case <-closeCh:
				case <-time.After(defaultConsumerExitTimeout):
					t.Error("timed out while waiting for consumer to exit")
				}
			})
		}
	})
}
