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
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
)

func TestProduceConsumeSingleTopic(t *testing.T) {
	// This test covers:
	// - TopicRouter publishes to a topic, regardless of the event content.
	// - Consumer consumes from a single topic.
	// - No errors are logged.
	events := 100
	timeout := 60 * time.Second

	forEachProvider(func(name string, pf providerF) {
		t.Run(name, func(t *testing.T) {
			runAsyncAndSync(func(name string, isSync bool) {
				t.Run(name, func(t *testing.T) {
					var records atomic.Int64
					processor := assertBatchFunc(t, consumerAssertions{
						records:   &records,
						processor: model.TransactionProcessor,
					})
					producer, consumer := pf(t, withProcessor(processor), withSync(isSync))

					ctx, cancel := context.WithTimeout(context.Background(), timeout)
					defer cancel()

					testProduceConsume(ctx, t, produceConsumeCfg{
						events:               events,
						expectedRecordsCount: events,
						records:              &records,
						producer:             producer,
						consumer:             consumer,
						timeout:              timeout,
					})
				})
			})
		})
	})
}

func TestProduceConsumeMultipleTopics(t *testing.T) {
	// This test covers:
	// - TopicRouter publishes to different topics based on event contents.
	// - Consumer can consume from more than one topic.
	// - No errors are logged.
	events := 100
	timeout := 60 * time.Second

	forEachProvider(func(name string, pf providerF) {
		t.Run(name, func(t *testing.T) {
			runAsyncAndSync(func(name string, isSync bool) {
				t.Run(name, func(t *testing.T) {
					var records atomic.Int64
					processor := assertBatchFunc(t, consumerAssertions{
						records:   &records,
						processor: model.TransactionProcessor,
					})
					producer, consumer := pf(t,
						withProcessor(processor),
						withSync(isSync),
						withTopicsGenerator(func(t testing.TB) []apmqueue.Topic {
							return SuffixTopics(
								apmqueue.Topic(t.Name()+"Even"),
								apmqueue.Topic(t.Name()+"Odd"),
							)
						}, func(topics []apmqueue.Topic) func(model.APMEvent) apmqueue.Topic {
							return func(event model.APMEvent) apmqueue.Topic {
								if event.Event.Duration%2 == 0 {
									return topics[0]
								}
								return topics[1]

							}
						}),
					)

					ctx, cancel := context.WithTimeout(context.Background(), timeout)
					defer cancel()

					testProduceConsume(ctx, t, produceConsumeCfg{
						events:               events,
						expectedRecordsCount: events,
						records:              &records,
						producer:             producer,
						consumer:             consumer,
						timeout:              timeout,
					})
				})
			})
		})
	})
}

type produceConsumeCfg struct {
	events               int
	replay               int
	expectedRecordsCount int
	producer             apmqueue.Producer
	consumer             apmqueue.Consumer
	records              *atomic.Int64
	timeout              time.Duration
}

func testProduceConsume(ctx context.Context, t testing.TB, cfg produceConsumeCfg) {
	// Run consumer and assert that the events are eventually set.
	go cfg.consumer.Run(ctx)
	for j := 0; j < cfg.replay+1; j++ {
		batch := make(model.Batch, 0, cfg.events)
		for i := 0; i < cfg.events; i++ {
			batch = append(batch, model.APMEvent{
				Timestamp: time.Now(),
				Processor: model.TransactionProcessor,
				Trace:     model.Trace{ID: fmt.Sprintf("trace%d-%d", j, i+1)},
				Event: model.Event{
					Duration: time.Millisecond * (time.Duration(rand.Int63n(999)) + 1),
				},
				Transaction: &model.Transaction{
					ID: fmt.Sprintf("transaction%d-%d", j, i+1),
				},
			})
		}

		// Produce the records to queue.
		assert.NoError(t, cfg.producer.ProcessBatch(ctx, &batch))
		if cfg.records == nil {
			return
		}
	}
	assert.NoError(t, cfg.producer.Close())
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
	processor model.Processor
	records   *atomic.Int64
}

func assertBatchFunc(t testing.TB, assertions consumerAssertions) model.BatchProcessor {
	return model.ProcessBatchFunc(func(_ context.Context, b *model.Batch) error {
		assert.Greater(t, len(*b), 0)
		for _, r := range *b {
			assert.Equal(t, assertions.processor, r.Processor, r)
			if assertions.records != nil {
				assertions.records.Add(1)
			}
		}
		return nil
	})
}

func TestShutdown(t *testing.T) {
	forEachProvider(func(name string, pf providerF) {
		t.Run(name, func(t *testing.T) {
			runAsyncAndSync(func(name string, isSync bool) {
				t.Run(name, func(t *testing.T) {
					type stopFunc func(context.CancelFunc, apmqueue.Consumer)

					for name, stop := range map[string]stopFunc{
						"ctx":   func(cancel context.CancelFunc, _ apmqueue.Consumer) { cancel() },
						"close": func(_ context.CancelFunc, c apmqueue.Consumer) { assert.NoError(t, c.Close()) },
					} {
						t.Run(name, func(t *testing.T) {
							received := make(chan struct{})
							processor := model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
								close(received)
								return nil
							})
							producer, consumer := pf(t, withProcessor(processor), withSync(isSync))

							assert.NoError(t, producer.ProcessBatch(context.Background(), &model.Batch{
								model.APMEvent{Transaction: &model.Transaction{ID: "1"}},
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
							case <-time.After(120 * time.Second):
								t.Error("timed out while waiting to receive an event")
							}

							stopCh := make(chan struct{})
							go func() {
								stop(cancel, consumer)
								close(stopCh)
							}()
							select {
							case <-stopCh:
							case <-time.After(120 * time.Second):
								t.Error("timed out while stopping consumer")
							}

							select {
							case <-closeCh:
							case <-time.After(120 * time.Second):
								t.Error("timed out while waiting for consumer to exit")
							}
						})
					}
				})
			})
		})
	})
}
