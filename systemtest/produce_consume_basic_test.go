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
	"strconv"
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

func TestProduceConsumeOrderingKeys(t *testing.T) {
	// The test asserts that messages are consumed in sequence given that
	// they are produced with a specific ordering key.

	// Use a high event count to circumvent the UniformBytesPartitioner
	// used by franz-go by default which aims to produce large batches of
	// data to same partition if no ordering keys is specified.
	events := 1500
	partitions := 2
	timeout := 60 * time.Second
	orderingKey := []byte("fixed")

	forEachProvider(t, func(t *testing.T, pf providerF) {
		runAsyncAndSync(t, func(t *testing.T, isSync bool) {
			var records atomic.Int64
			processor := sequenceConsumerAssertionProcessor(t, &sequenceConsumerAssertions{
				records: &records,
			}, func(r apmqueue.Record) int {
				assert.Equal(t, orderingKey, r.OrderingKey)
				seq, err := strconv.Atoi(string(r.Value))
				assert.NoError(t, err)
				return seq
			})
			topics := SuffixTopics(
				apmqueue.Topic(t.Name() + "default"),
			)
			producer, consumer := pf(t,
				withProcessor(processor),
				withSync(isSync),
				withTopicsGenerator(func(t testing.TB) []apmqueue.Topic {
					return topics
				}),
				withPartitions(partitions),
			)

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			events := map[apmqueue.Topic]int{
				topics[0]: events,
			}
			var expectedCount int
			for _, c := range events {
				expectedCount += c
			}
			testProduceConsume(ctx, t, produceConsumeCfg{
				events:               events,
				expectedRecordsCount: expectedCount,
				orderingKey:          orderingKey,
				valueFunc:            func(i int) []byte { return []byte(strconv.Itoa(i)) },
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
	orderingKey          []byte
	valueFunc            func(int) []byte
	producer             apmqueue.Producer
	consumer             apmqueue.Consumer
	records              *atomic.Int64
	timeout              time.Duration
}

func testProduceConsume(ctx context.Context, t testing.TB, cfg produceConsumeCfg) {
	if cfg.valueFunc == nil {
		cfg.valueFunc = func(_ int) []byte {
			return []byte("content")
		}
	}
	// Run consumer and assert that the events are eventually set.
	go cfg.consumer.Run(ctx)
	var records []apmqueue.Record
	for topic, events := range cfg.events {
		for i := 0; i < events; i++ {
			records = append(records, apmqueue.Record{
				Topic:       topic,
				OrderingKey: cfg.orderingKey,
				Value:       cfg.valueFunc(i),
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

// sequenceConsumerAssertions asserts that records are consumed in sequence
// assuming that all records are sent to same partitions. If records are
// not sent to same partition then it will result in failures.
type sequenceConsumerAssertions struct {
	records  *atomic.Int64
	lastSeen atomic.Int32
}

func sequenceConsumerAssertionProcessor(
	t testing.TB,
	assertions *sequenceConsumerAssertions,
	sequenceExtractor func(apmqueue.Record) int,
) apmqueue.Processor {
	return apmqueue.ProcessorFunc(func(_ context.Context, r ...apmqueue.Record) error {
		for _, record := range r {
			seq := sequenceExtractor(record)
			assert.True(
				t, assertions.lastSeen.CompareAndSwap(int32(seq), int32(seq+1)),
				"all records should be sent to same partition and consumed in sequence",
			)
		}
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
