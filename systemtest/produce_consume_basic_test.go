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
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
	"github.com/elastic/apm-queue/kafka"
	"github.com/elastic/apm-queue/pubsublite"
)

func TestProduceConsumeSingleTopic(t *testing.T) {
	logger := NoLevelLogger(t, zap.ErrorLevel)
	events := 100
	timeout := 60 * time.Second
	for _, sync := range []bool{true, false} {
		name := "Async"
		if sync {
			name = "Sync"
		}
		topics := SuffixTopics(t.Name())
		topicRouter := func(event model.APMEvent) apmqueue.Topic {
			return apmqueue.Topic(topics[0])
		}
		t.Run("Kafka"+name, func(t *testing.T) {
			err := ProvisionKafka(context.Background(),
				newLocalKafkaConfig(topics...),
			)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			var records atomic.Int64
			testProduceConsume(ctx, t, produceConsumeCfg{
				events:  events,
				records: &records,
				producer: newKafkaProducer(t, kafka.ProducerConfig{
					Logger:      logger,
					Encoder:     json.JSON{},
					TopicRouter: topicRouter,
					Sync:        sync,
				}),
				consumer: newKafkaConsumer(t, kafka.ConsumerConfig{
					Logger:  logger,
					Decoder: json.JSON{},
					Topics:  topics,
					GroupID: t.Name(),
					Processor: model.ProcessBatchFunc(func(_ context.Context, b *model.Batch) error {
						for _, r := range *b {
							assert.Equal(t, r.Processor, model.TransactionProcessor, r)
							records.Add(1)
						}
						return nil
					}),
				}),
				timeout: timeout,
			})
		})
		t.Run("PubSubLite"+name, func(t *testing.T) {
			err := ProvisionPubSubLite(context.Background(),
				newPubSubLiteConfig(topics...),
			)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			var records atomic.Int64
			testProduceConsume(ctx, t, produceConsumeCfg{
				events:  events,
				records: &records,
				producer: newPubSubLiteProducer(ctx, t, pubsublite.ProducerConfig{
					Topics:      []apmqueue.Topic{apmqueue.Topic(topics[0])},
					Logger:      logger,
					Encoder:     json.JSON{},
					TopicRouter: topicRouter,
					Sync:        sync,
				}),
				consumer: newPubSubLiteConsumer(ctx, t, pubsublite.ConsumerConfig{
					Logger:        logger,
					Decoder:       json.JSON{},
					Subscriptions: pubSubLiteSubscriptions(topics...),
					Processor: model.ProcessBatchFunc(func(_ context.Context, b *model.Batch) error {
						for _, r := range *b {
							assert.Equal(t, r.Processor, model.TransactionProcessor, r)
							records.Add(1)
						}
						return nil
					}),
				}),
				timeout: timeout,
			})
		})
	}
}

type produceConsumeCfg struct {
	events   int
	producer apmqueue.Producer
	consumer apmqueue.Consumer
	records  *atomic.Int64
	timeout  time.Duration
}

func testProduceConsume(ctx context.Context, t testing.TB, cfg produceConsumeCfg) {
	// Run consumer and assert that the events are eventually set.
	go cfg.consumer.Run(ctx)
	batch := make(model.Batch, 0, cfg.events)
	for i := 0; i < cfg.events; i++ {
		batch = append(batch, model.APMEvent{
			Timestamp: time.Now(),
			Processor: model.TransactionProcessor,
			Trace:     model.Trace{ID: fmt.Sprintf("trace-%d", i+1)},
			Event: model.Event{
				Duration: time.Millisecond * (time.Duration(rand.Int63n(999)) + 1),
			},
			Transaction: &model.Transaction{
				ID: fmt.Sprintf("transaction-%d", i+1),
			},
		})
	}

	// Produce the records to queue.
	assert.NoError(t, cfg.producer.ProcessBatch(ctx, &batch))
	assert.Eventually(t, func() bool {
		return cfg.records.Load() == int64(cfg.events) // Assertion
	},
		cfg.timeout,          // Timeout
		100*time.Millisecond, // Poll
		"expected records (%d) records do not match consumed records (%d)", // ErrMessage
		cfg.events,
		cfg.records.Load(),
	)
}
