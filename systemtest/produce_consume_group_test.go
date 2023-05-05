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

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
	"github.com/elastic/apm-queue/kafka"
	"github.com/elastic/apm-queue/pubsublite"
)

func TestProduceConsumeMultipleGroups(t *testing.T) {
	logger := NoLevelLogger(t, zap.ErrorLevel)

	testCases := map[string]struct {
		deliveryType    apmqueue.DeliveryType
		events          int
		replay          int
		expectedRecords int
		timeout         time.Duration
	}{
		"at most once": {
			deliveryType:    apmqueue.AtMostOnceDeliveryType,
			events:          100,
			replay:          1,
			expectedRecords: 199,
			timeout:         60 * time.Second,
		},
		"at least once": {
			deliveryType:    apmqueue.AtLeastOnceDeliveryType,
			events:          100,
			replay:          1,
			expectedRecords: 199,
			timeout:         60 * time.Second,
		},
	}

	for name, tc := range testCases {
		t.Run("Kafka/"+name, func(t *testing.T) {
			topics := SuffixTopics(apmqueue.Topic(t.Name()))
			topicRouter := func(event model.APMEvent) apmqueue.Topic {
				return apmqueue.Topic(topics[0])
			}

			err := ProvisionKafka(context.Background(),
				newLocalKafkaConfig(topics...),
			)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			var records atomic.Int64
			var once sync.Once
			testProduceConsume(ctx, t, produceConsumeCfg{
				events:          tc.events,
				replay:          tc.replay,
				expectedRecords: tc.expectedRecords,
				records:         &records,
				producer: newKafkaProducer(t, kafka.ProducerConfig{
					Logger:      logger,
					Encoder:     json.JSON{},
					TopicRouter: topicRouter,
				}),
				consumer: newKafkaConsumer(t, kafka.ConsumerConfig{
					Logger:   logger,
					Decoder:  json.JSON{},
					Topics:   topics,
					GroupID:  t.Name(),
					Delivery: tc.deliveryType,
					Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
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
					}),
				}),
				timeout: tc.timeout,
			})
		})
		t.Run("PubSubLite_"+name, func(t *testing.T) {
			topics := SuffixTopics(apmqueue.Topic(t.Name()))
			topicRouter := func(event model.APMEvent) apmqueue.Topic {
				return apmqueue.Topic(topics[0])
			}

			err := ProvisionPubSubLite(context.Background(),
				newPubSubLiteConfig(topics...),
			)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			var records atomic.Int64
			var once sync.Once
			testProduceConsume(ctx, t, produceConsumeCfg{
				events:          tc.events,
				replay:          tc.replay,
				expectedRecords: tc.expectedRecords,
				records:         &records,
				producer: newPubSubLiteProducer(t, pubsublite.ProducerConfig{
					Logger:      logger,
					Encoder:     json.JSON{},
					TopicRouter: topicRouter,
				}),
				consumer: newPubSubLiteConsumer(ctx, t, pubsublite.ConsumerConfig{
					Logger:   logger,
					Decoder:  json.JSON{},
					Topics:   topics,
					Delivery: tc.deliveryType,
					Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
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
					}),
				}),
				timeout: tc.timeout,
			})
		})
	}
}
