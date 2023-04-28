package systemtest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
	"github.com/elastic/apm-queue/kafka"
	"github.com/elastic/apm-queue/pubsublite"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
			expectedRecords: 100,
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
				producer: newPubSubLiteProducer(ctx, t, pubsublite.ProducerConfig{
					Topics:      []apmqueue.Topic{apmqueue.Topic(topics[0])},
					Logger:      logger,
					Encoder:     json.JSON{},
					TopicRouter: topicRouter,
				}),
				consumer: newPubSubLiteConsumer(ctx, t, pubsublite.ConsumerConfig{
					Logger:        logger,
					Decoder:       json.JSON{},
					Subscriptions: pubSubLiteSubscriptions(topics...),
					Delivery:      tc.deliveryType,
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
