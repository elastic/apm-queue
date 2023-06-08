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
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
	"github.com/elastic/apm-queue/kafka"
	"github.com/elastic/apm-queue/pubsublite"
)

var skipKafka, skipPubsublite bool

type config struct {
	processor      model.BatchProcessor
	sync           bool
	dt             apmqueue.DeliveryType
	codec          universalEncoderDecoder
	maxPollRecords int
	loggerF        func(testing.TB) *zap.Logger
	topicsF        func(testing.TB) ([]apmqueue.Topic, func(model.APMEvent) apmqueue.Topic)
}

const (
	defaultProvisionTimeout    = 90 * time.Second
	defaultConsumerWaitTimeout = 90 * time.Second
	defaultConsumerExitTimeout = 20 * time.Second
)

var (
	defaultCfg = config{
		codec:   json.JSON{},
		sync:    true,
		loggerF: TestLogger,
		topicsF: func(t testing.TB) ([]apmqueue.Topic, func(model.APMEvent) apmqueue.Topic) {
			topics := SuffixTopics(apmqueue.Topic(t.Name()))
			topicRouter := func(model.APMEvent) apmqueue.Topic {
				return topics[0]
			}
			return topics, topicRouter
		},
	}
)

type option func(*config)

type universalEncoderDecoder interface {
	kafka.Encoder
	kafka.Decoder
	pubsublite.Encoder
	pubsublite.Decoder
}

type providerF func(testing.TB, ...option) (apmqueue.Producer, apmqueue.Consumer)

func forEachProvider(t *testing.T, f func(*testing.T, providerF)) {
	t.Run("Kafka", func(t *testing.T) {
		if skipKafka {
			t.SkipNow()
		}
		f(t, kafkaTypes)
	})
	t.Run("PubSubLite", func(t *testing.T) {
		if skipPubsublite {
			t.SkipNow()
		}
		f(t, pubSubTypes)
	})
}

func forEachDeliveryType(t *testing.T, f func(*testing.T, apmqueue.DeliveryType)) {
	t.Run("ALOD", func(t *testing.T) {
		f(t, apmqueue.AtLeastOnceDeliveryType)
	})
	t.Run("AMOD", func(t *testing.T) {
		f(t, apmqueue.AtMostOnceDeliveryType)
	})
}

func runAsyncAndSync(t *testing.T, f func(*testing.T, bool)) {
	t.Run("sync", func(t *testing.T) {
		f(t, true)
	})
	t.Run("async", func(t *testing.T) {
		f(t, false)
	})
}

func kafkaTypes(t testing.TB, opts ...option) (apmqueue.Producer, apmqueue.Consumer) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultProvisionTimeout)
	defer cancel()

	cfg := defaultCfg
	for _, opt := range opts {
		opt(&cfg)
	}

	logger := cfg.loggerF(t)
	topics, topicRouter := cfg.topicsF(t)
	CreateKafkaTopics(ctx, t, topics...)

	producer := newKafkaProducer(t, kafka.ProducerConfig{
		CommonConfig: kafka.CommonConfig{Logger: logger.Named("producer")},
		Encoder:      cfg.codec,
		TopicRouter:  topicRouter,
		Sync:         cfg.sync,
	})
	consumer := newKafkaConsumer(t, kafka.ConsumerConfig{
		CommonConfig: kafka.CommonConfig{Logger: logger.Named("consumer")},
		Decoder:      cfg.codec,
		Topics:       topics,
		GroupID:      t.Name(),
		Processor:    cfg.processor,
		Delivery:     cfg.dt,
	})
	return producer, consumer
}

func pubSubTypes(t testing.TB, opts ...option) (apmqueue.Producer, apmqueue.Consumer) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultProvisionTimeout)
	defer cancel()

	cfg := defaultCfg
	for _, opt := range opts {
		opt(&cfg)
	}

	logger := cfg.loggerF(t)
	topics, topicRouter := cfg.topicsF(t)
	subscriptions := make([]apmqueue.Subscription, len(topics))
	for i, topic := range topics {
		subscriptions[i] = apmqueue.Subscription{
			Name:  string(topic),
			Topic: topic,
		}
	}
	CreatePubsubTopics(ctx, t, topics...)

	producer := newPubSubLiteProducer(t, pubsublite.ProducerConfig{
		CommonConfig: pubsublite.CommonConfig{Logger: logger.Named("producer")},
		Encoder:      cfg.codec,
		TopicRouter:  topicRouter,
		Sync:         cfg.sync,
	})
	consumer := newPubSubLiteConsumer(context.Background(), t, pubsublite.ConsumerConfig{
		CommonConfig:  pubsublite.CommonConfig{Logger: logger.Named("consumer")},
		Decoder:       cfg.codec,
		Subscriptions: subscriptions,
		Processor:     cfg.processor,
		Delivery:      cfg.dt,
	})
	return producer, consumer
}

func withProcessor(p model.BatchProcessor) option {
	return func(c *config) {
		c.processor = p
	}
}

func withSync(sync bool) option {
	return func(c *config) {
		c.sync = sync
	}
}

func withDeliveryType(dt apmqueue.DeliveryType) option {
	return func(c *config) {
		c.dt = dt
	}
}

func withMaxPollRecords(max int) option {
	return func(c *config) {
		c.maxPollRecords = max
	}
}

func withLogger(f func(testing.TB) *zap.Logger) option {
	return func(c *config) {
		c.loggerF = f
	}
}

func withEncoderDecoder(u universalEncoderDecoder) option {
	return func(c *config) {
		c.codec = u
	}
}

func withTopic(topicsGenerator func(testing.TB) apmqueue.Topic) option {
	return func(c *config) {
		c.topicsF = func(t testing.TB) ([]apmqueue.Topic, func(model.APMEvent) apmqueue.Topic) {
			topic := topicsGenerator(t)
			topicRouter := func(model.APMEvent) apmqueue.Topic {
				return topic
			}

			return []apmqueue.Topic{topic}, topicRouter
		}
	}
}

func withTopicsGenerator(topicsGenerator func(testing.TB) []apmqueue.Topic, topicRouterGenerator func([]apmqueue.Topic) func(model.APMEvent) apmqueue.Topic) option {
	return func(c *config) {
		c.topicsF = func(t testing.TB) ([]apmqueue.Topic, func(model.APMEvent) apmqueue.Topic) {
			topics := topicsGenerator(t)
			topicRouter := topicRouterGenerator(topics)

			return topics, topicRouter
		}
	}
}
