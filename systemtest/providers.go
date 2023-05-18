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

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
	"github.com/elastic/apm-queue/kafka"
	"github.com/elastic/apm-queue/pubsublite"
)

type config struct {
	processor model.BatchProcessor
	sync      bool
	codec     universalEncoderDecoder
	loggerF   func(testing.TB) *zap.Logger
	topicsF   func(testing.TB) ([]apmqueue.Topic, func(model.APMEvent) apmqueue.Topic)
}

var (
	defaultCfg = config{
		codec: json.JSON{},
		sync:  true,
		loggerF: func(t testing.TB) *zap.Logger {
			return NoLevelLogger(t, zap.ErrorLevel)
		},
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

type providerF func(context.Context, testing.TB, ...option) (apmqueue.Producer, apmqueue.Consumer)

func forEachProvider(f func(string, providerF)) {
	f("Kafka", kafkaTypes)
	f("PubSubLite", pubSubTypes)
}

func runAsyncAndSync(f func(string, bool)) {
	f("sync", true)
	f("async", false)
}

func kafkaTypes(ctx context.Context, t testing.TB, opts ...option) (apmqueue.Producer, apmqueue.Consumer) {
	cfg := defaultCfg
	for _, opt := range opts {
		opt(&cfg)
	}

	logger := cfg.loggerF(t)
	topics, topicRouter := cfg.topicsF(t)

	require.NoError(t, ProvisionKafka(ctx, newLocalKafkaConfig(topics...)))

	producer := newKafkaProducer(t, kafka.ProducerConfig{
		CommonConfig: kafka.CommonConfig{Logger: logger},
		Encoder:      cfg.codec,
		TopicRouter:  topicRouter,
		Sync:         cfg.sync,
	})
	consumer := newKafkaConsumer(t, kafka.ConsumerConfig{
		CommonConfig: kafka.CommonConfig{Logger: logger},
		Decoder:      cfg.codec,
		Topics:       topics,
		GroupID:      t.Name(),
		Processor:    cfg.processor,
	})

	return producer, consumer
}

func pubSubTypes(ctx context.Context, t testing.TB, opts ...option) (apmqueue.Producer, apmqueue.Consumer) {
	cfg := defaultCfg
	for _, opt := range opts {
		opt(&cfg)
	}

	logger := cfg.loggerF(t)
	topics, topicRouter := cfg.topicsF(t)

	require.NoError(t, ProvisionPubSubLite(ctx, newPubSubLiteConfig(topics...)))

	producer := newPubSubLiteProducer(t, pubsublite.ProducerConfig{
		CommonConfig: pubsublite.CommonConfig{Logger: logger},
		Encoder:      cfg.codec,
		TopicRouter:  topicRouter,
		Sync:         cfg.sync,
	})
	consumer := newPubSubLiteConsumer(ctx, t, pubsublite.ConsumerConfig{
		CommonConfig: pubsublite.CommonConfig{Logger: logger},
		Decoder:      cfg.codec,
		Topics:       topics,
		Processor:    cfg.processor,
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
