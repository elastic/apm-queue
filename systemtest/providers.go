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

	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/kafka"
	"github.com/elastic/apm-queue/pubsublite"
)

var skipKafka, skipPubsublite bool

type config struct {
	processor      apmqueue.Processor
	sync           bool
	dt             apmqueue.DeliveryType
	maxPollRecords int
	loggerF        func(testing.TB) *zap.Logger
	topicsF        func(testing.TB) []apmqueue.Topic
	partitions     int
}

const (
	defaultProvisionTimeout    = 90 * time.Second
	defaultConsumerWaitTimeout = 90 * time.Second
	defaultConsumerExitTimeout = 20 * time.Second
)

var (
	defaultCfg = config{
		sync:    true,
		loggerF: TestLogger,
		topicsF: func(t testing.TB) []apmqueue.Topic {
			topics := SuffixTopics(apmqueue.Topic(t.Name()))
			return topics
		},
		partitions: 1,
	}
)

type option func(*config)

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
	topics := cfg.topicsF(t)
	CreateKafkaTopics(ctx, t, cfg.partitions, topics...)

	producer := newKafkaProducer(t, kafka.ProducerConfig{
		CommonConfig: kafka.CommonConfig{Logger: logger.Named("producer")},
		Sync:         cfg.sync,
	})
	consumer := newKafkaConsumer(t, kafka.ConsumerConfig{
		CommonConfig: kafka.CommonConfig{Logger: logger.Named("consumer")},
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
	topics := cfg.topicsF(t)
	consumerName := "test_consumer"
	CreatePubsubTopics(ctx, t, cfg.partitions, topics...)
	CreatePubsubTopicSubscriptions(ctx, t, consumerName, topics...)

	producer := newPubSubLiteProducer(t, pubsublite.ProducerConfig{
		CommonConfig: pubsublite.CommonConfig{Logger: logger.Named("producer")},
		Sync:         cfg.sync,
	})
	consumer := newPubSubLiteConsumer(context.Background(), t, pubsublite.ConsumerConfig{
		CommonConfig: pubsublite.CommonConfig{Logger: logger.Named("consumer")},
		Topics:       topics,
		ConsumerName: consumerName,
		Processor:    cfg.processor,
		Delivery:     cfg.dt,
	})
	return producer, consumer
}

func withProcessor(p apmqueue.Processor) option {
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

func withTopic(topicsGenerator func(testing.TB) apmqueue.Topic) option {
	return func(c *config) {
		c.topicsF = func(t testing.TB) []apmqueue.Topic {
			return []apmqueue.Topic{topicsGenerator(t)}
		}
	}
}

func withTopicsGenerator(topicsGenerator func(testing.TB) []apmqueue.Topic) option {
	return func(c *config) {
		c.topicsF = topicsGenerator
	}
}

func withPartitions(count int) option {
	return func(c *config) {
		c.partitions = count
	}
}
