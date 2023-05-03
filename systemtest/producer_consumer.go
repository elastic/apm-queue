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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/apm-queue/kafka"
	"github.com/elastic/apm-queue/pubsublite"
)

func newKafkaProducer(t testing.TB, cfg kafka.ProducerConfig) *kafka.Producer {
	cfg.Brokers = KafkaBrokers()
	// Use a patched TLS dialer for Kafka.
	cfg.Dialer = newKafkaTLSDialer().DialContext
	producer, err := kafka.NewProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := producer.Close()
		assert.NoError(t, err)
	})
	return producer
}

func newPubSubLiteProducer(t testing.TB, cfg pubsublite.ProducerConfig) *pubsublite.Producer {
	cfg.Project = googleProject
	cfg.Region = googleRegion
	producer, err := pubsublite.NewProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := producer.Close()
		assert.NoError(t, err)
	})
	return producer
}

func newKafkaConsumer(t testing.TB, cfg kafka.ConsumerConfig) *kafka.Consumer {
	cfg.Brokers = KafkaBrokers()
	// Use a patched TLS dialer for Kafka.
	cfg.Dialer = newKafkaTLSDialer().DialContext
	consumer, err := kafka.NewConsumer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := consumer.Close()
		assert.NoError(t, err)
	})
	return consumer
}

func newPubSubLiteConsumer(ctx context.Context, t testing.TB, cfg pubsublite.ConsumerConfig) *pubsublite.Consumer {
	cfg.Project = googleProject
	cfg.Region = googleRegion
	consumer, err := pubsublite.NewConsumer(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := consumer.Close()
		assert.NoError(t, err)
	})
	return consumer
}
