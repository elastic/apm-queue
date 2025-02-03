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

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func assertNotNilOptions(t testing.TB, cfg *ConsumerConfig) {
	t.Helper()

	assert.NotNil(t, cfg.Processor)
	cfg.Processor = nil
	assert.NotNil(t, cfg.Logger)
	cfg.Logger = nil
	assert.NotNil(t, cfg.TopicAttributeFunc)
	cfg.TopicAttributeFunc = nil
}

func newConsumer(t testing.TB, cfg ConsumerConfig) *Consumer {
	if cfg.MaxPollWait <= 0 {
		// Lower MaxPollWait, ShutdownGracePeriod to speed up execution.
		cfg.MaxPollWait = 50 * time.Millisecond
		cfg.ShutdownGracePeriod = time.Second
	}
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, consumer.Close())
	})
	return consumer
}

func produceRecord(ctx context.Context, t testing.TB, c *kgo.Client, r *kgo.Record) {
	t.Helper()
	results := c.ProduceSync(ctx, r)
	assert.NoError(t, results.FirstErr())
	r, err := results.First()
	assert.NoError(t, err)
	assert.NotNil(t, r)
}

func zapTest(t testing.TB, opts ...zaptest.LoggerOption) *zap.Logger {
	t.Helper()
	if len(opts) == 0 {
		opts = append(opts, zaptest.Level(zap.InfoLevel))
	}
	return zaptest.NewLogger(t, opts...)
}

func withZapLevel(level zapcore.Level) zaptest.LoggerOption {
	return zaptest.Level(level)
}

func newProducer(t testing.TB, cfg ProducerConfig) *Producer {
	t.Helper()
	producer, err := NewProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, producer.Close())
	})
	return producer
}

func newFakeCluster(t testing.TB) (*kfake.Cluster, CommonConfig) {
	cluster, err := kfake.NewCluster(
		// Just one broker to simplify dealing with sharded requests.
		kfake.NumBrokers(1),
	)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	return cluster, CommonConfig{
		Brokers:   cluster.ListenAddrs(),
		Logger:    zap.NewNop(),
		Namespace: "name_space",
	}
}

func newClusterAddrWithTopics(t testing.TB, partitions int32, topics ...string) []string {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(partitions, topics...))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	return cluster.ListenAddrs()
}

func newClusterWithTopics(t testing.TB, partitions int32, topics ...string) (*kgo.Client, []string) {
	t.Helper()
	addrs := newClusterAddrWithTopics(t, partitions, topics...)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		// Reduce the max wait time to speed up tests.
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	require.NoError(t, err)

	return client, addrs
}

func getComittedOffsets(ctx context.Context, t testing.TB,
	c *kadm.Client, group string,
) map[string]int64 {
	t.Helper()
	res, err := c.FetchOffsets(ctx, group)
	require.NoError(t, err)

	offsets := make(map[string]int64)
	res.Offsets().Each(func(o kadm.Offset) {
		// if _, ok := offsets[o.Topic]; !ok {
		// 	offsets[o.Topic]++
		// }
		offsets[o.Topic] = o.At
	})
	return offsets
}
