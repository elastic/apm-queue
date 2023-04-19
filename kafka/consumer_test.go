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
	"crypto/tls"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	"github.com/elastic/apm-queue/codec/json"
	saslplain "github.com/elastic/apm-queue/kafka/sasl/plain"
)

func TestNewConsumer(t *testing.T) {
	testCases := map[string]struct {
		expectErr bool
		cfg       ConsumerConfig
	}{
		"empty": {
			expectErr: true,
		},
		"invalid client consumer options": {
			cfg: ConsumerConfig{
				Brokers:   []string{"localhost:invalidport"},
				Topics:    []string{"topic"},
				GroupID:   "groupid",
				Decoder:   json.JSON{},
				Logger:    zap.NewNop(),
				Processor: model.ProcessBatchFunc(func(context.Context, *model.Batch) error { return nil }),
			},
			expectErr: true,
		},
		"valid": {
			cfg: ConsumerConfig{
				Brokers:   []string{"localhost:9092"},
				Topics:    []string{"topic"},
				GroupID:   "groupid",
				ClientID:  "clientid",
				Version:   "1.0",
				Decoder:   json.JSON{},
				Logger:    zap.NewNop(),
				Processor: model.ProcessBatchFunc(func(context.Context, *model.Batch) error { return nil }),
				SASL:      saslplain.New(saslplain.Plain{}),
				TLS:       &tls.Config{},
			},
			expectErr: false,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			consumer, err := NewConsumer(tc.cfg)
			if err == nil {
				defer assert.NoError(t, consumer.Close())
			}
			if tc.expectErr {
				assert.Error(t, err)
				assert.Nil(t, consumer)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, consumer)
			}
		})
	}
}

func TestConsumerHealth(t *testing.T) {
	testCases := map[string]struct {
		expectErr  bool
		closeEarly bool
	}{
		"success": {
			expectErr:  false,
			closeEarly: false,
		},
		"failure": {
			expectErr:  true,
			closeEarly: true,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cluster, err := kfake.NewCluster()
			require.NoError(t, err)

			addrs := cluster.ListenAddrs()
			consumer := newConsumer(t, ConsumerConfig{
				Brokers:   addrs,
				Topics:    []string{"topic"},
				GroupID:   "groupid",
				Decoder:   json.JSON{},
				Logger:    zap.NewNop(),
				Processor: model.ProcessBatchFunc(func(context.Context, *model.Batch) error { return nil }),
			})

			if tc.closeEarly {
				cluster.Close()
			} else {
				defer cluster.Close()
			}

			if tc.expectErr {
				assert.Error(t, consumer.Healthy())
			} else {
				assert.NoError(t, consumer.Healthy())
			}
		})
	}
}

func TestConsumerFetch(t *testing.T) {
	event := model.APMEvent{Transaction: &model.Transaction{ID: "1"}}
	topics := []string{"topic"}
	client, addrs := newClusterWithTopics(t, topics...)

	cfg := ConsumerConfig{
		Brokers: addrs,
		Topics:  topics,
		GroupID: "groupid",
		Decoder: json.JSON{},
		Logger:  zap.NewNop(),
		Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
			assert.Len(t, *b, 1)
			assert.Equal(t, event, (*b)[0])
			return nil
		}),
	}

	b, err := json.JSON{}.Encode(event)
	require.NoError(t, err)

	record := &kgo.Record{
		Topic: topics[0],
		Value: b,
	}

	results := client.ProduceSync(context.Background(), record)
	assert.NoError(t, results.FirstErr())

	r, err := results.First()
	assert.NoError(t, err)
	assert.NotNil(t, r)

	consumer := newConsumer(t, cfg)
	assert.NoError(t, consumer.fetch(context.Background()))
}

func TestMultipleConsumers(t *testing.T) {
	event := model.APMEvent{Transaction: &model.Transaction{ID: "1"}}
	topics := []string{"topic"}
	client, addrs := newClusterWithTopics(t, topics...)

	var count atomic.Int32
	cfg := ConsumerConfig{
		Brokers: addrs,
		Topics:  topics,
		GroupID: "groupid",
		Decoder: json.JSON{},
		Logger:  zap.NewNop(),
		Processor: model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
			count.Add(1)
			assert.Len(t, *b, 1)
			//assert.Equal(t, event, (*b)[0])
			return nil
		}),
	}

	b, err := json.JSON{}.Encode(event)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer1 := newConsumer(t, cfg)
	go func() {
		assert.ErrorIs(t, consumer1.Run(ctx), context.Canceled)
	}()

	waitCh := make(chan struct{})
	go func() {
		defer close(waitCh)
		for i := 0; i < 1000; i++ {
			results := client.ProduceSync(context.Background(), &kgo.Record{
				Topic: topics[0],
				Value: b,
			})
			assert.NoError(t, results.FirstErr())
			record, err := results.First()
			assert.NoError(t, err)
			assert.NotNil(t, record)
		}
	}()

	consumer2 := newConsumer(t, cfg)
	go func() {
		assert.ErrorIs(t, consumer2.Run(ctx), context.Canceled)
	}()

	<-waitCh

	assert.Eventually(t, func() bool {
		return count.Load() == 1000
	}, 1*time.Second, 50*time.Millisecond)
}

func TestMultipleConsumerGroups(t *testing.T) {
	event := model.APMEvent{Transaction: &model.Transaction{ID: "1"}}
	topics := []string{"topic"}
	codec := json.JSON{}
	client, addrs := newClusterWithTopics(t, topics...)

	cfg := ConsumerConfig{
		Brokers: addrs,
		Topics:  topics,
		Decoder: codec,
		Logger:  zap.NewNop(),
	}

	b, err := codec.Encode(event)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := make(map[string]*atomic.Int64)

	amount := 100

	for i := 0; i < amount; i++ {
		gid := "groupid" + strconv.Itoa(i)
		cfg.GroupID = gid
		m[gid] = &atomic.Int64{}
		cfg.Processor = model.ProcessBatchFunc(func(ctx context.Context, b *model.Batch) error {
			count := m[gid]
			count.Add(1)
			assert.Len(t, *b, 1)
			assert.Equal(t, event, (*b)[0])
			return nil
		})
		consumer := newConsumer(t, cfg)
		go func() {
			assert.ErrorIs(t, consumer.Run(ctx), context.Canceled)
		}()
	}

	waitCh := make(chan struct{})
	go func() {
		defer close(waitCh)
		for i := 0; i < amount; i++ {
			client.Produce(context.Background(), &kgo.Record{
				Topic: topics[0],
				Value: b,
			}, func(r *kgo.Record, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, r)
			})

		}
	}()

	<-waitCh

	for k, i := range m {
		assert.Eventually(t, func() bool {
			return i.Load() == int64(amount)
		}, 1*time.Second, 50*time.Millisecond, k)
	}
}

func TestConsumerRunError(t *testing.T) {
	consumer := newConsumer(t, ConsumerConfig{
		Brokers:   []string{"localhost:9092"},
		Topics:    []string{"topic"},
		GroupID:   "groupid",
		Decoder:   json.JSON{},
		Logger:    zap.NewNop(),
		Processor: model.ProcessBatchFunc(func(context.Context, *model.Batch) error { return nil }),
	})

	// ctx canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, consumer.Run(ctx))

	consumer.Close()
	require.Error(t, consumer.Run(context.Background()))
}

func newConsumer(t *testing.T, cfg ConsumerConfig) *Consumer {
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, consumer.Close())
	})
	return consumer
}
