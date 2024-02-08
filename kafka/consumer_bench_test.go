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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	apmqueue "github.com/elastic/apm-queue"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type consumerBench struct {
	maxPollRecords int
	topics         int
	partitions     int
	records        int
}

func BenchmarkConsumer(b *testing.B) {
	// NOTE(marclop) maxPollRecords is iterated first.
	scenarios := []consumerBench{
		// 1 record per topic
		{
			topics:     1,
			partitions: 1,
			records:    1,
		},
		{
			topics:     1,
			partitions: 10,
			records:    1,
		},
		{
			topics:     10,
			partitions: 10,
			records:    1,
		},
		{
			topics:     100,
			partitions: 10,
			records:    1,
		},
		// 10 records
		{
			topics:     1,
			partitions: 1,
			records:    10,
		},
		{
			topics:     1,
			partitions: 10,
			records:    10,
		},
		{
			topics:     10,
			partitions: 10,
			records:    10,
		},
		{
			topics:     100,
			partitions: 10,
			records:    10,
		},
		// 1000 topics, 2 partitions
		{
			topics:     1000,
			partitions: 2,
			records:    10,
		},
		// 50 records
		{
			topics:     1,
			partitions: 1,
			records:    50,
		},
		{
			topics:     1,
			partitions: 10,
			records:    50,
		},
		{
			topics:     10,
			partitions: 10,
			records:    50,
		},
		{
			topics:     100,
			partitions: 10,
			records:    50,
		},
	}
	for _, maxPoll := range []int{100, 200, 300, 400, 500} {
		b.Run(fmt.Sprintf("%dMaxPoll", maxPoll), func(b *testing.B) {
			for _, cfg := range scenarios {
				cfg.maxPollRecords = maxPoll
				name := fmt.Sprintf("%d max_poll %d topic %d partition %d records",
					cfg.maxPollRecords, cfg.topics, cfg.partitions, cfg.records,
				)
				b.Run(name, func(b *testing.B) {
					benchmarkConsumer(b, cfg)
				})
			}
		})
	}
}

func benchmarkConsumer(b *testing.B, bCfg consumerBench) {
	topics := make([]string, bCfg.topics)
	for i := 0; i < len(topics); i++ {
		topics[i] = fmt.Sprint("topic_", i)
	}
	cfg := CommonConfig{Logger: zap.NewNop()}
	_, cfg.Brokers = newClusterWithTopics(b, int32(bCfg.partitions), topics...)
	producer := newProducer(b, ProducerConfig{CommonConfig: cfg})

	var consumedRecords atomic.Int64
	consumer := newConsumer(b, ConsumerConfig{
		CommonConfig:   cfg,
		ConsumeRegex:   true,
		GroupID:        b.Name(),
		MaxPollRecords: bCfg.maxPollRecords,
		Topics:         []apmqueue.Topic{apmqueue.Topic("topic.*")},
		Processor: apmqueue.ProcessorFunc(
			func(_ context.Context, r ...apmqueue.Record) error {
				consumedRecords.Add(int64(len(r)))
				return nil
			},
		),
	})
	// start the consumer in the background and report errors.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errs := make(chan error)
	go func() {
		errs <- consumer.Run(ctx)
	}()

	records := make([]apmqueue.Record, 0, len(topics)+bCfg.records)
	for i := 0; i < bCfg.records; i++ {
		for _, topic := range topics {
			records = append(records, apmqueue.Record{
				Topic: apmqueue.Topic(topic),
				Value: []byte(
					"random string of bytes produced to the kafka queue",
				),
			})
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			producer.Produce(ctx, records...)
		}
	})
	cancel()
	assert.NoError(b, consumer.Close())
	<-errs
	b.ReportMetric(float64(consumedRecords.Load())/b.Elapsed().Seconds(), "consumed/s")
}
