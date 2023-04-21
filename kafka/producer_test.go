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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
	"github.com/elastic/apm-queue/queuecontext"
)

func TestNewProducer(t *testing.T) {
	_, err := NewProducer(ProducerConfig{})
	assert.Error(t, err)
}

func TestNewProducerBasic(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	defer tp.Shutdown(context.Background())

	// This test ensures that basic producing is working, it tests:
	// * Producing to a single topic
	// * Producing a set number of records
	// * Content contains headers from arbitrary metadata.
	// * Record.Value can be decoded with the same codec.
	test := func(t *testing.T, sync bool) {
		t.Run(fmt.Sprintf("sync_%t", sync), func(t *testing.T) {
			topic := "default-topic"
			client, brokers := newClusterWithTopics(t, topic)
			codec := json.JSON{}
			producer, err := NewProducer(ProducerConfig{
				Brokers: brokers,
				Sync:    sync,
				Logger:  zap.NewNop(),
				Encoder: codec,
				TopicRouter: func(event model.APMEvent) apmqueue.Topic {
					return apmqueue.Topic(topic)
				},
				TracerProvider: tp,
			})
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			ctx = queuecontext.WithMetadata(ctx, map[string]string{"a": "b", "c": "d"})
			batch := model.Batch{
				{Transaction: &model.Transaction{ID: "1"}},
				{Transaction: &model.Transaction{ID: "2"}},
			}

			spanCount := len(exp.GetSpans())
			if !sync {
				// Cancel the context before calling processBatch
				var c func()
				var ctxCancelled context.Context
				ctxCancelled, c = context.WithCancel(ctx)
				c()
				require.NoError(t, producer.ProcessBatch(ctxCancelled, &batch))
			} else {
				require.NoError(t, producer.ProcessBatch(ctx, &batch))
			}

			client.AddConsumeTopics(topic)
			for i := 0; i < len(batch); i++ {
				fetches := client.PollRecords(ctx, 1)
				require.NoError(t, fetches.Err())

				// Assert length.
				records := fetches.Records()
				assert.Len(t, records, 1)

				var event model.APMEvent
				record := records[0]
				err := codec.Decode(record.Value, &event)
				require.NoError(t, err)

				// Assert contents and decoding.
				assert.Equal(t, model.APMEvent{
					Transaction: &model.Transaction{ID: fmt.Sprint(i + 1)},
				}, event)

				// Sort headers and assert their existence.
				sort.Slice(record.Headers, func(i, j int) bool {
					return record.Headers[i].Key < record.Headers[j].Key
				})
				assert.Equal(t, []kgo.RecordHeader{
					{Key: "a", Value: []byte("b")},
					{Key: "c", Value: []byte("d")},
				}, record.Headers)
			}

			// Assert no more records have been produced. A nil context is used to
			// cause PollRecords to return immediately.
			//lint:ignore SA1012 passing a nil context is a valid use for this call.
			fetches := client.PollRecords(nil, 1)
			assert.Len(t, fetches.Records(), 0)

			// Assert tracing happened properly
			assert.Equal(t, len(exp.GetSpans()), spanCount+3)
			var span tracetest.SpanStub
			for _, s := range exp.GetSpans() {
				if s.Name == "producer.ProcessBatch" {
					span = s
				}
			}

			assert.Equal(t, "producer.ProcessBatch", span.Name)
			assert.Equal(t, []attribute.KeyValue{
				attribute.Bool("sync", sync),
				attribute.Int("batch.size", 2),
			}, span.Attributes)

			exp.Reset()
		})
	}
	test(t, true)
	test(t, false)
}

func newClusterWithTopics(t *testing.T, topics ...string) (*kgo.Client, []string) {
	t.Helper()
	cluster, err := kfake.NewCluster()
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	addrs := cluster.ListenAddrs()

	client, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	require.NoError(t, err)

	kadmClient := kadm.NewClient(client)
	t.Cleanup(kadmClient.Close)

	_, err = kadmClient.CreateTopics(context.Background(), 2, 1, nil, topics...)
	require.NoError(t, err)
	return client, addrs
}
