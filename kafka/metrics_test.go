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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/queuecontext"
)

func TestProducerMetrics(t *testing.T) {
	test := func(ctx context.Context,
		t *testing.T,
		producer apmqueue.Producer,
		rdr sdkmetric.Reader,
		want metricdata.Metrics,
	) {
		topic := apmqueue.Topic("default-topic")
		producer.Produce(ctx,
			apmqueue.Record{Topic: topic, Value: []byte("1")},
			apmqueue.Record{Topic: topic, Value: []byte("2")},
			apmqueue.Record{Topic: topic, Value: []byte("3")},
		)

		var rm metricdata.ResourceMetrics
		assert.NoError(t, rdr.Collect(context.Background(), &rm))

		metrics := filterMetrics(t, rm.ScopeMetrics)
		assert.Len(t, metrics, 1)
		metricdatatest.AssertEqual(t, want, metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	}
	t.Run("DeadlineExceeded", func(t *testing.T) {
		producer, rdr := setupTestProducer(t)
		want := metricdata.Metrics{
			Name:        "producer.messages.errored",
			Description: "The number of messages that failed to be produced",
			Unit:        "1",
			Data: metricdata.Sum[int64]{
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 3, Attributes: attribute.NewSet(
							attribute.String("error", "timeout"),
							semconv.MessagingDestinationName("default-topic"),
							semconv.MessagingKafkaDestinationPartition(0),
						),
					},
				},
			},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()
		test(ctx, t, producer, rdr, want)
	})
	t.Run("ContextCanceled", func(t *testing.T) {
		producer, rdr := setupTestProducer(t)
		want := metricdata.Metrics{
			Name:        "producer.messages.errored",
			Description: "The number of messages that failed to be produced",
			Unit:        "1",
			Data: metricdata.Sum[int64]{
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 3, Attributes: attribute.NewSet(
							attribute.String("error", "canceled"),
							semconv.MessagingDestinationName("default-topic"),
							semconv.MessagingKafkaDestinationPartition(0),
						),
					},
				},
			},
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		test(ctx, t, producer, rdr, want)
	})
	t.Run("Other", func(t *testing.T) {
		producer, rdr := setupTestProducer(t)
		want := metricdata.Metrics{
			Name:        "producer.messages.errored",
			Description: "The number of messages that failed to be produced",
			Unit:        "1",
			Data: metricdata.Sum[int64]{
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 3,
						Attributes: attribute.NewSet(
							attribute.String("error", "other"),
							semconv.MessagingDestinationName("default-topic"),
							semconv.MessagingKafkaDestinationPartition(0),
						),
					},
				},
			},
		}
		require.NoError(t, producer.Close())
		test(context.Background(), t, producer, rdr, want)
	})
	t.Run("Produced", func(t *testing.T) {
		producer, rdr := setupTestProducer(t)
		want := metricdata.Metrics{
			Name:        "producer.messages.produced",
			Description: "The number of messages produced",
			Unit:        "1",
			Data: metricdata.Sum[int64]{
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 3,
						Attributes: attribute.NewSet(
							semconv.MessagingDestinationName("default-topic"),
							semconv.MessagingKafkaDestinationPartition(0),
						),
					},
				},
			},
		}
		test(context.Background(), t, producer, rdr, want)
	})
	t.Run("ProducedWithHeaders", func(t *testing.T) {
		producer, rdr := setupTestProducer(t)
		want := metricdata.Metrics{
			Name:        "producer.messages.produced",
			Description: "The number of messages produced",
			Unit:        "1",
			Data: metricdata.Sum[int64]{
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 3,
						Attributes: attribute.NewSet(
							semconv.MessagingDestinationName("default-topic"),
							semconv.MessagingKafkaDestinationPartition(0),
							attribute.String("key", "value"),
							attribute.String("some key", "some value"),
						),
					},
				},
			},
		}
		ctx := queuecontext.WithMetadata(context.Background(), map[string]string{
			"key":      "value",
			"some key": "some value",
		})
		test(ctx, t, producer, rdr, want)
	})
}

func TestConsumerMetrics(t *testing.T) {
	topic := apmqueue.Topic(t.Name())
	records := 10

	done := make(chan struct{})
	var processed atomic.Int64
	proc := apmqueue.ProcessorFunc(func(_ context.Context, r ...apmqueue.Record) error {
		processed.Add(int64(len(r)))
		if processed.Load() == int64(records) {
			close(done)
		}
		return nil
	})
	tc := setupTestConsumer(t, proc, topic)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() { tc.consumer.Run(ctx) }() // Run Consumer.
	for i := 0; i < records; i++ {       // Produce records.
		produceRecord(ctx, t, tc.client, &kgo.Record{
			Topic: string(topic),
			Value: []byte(fmt.Sprint(i)),
			Headers: []kgo.RecordHeader{
				{Key: "header", Value: []byte("included")},
				{Key: "traceparent", Value: []byte("excluded")},
			},
		})
	}

	select {
	case <-ctx.Done():
		t.Error("Timed out while waiting for records to be consumed")
		return
	case <-done:
	}

	var rm metricdata.ResourceMetrics
	assert.NoError(t, tc.reader.Collect(context.Background(), &rm))

	metrics := filterMetrics(t, rm.ScopeMetrics)
	assert.Len(t, metrics, 1)
	metricdatatest.AssertEqual(t, metricdata.Metrics{
		Name:        msgFetchedKey,
		Description: "The number of messages that were fetched from a kafka topic",
		Unit:        "1",
		Data: metricdata.Sum[int64]{
			Temporality: metricdata.CumulativeTemporality,
			IsMonotonic: true,
			DataPoints: []metricdata.DataPoint[int64]{
				{
					Value: int64(records),
					Attributes: attribute.NewSet(
						semconv.MessagingDestinationName(t.Name()),
						semconv.MessagingKafkaDestinationPartition(0),
						attribute.String("header", "included"),
					),
				},
			},
		},
	}, metrics[0], metricdatatest.IgnoreTimestamp())
}

func filterMetrics(t testing.TB, sm []metricdata.ScopeMetrics) []metricdata.Metrics {
	t.Helper()

	for _, m := range sm {
		if m.Scope.Name == instrumentName {
			return m.Metrics
		}
	}
	t.Fatal("unable to find metrics for", instrumentName)
	return []metricdata.Metrics{}
}

func setupTestProducer(t testing.TB) (*Producer, sdkmetric.Reader) {
	t.Helper()

	rdr := sdkmetric.NewManualReader()
	topic := apmqueue.Topic("default-topic")
	_, brokers := newClusterWithTopics(t, 1, topic)
	producer, err := NewProducer(ProducerConfig{
		CommonConfig: CommonConfig{
			Brokers:        brokers,
			Logger:         zap.NewNop(),
			TracerProvider: trace.NewNoopTracerProvider(),
			MeterProvider: sdkmetric.NewMeterProvider(
				sdkmetric.WithReader(rdr),
			),
		},
		Sync: true,
	})
	require.NoError(t, err)
	require.NotNil(t, producer)
	return producer, rdr
}

type testMetricConsumer struct {
	consumer *Consumer
	client   *kgo.Client
	reader   sdkmetric.Reader
}

func setupTestConsumer(t testing.TB, p apmqueue.Processor, topics ...apmqueue.Topic) (mc testMetricConsumer) {
	t.Helper()

	mc.reader = sdkmetric.NewManualReader()
	cfg := ConsumerConfig{
		Topics:    topics,
		GroupID:   t.Name(),
		Processor: p,
		CommonConfig: CommonConfig{
			Logger:         zap.NewNop(),
			TracerProvider: trace.NewNoopTracerProvider(),
			MeterProvider: sdkmetric.NewMeterProvider(
				sdkmetric.WithReader(mc.reader),
			),
		},
	}
	mc.client, cfg.Brokers = newClusterWithTopics(t, 1, topics...)
	mc.consumer = newConsumer(t, cfg)
	return
}
