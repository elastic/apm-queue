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
	"slices"
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
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue/v2"
	"github.com/elastic/apm-queue/v2/queuecontext"
)

func TestProducerMetrics(t *testing.T) {
	test := func(ctx context.Context,
		t *testing.T,
		producer apmqueue.Producer,
		rdr sdkmetric.Reader,
		want []metricdata.Metrics,
		ignore ...string,
	) {
		topic := apmqueue.Topic("default-topic")
		producer.Produce(ctx,
			apmqueue.Record{Topic: topic, Value: []byte("1")},
			apmqueue.Record{Topic: topic, Value: []byte("2")},
			apmqueue.Record{Topic: topic, Value: []byte("3")},
		)
		// Close the producer so records are flushed.
		require.NoError(t, producer.Close())

		var rm metricdata.ResourceMetrics
		assert.NoError(t, rdr.Collect(context.Background(), &rm))

		metrics := filterMetrics(t, rm.ScopeMetrics)
		assert.Equal(t, len(want)+len(ignore), len(metrics))
		for i := range metrics {
			t.Log(metrics[i].Name)
		}
		for i := range want {
			if !slices.Contains(ignore, metrics[i].Name) {
				metricdatatest.AssertEqual(t, want[i], metrics[i],
					metricdatatest.IgnoreTimestamp(),
				)
			}
		}
	}
	t.Run("DeadlineExceeded", func(t *testing.T) {
		producer, rdr := setupTestProducer(t, nil)
		want := []metricdata.Metrics{
			{
				Name:        "messaging.kafka.connect_errors.count",
				Description: "Total number of connection errors, by broker",
				Unit:        "1",
				Data: metricdata.Sum[int64]{
					Temporality: metricdata.CumulativeTemporality,
					IsMonotonic: true,
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Value: 1,
							Attributes: attribute.NewSet(
								semconv.MessagingSystem("kafka"),
							),
						},
					},
				},
			},
			{
				Name:        "producer.messages.count",
				Description: "The number of messages produced",
				Unit:        "1",
				Data: metricdata.Sum[int64]{
					Temporality: metricdata.CumulativeTemporality,
					IsMonotonic: true,
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Value: 3,
							Attributes: attribute.NewSet(
								attribute.String("outcome", "failure"),
								attribute.String(errorReasonKey, "timeout"),
								attribute.String("namespace", "name_space"),
								attribute.String("topic", "name_space-default-topic"),
								semconv.MessagingSystem("kafka"),
								semconv.MessagingDestinationName("default-topic"),
								semconv.MessagingKafkaDestinationPartition(0),
							),
						},
					},
				},
			},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()
		test(ctx, t, producer, rdr, want)
	})
	t.Run("ContextCanceled", func(t *testing.T) {
		producer, rdr := setupTestProducer(t, nil)
		want := []metricdata.Metrics{
			{
				Name:        "messaging.kafka.connect_errors.count",
				Description: "Total number of connection errors, by broker",
				Unit:        "1",
				Data: metricdata.Sum[int64]{
					Temporality: metricdata.CumulativeTemporality,
					IsMonotonic: true,
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Value: 1,
							Attributes: attribute.NewSet(
								semconv.MessagingSystem("kafka"),
							),
						},
					},
				},
			},
			{
				Name:        "producer.messages.count",
				Description: "The number of messages produced",
				Unit:        "1",
				Data: metricdata.Sum[int64]{
					Temporality: metricdata.CumulativeTemporality,
					IsMonotonic: true,
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Value: 3, Attributes: attribute.NewSet(
								attribute.String("outcome", "failure"),
								attribute.String(errorReasonKey, "canceled"),
								attribute.String("namespace", "name_space"),
								attribute.String("topic", "name_space-default-topic"),
								semconv.MessagingSystem("kafka"),
								semconv.MessagingDestinationName("default-topic"),
								semconv.MessagingKafkaDestinationPartition(0),
							),
						},
					},
				},
			},
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		test(ctx, t, producer, rdr, want)
	})
	t.Run("Unknown error reason", func(t *testing.T) {
		producer, rdr := setupTestProducer(t, nil)
		want := metricdata.Metrics{
			Name:        "producer.messages.count",
			Description: "The number of messages produced",
			Unit:        "1",
			Data: metricdata.Sum[int64]{
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 3,
						Attributes: attribute.NewSet(
							attribute.String("outcome", "failure"),
							attribute.String(errorReasonKey, "unknown"),
							attribute.String("namespace", "name_space"),
							attribute.String("topic", "name_space-default-topic"),
							semconv.MessagingSystem("kafka"),
							semconv.MessagingDestinationName("default-topic"),
							semconv.MessagingKafkaDestinationPartition(0),
						),
					},
				},
			},
		}
		require.NoError(t, producer.Close())
		test(context.Background(), t, producer, rdr, []metricdata.Metrics{want})
	})
	t.Run("Produced", func(t *testing.T) {
		producer, rdr := setupTestProducer(t, func(topic string) attribute.KeyValue {
			return attribute.String("test", "test")
		})
		want := []metricdata.Metrics{
			{
				Name:        "producer.messages.count",
				Description: "The number of messages produced",
				Unit:        "1",
				Data: metricdata.Sum[int64]{
					Temporality: metricdata.CumulativeTemporality,
					IsMonotonic: true,
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Value: 3,
							Attributes: attribute.NewSet(
								attribute.String("outcome", "success"),
								attribute.String("namespace", "name_space"),
								attribute.String("topic", "name_space-default-topic"),
								semconv.MessagingSystem("kafka"),
								semconv.MessagingDestinationName("default-topic"),
								semconv.MessagingKafkaDestinationPartition(0),
								attribute.String("test", "test"),
							),
						},
					},
				},
			},
			{
				Name:        "producer.messages.bytes",
				Description: "The number of bytes produced",
				Unit:        "1",
				Data: metricdata.Sum[int64]{
					Temporality: metricdata.CumulativeTemporality,
					IsMonotonic: true,
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Value: 24,
							Attributes: attribute.NewSet(
								attribute.String("outcome", "success"),
								attribute.String("namespace", "name_space"),
								attribute.String("topic", "name_space-default-topic"),
								semconv.MessagingSystem("kafka"),
								semconv.MessagingDestinationName("default-topic"),
								semconv.MessagingKafkaDestinationPartition(0),
								attribute.String("test", "test"),
							),
						},
					},
				},
			},
			{
				Name:        "producer.messages.uncompressed_bytes",
				Description: "The number of uncompressed bytes produced",
				Unit:        "1",
				Data: metricdata.Sum[int64]{
					Temporality: metricdata.CumulativeTemporality,
					IsMonotonic: true,
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Value: 24,
							Attributes: attribute.NewSet(
								attribute.String("outcome", "success"),
								attribute.String("namespace", "name_space"),
								attribute.String("topic", "name_space-default-topic"),
								semconv.MessagingSystem("kafka"),
								semconv.MessagingDestinationName("default-topic"),
								semconv.MessagingKafkaDestinationPartition(0),
								attribute.String("test", "test"),
							),
						},
					},
				},
			},
		}
		test(context.Background(), t, producer, rdr, want,
			"messaging.kafka.connects.count",
			"messaging.kafka.disconnects.count",
			"messaging.kafka.write_bytes",
			"messaging.kafka.read_bytes.count",
			"messaging.kafka.produce_bytes.count",
			"messaging.kafka.produce_records.count",
		)
	})
	t.Run("ProducedWithHeaders", func(t *testing.T) {
		producer, rdr := setupTestProducer(t, func(topic string) attribute.KeyValue {
			return attribute.String("some key", "some value")
		})
		want := []metricdata.Metrics{
			{
				Name:        "producer.messages.count",
				Description: "The number of messages produced",
				Unit:        "1",
				Data: metricdata.Sum[int64]{
					Temporality: metricdata.CumulativeTemporality,
					IsMonotonic: true,
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Value: 3,
							Attributes: attribute.NewSet(
								attribute.String("outcome", "success"),
								attribute.String("namespace", "name_space"),
								attribute.String("topic", "name_space-default-topic"),
								semconv.MessagingSystem("kafka"),
								semconv.MessagingDestinationName("default-topic"),
								semconv.MessagingKafkaDestinationPartition(0),
								attribute.String("some key", "some value"),
							),
						},
					},
				},
			},
			{
				Name:        "producer.messages.bytes",
				Description: "The number of bytes produced",
				Unit:        "By",
				Data: metricdata.Sum[int64]{
					Temporality: metricdata.CumulativeTemporality,
					IsMonotonic: true,
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Value: 53,
							Attributes: attribute.NewSet(
								attribute.String("outcome", "success"),
								attribute.String("namespace", "name_space"),
								attribute.String("topic", "name_space-default-topic"),
								semconv.MessagingSystem("kafka"),
								semconv.MessagingDestinationName("default-topic"),
								semconv.MessagingKafkaDestinationPartition(0),
								attribute.String("some key", "some value"),
							),
						},
					},
				},
			},
			{
				Name:        "producer.messages.uncompressed_bytes",
				Description: "The number of uncompressed bytes produced",
				Unit:        "By",
				Data: metricdata.Sum[int64]{
					Temporality: metricdata.CumulativeTemporality,
					IsMonotonic: true,
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Value: 114,
							Attributes: attribute.NewSet(
								attribute.String("outcome", "success"),
								attribute.String("namespace", "name_space"),
								attribute.String("topic", "name_space-default-topic"),
								semconv.MessagingSystem("kafka"),
								semconv.MessagingDestinationName("default-topic"),
								semconv.MessagingKafkaDestinationPartition(0),
								attribute.String("some key", "some value"),
							),
						},
					},
				},
			},
		}
		ctx := queuecontext.WithMetadata(context.Background(), map[string]string{
			"key":      "value",
			"some key": "some value",
		})
		test(ctx, t, producer, rdr, want,
			"messaging.kafka.connects.count",
			"messaging.kafka.disconnects.count",
			"messaging.kafka.write_bytes",
			"messaging.kafka.read_bytes.count",
			"messaging.kafka.produce_bytes.count",
			"messaging.kafka.produce_records.count",
		)
	})
}

func TestConsumerMetrics(t *testing.T) {
	records := 10

	done := make(chan struct{})
	var processed atomic.Int64
	proc := apmqueue.ProcessorFunc(func(_ context.Context, r apmqueue.Record) error {
		processed.Add(1)
		if processed.Load() == int64(records) {
			close(done)
		}
		return nil
	})
	tc := setupTestConsumer(t, proc, func(topic string) attribute.KeyValue {
		return attribute.String("header", "included")
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() { tc.consumer.Run(ctx) }() // Run Consumer.
	for i := 0; i < records; i++ {       // Produce records.
		produceRecord(ctx, t, tc.client, &kgo.Record{
			Topic: "name_space-" + t.Name(),
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

	wantMetrics := []metricdata.Metrics{
		{
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
							attribute.String("namespace", "name_space"),
							semconv.MessagingSystem("kafka"),
							semconv.MessagingSourceName(t.Name()),
							semconv.MessagingKafkaSourcePartition(0),
							attribute.String("header", "included"),
						),
					},
				},
			},
		},
		{
			Name:        "consumer.messages.delay",
			Description: "The delay between producing messages and reading them",
			Unit:        "s",
			Data: metricdata.Histogram[float64]{
				Temporality: metricdata.CumulativeTemporality,
				DataPoints: []metricdata.HistogramDataPoint[float64]{{
					Attributes: attribute.NewSet(
						attribute.String("namespace", "name_space"),
						semconv.MessagingSystem("kafka"),
						semconv.MessagingSourceName(t.Name()),
						semconv.MessagingKafkaSourcePartition(0),
						attribute.String("header", "included"),
					),

					Bounds: []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000},
					Count:  uint64(records),
				}},
			},
		},
	}

	ignore := []string{
		"messaging.kafka.connects.count",
		"messaging.kafka.write_bytes",
		"messaging.kafka.read_bytes.count",
		"messaging.kafka.fetch_bytes.count",
		"messaging.kafka.fetch_records.count",
		"consumer.messages.bytes",
		"consumer.messages.uncompressed_bytes",
	}

	metrics := filterMetrics(t, rm.ScopeMetrics)
	assert.Len(t, metrics, len(wantMetrics)+len(ignore))

	for k, m := range wantMetrics {
		metric := metrics[k]
		if slices.Contains(ignore, metric.Name) {
			continue
		}

		// Remove time-specific data for histograms
		if dp, ok := metric.Data.(metricdata.Histogram[float64]); ok {
			for k := range dp.DataPoints {
				dp.DataPoints[k].Min = m.Data.(metricdata.Histogram[float64]).DataPoints[k].Min
				dp.DataPoints[k].Max = m.Data.(metricdata.Histogram[float64]).DataPoints[k].Max
				dp.DataPoints[k].Sum = 0
				dp.DataPoints[k].BucketCounts = nil
			}
			metric.Data = dp
		}

		metricdatatest.AssertEqual(t,
			m,
			metric,
			metricdatatest.IgnoreTimestamp(),
			metricdatatest.IgnoreExemplars(),
		)
	}
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

func setupTestProducer(t testing.TB, tafunc TopicAttributeFunc) (*Producer, sdkmetric.Reader) {
	t.Helper()

	rdr := sdkmetric.NewManualReader()
	brokers := newClusterAddrWithTopics(t, 1, "name_space-default-topic")
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr))
	t.Cleanup(func() {
		require.NoError(t, mp.Shutdown(context.Background()))
	})
	producer := newProducer(t, ProducerConfig{
		CommonConfig: CommonConfig{
			Brokers:            brokers,
			Logger:             zap.NewNop(),
			Namespace:          "name_space",
			TracerProvider:     noop.NewTracerProvider(),
			MeterProvider:      mp,
			TopicAttributeFunc: tafunc,
		},
		Sync: true,
	})
	return producer, rdr
}

type testMetricConsumer struct {
	consumer *Consumer
	client   *kgo.Client
	reader   sdkmetric.Reader
}

func setupTestConsumer(t testing.TB, p apmqueue.Processor, tafunc TopicAttributeFunc) (mc testMetricConsumer) {
	t.Helper()

	mc.reader = sdkmetric.NewManualReader()
	cfg := ConsumerConfig{
		Topics:    []apmqueue.Topic{apmqueue.Topic(t.Name())},
		GroupID:   t.Name(),
		Processor: p,
		CommonConfig: CommonConfig{
			Logger:         zap.NewNop(),
			Namespace:      "name_space",
			TracerProvider: noop.NewTracerProvider(),
			MeterProvider: sdkmetric.NewMeterProvider(
				sdkmetric.WithReader(mc.reader),
			),
			TopicAttributeFunc: tafunc,
		},
	}
	mc.client, cfg.Brokers = newClusterWithTopics(t, 1, "name_space-"+t.Name())
	mc.consumer = newConsumer(t, cfg)
	return
}
