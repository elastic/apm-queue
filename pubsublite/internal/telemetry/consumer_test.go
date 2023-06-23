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

package telemetry

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apmqueue "github.com/elastic/apm-queue"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.opentelemetry.io/otel/trace"
)

func TestConsumer(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer tp.Shutdown(context.Background())

	for _, tt := range []struct {
		name       string
		msg        *pubsub.Message
		attributes []attribute.KeyValue

		traceID [16]byte

		expectedSpans tracetest.SpanStubs
		expectMetrics []metricdata.Metrics
	}{
		{
			name:          "with no message",
			expectedSpans: tracetest.SpanStubs{},
		},
		{
			name: "with no attributes",
			msg:  &pubsub.Message{},
			expectedSpans: tracetest.SpanStubs{
				tracetest.SpanStub{
					Name:     "topic process",
					SpanKind: trace.SpanKindConsumer,
					Attributes: []attribute.KeyValue{
						semconv.MessagingSystemKey.String("pubsublite"),
						semconv.MessagingSourceKindTopic,
						semconv.MessagingOperationProcess,
						semconv.MessagingMessageIDKey.String(""),
						semconv.MessageUncompressedSize(0),
					},
					InstrumentationLibrary: instrumentation.Library{
						Name: "test",
					},
				},
			},
		},
		{
			name: "with attributes that don't match a parent span",
			msg: &pubsub.Message{Attributes: map[string]string{
				"hello": "world",
			}},
			attributes: []attribute.KeyValue{
				attribute.String("project", "project_name"),
			},
			expectedSpans: tracetest.SpanStubs{
				tracetest.SpanStub{
					Name:     "topic process",
					SpanKind: trace.SpanKindConsumer,
					Attributes: []attribute.KeyValue{
						attribute.String("project", "project_name"),
						semconv.MessagingSystemKey.String("pubsublite"),
						semconv.MessagingSourceKindTopic,
						semconv.MessagingOperationProcess,
						attribute.String("hello", "world"),
						semconv.MessagingMessageIDKey.String(""),
						semconv.MessageUncompressedSize(0),
					},
					InstrumentationLibrary: instrumentation.Library{
						Name: "test",
					},
				},
			},
			expectMetrics: []metricdata.Metrics{
				{
					Name:        "consumer.messages.fetched",
					Description: "The number of messages fetched",
					Unit:        "1",
					Data: metricdata.Sum[int64]{
						IsMonotonic: true,
						Temporality: metricdata.CumulativeTemporality,
						DataPoints: []metricdata.DataPoint[int64]{{
							Value: 1,
							Attributes: attribute.NewSet(
								attribute.String("project", "project_name"),
								semconv.MessagingSystemKey.String("pubsublite"),
								semconv.MessagingSourceKindTopic,
								semconv.MessagingOperationProcess,
								attribute.String("hello", "world"),
							),
						}},
					},
				},
			},
		},
		{
			name: "with attributes that matches a parent span",
			msg: &pubsub.Message{Attributes: map[string]string{
				"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			}},
			traceID: mustTraceIDFromHex(t, "4bf92f3577b34da6a3ce929d0e0e4736"),
			expectedSpans: tracetest.SpanStubs{
				tracetest.SpanStub{
					Name:     "topic process",
					SpanKind: trace.SpanKindConsumer,
					SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
						TraceID: mustTraceIDFromHex(t, "4bf92f3577b34da6a3ce929d0e0e4736"),
					}),
					Parent: trace.NewSpanContext(trace.SpanContextConfig{
						TraceID: mustTraceIDFromHex(t, "4bf92f3577b34da6a3ce929d0e0e4736"),
						Remote:  true,
					}),
					Attributes: []attribute.KeyValue{
						semconv.MessagingSystemKey.String("pubsublite"),
						semconv.MessagingSourceKindTopic,
						semconv.MessagingOperationProcess,
						semconv.MessagingMessageIDKey.String(""),
						semconv.MessageUncompressedSize(0),
					},
					InstrumentationLibrary: instrumentation.Library{
						Name: "test",
					},
				},
			},
			expectMetrics: []metricdata.Metrics{
				{
					Name:        "consumer.messages.fetched",
					Description: "The number of messages fetched",
					Unit:        "1",
					Data: metricdata.Sum[int64]{
						IsMonotonic: true,
						Temporality: metricdata.CumulativeTemporality,
						DataPoints: []metricdata.DataPoint[int64]{{
							Value: 1,
							Attributes: attribute.NewSet(
								semconv.MessagingSystemKey.String("pubsublite"),
								semconv.MessagingSourceKindTopic,
								semconv.MessagingOperationProcess,
							),
						}},
					},
				},
			},
		},
		{
			name: "with an event time attribute",
			msg: &pubsub.Message{Attributes: map[string]string{
				apmqueue.EventTimeKey: time.Now().Format(time.RFC3339),
			}},
			attributes: []attribute.KeyValue{
				attribute.String("project", "project_name"),
			},
			expectedSpans: tracetest.SpanStubs{
				tracetest.SpanStub{
					Name:     "topic process",
					SpanKind: trace.SpanKindConsumer,
					Attributes: []attribute.KeyValue{
						attribute.String("project", "project_name"),
						semconv.MessagingSystemKey.String("pubsublite"),
						semconv.MessagingSourceKindTopic,
						semconv.MessagingOperationProcess,
						semconv.MessagingMessageIDKey.String(""),
						semconv.MessageUncompressedSize(0),
						attribute.Float64("timestamp", 1.0),
					},
					InstrumentationLibrary: instrumentation.Library{
						Name: "test",
					},
				},
			},
			expectMetrics: []metricdata.Metrics{
				{
					Name:        "consumer.messages.fetched",
					Description: "The number of messages fetched",
					Unit:        "1",
					Data: metricdata.Sum[int64]{
						IsMonotonic: true,
						Temporality: metricdata.CumulativeTemporality,
						DataPoints: []metricdata.DataPoint[int64]{{
							Value: 1,
							Attributes: attribute.NewSet(
								attribute.String("project", "project_name"),
								semconv.MessagingSystemKey.String("pubsublite"),
								semconv.MessagingSourceKindTopic,
								semconv.MessagingOperationProcess,
							),
						}},
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
								attribute.String("project", "project_name"),
								semconv.MessagingSystemKey.String("pubsublite"),
								semconv.MessagingSourceKindTopic,
								semconv.MessagingOperationProcess,
							),

							Bounds: []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000},
							Count:  uint64(1),
						}},
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer exp.Reset() // Reset for next tests

			rdr := sdkmetric.NewManualReader()
			cm, err := NewConsumerMetrics(sdkmetric.NewMeterProvider(
				sdkmetric.WithReader(rdr),
			))
			require.NoError(t, err)
			recive := Consumer(func(ctx context.Context, msg *pubsub.Message) {
				// No need to do anything here
			}, tp.Tracer("test"), cm, "topic", tt.attributes)

			recive(context.Background(), tt.msg)
			assertSpans(t, tt.traceID, tt.expectedSpans, exp.GetSpans())

			if len(tt.expectMetrics) == 0 {
				return
			}

			var rm metricdata.ResourceMetrics
			assert.NoError(t, rdr.Collect(context.Background(), &rm))

			require.Len(t, rm.ScopeMetrics, 1)
			assert.Len(t, rm.ScopeMetrics[0].Metrics, len(tt.expectMetrics))

			for k, m := range tt.expectMetrics {
				metric := rm.ScopeMetrics[0].Metrics[k]

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
		})
	}
}

func TestConsumerMultipleEvents(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer tp.Shutdown(context.Background())

	rdr := sdkmetric.NewManualReader()
	cm, err := NewConsumerMetrics(sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(rdr),
	))
	require.NoError(t, err)

	sharedAttrs := []attribute.KeyValue{semconv.MessagingSourceName("topic")}
	var events int
	consume := Consumer(func(context.Context, *pubsub.Message) {
		events++
	}, tp.Tracer("test"), cm, "topic", sharedAttrs)

	msgs := []*pubsub.Message{
		{ID: "1", Attributes: map[string]string{"a": "b"}, Data: []byte("1")},
		{ID: "2", Attributes: map[string]string{"c": "d"}, Data: []byte("2")},
		{ID: "3", Attributes: map[string]string{"e": "f"}, Data: []byte("3")},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for _, msg := range msgs {
		consume(ctx, msg)
	}

	assert.Equal(t, len(msgs), events) // Assert messages processed
	assert.Equal(t,
		[]attribute.KeyValue{semconv.MessagingSourceName("topic")}, sharedAttrs,
	) // Assert the shared attribute slice hasn't been modified.

	// Assert the attribute slice isn't mutated between events.
	assertSpans(t, [16]byte{}, tracetest.SpanStubs{
		{
			Name:     "topic process",
			SpanKind: trace.SpanKindConsumer,
			Attributes: []attribute.KeyValue{
				semconv.MessagingSourceName("topic"),
				semconv.MessagingSystemKey.String("pubsublite"),
				semconv.MessagingSourceKindTopic,
				semconv.MessagingOperationProcess,
				attribute.String("a", "b"),
				semconv.MessagingMessageIDKey.String("1"),
				semconv.MessageUncompressedSize(1),
			},
			InstrumentationLibrary: instrumentation.Library{Name: "test"},
		},
		{
			Name:     "topic process",
			SpanKind: trace.SpanKindConsumer,
			Attributes: []attribute.KeyValue{
				semconv.MessagingSourceName("topic"),
				semconv.MessagingSystemKey.String("pubsublite"),
				semconv.MessagingSourceKindTopic,
				semconv.MessagingOperationProcess,
				attribute.String("c", "d"),
				semconv.MessagingMessageIDKey.String("2"),
				semconv.MessageUncompressedSize(1),
			},
			InstrumentationLibrary: instrumentation.Library{Name: "test"},
		},
		{
			Name:     "topic process",
			SpanKind: trace.SpanKindConsumer,
			Attributes: []attribute.KeyValue{
				semconv.MessagingSourceName("topic"),
				semconv.MessagingSystemKey.String("pubsublite"),
				semconv.MessagingSourceKindTopic,
				semconv.MessagingOperationProcess,
				attribute.String("e", "f"),
				semconv.MessagingMessageIDKey.String("3"),
				semconv.MessageUncompressedSize(1),
			},
			InstrumentationLibrary: instrumentation.Library{Name: "test"},
		},
	}, exp.GetSpans())
}

func assertSpans(t testing.TB, traceID [16]byte, expected, actual tracetest.SpanStubs) {
	t.Helper()

	for i := range actual {
		// Nullify data we don't use/can't set manually
		actual[i].SpanContext = actual[i].SpanContext.
			WithTraceID(traceID).
			WithSpanID([8]byte{}).
			WithTraceFlags(0)
		actual[i].Parent = actual[i].Parent.
			WithTraceID(traceID).
			WithSpanID([8]byte{}).
			WithTraceFlags(0)
		actual[i].Resource = nil
		actual[i].StartTime = time.Time{}
		actual[i].EndTime = time.Time{}

		for j, v := range actual[i].Attributes {
			if v.Key == apmqueue.EventTimeKey {
				actual[i].Attributes[j].Value = attribute.Float64Value(1.0)
			}
		}
	}
	assert.Equal(t, expected, actual)
}

func mustTraceIDFromHex(t testing.TB, s string) (tr trace.TraceID) {
	t.Helper()

	tr, err := trace.TraceIDFromHex(s)
	assert.NoError(t, err)
	return tr
}
