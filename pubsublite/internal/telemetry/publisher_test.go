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
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/apm-queue/pubsublite/internal/pubsubabs"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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

func TestPublisher(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	defer tp.Shutdown(context.Background())
	otel.SetTextMapPropagator(propagation.TraceContext{})

	for _, tt := range []struct {
		name       string
		msg        *pubsub.Message
		attributes []attribute.KeyValue

		traceID      [16]byte
		spanID       [8]byte
		parentSpanID [8]byte

		resServerID string
		resErr      error

		success bool

		expectedSpans tracetest.SpanStubs
		expectMetrics metricdata.Metrics
	}{
		{
			name: "failure with no attributes",
			msg:  &pubsub.Message{},

			expectedSpans: tracetest.SpanStubs{
				tracetest.SpanStub{
					Name:     "pubsublite.Publish",
					SpanKind: trace.SpanKindProducer,
					Attributes: []attribute.KeyValue{
						semconv.MessagingSystemKey.String("pubsublite"),
						semconv.MessagingDestinationKindTopic,
						semconv.MessagingMessageIDKey.String("msg-id"),
					},
					InstrumentationLibrary: instrumentation.Library{
						Name: "test",
					},
					Events: []sdktrace.Event{{
						Name: "exception",
						Attributes: []attribute.KeyValue{
							attribute.String("exception.type", "*errors.errorString"),
							attribute.String("exception.message", "failed processing message"),
						},
					}},
					Status: sdktrace.Status{
						Code:        codes.Error,
						Description: "failed processing message",
					},
				},
			},
			expectMetrics: metricdata.Metrics{
				Name:        "producer.messages.errored",
				Description: "The number of messages that failed to be produced",
				Unit:        "1",
				Data: metricdata.Sum[int64]{
					IsMonotonic: true,
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.DataPoint[int64]{{
						Value: 1,
						Attributes: attribute.NewSet(
							semconv.MessagingSystemKey.String("pubsublite"),
							semconv.MessagingDestinationKindTopic,
						),
					}},
				},
			},
		},
		{
			name: "success with otel attributes",
			msg:  &pubsub.Message{},

			attributes: []attribute.KeyValue{
				attribute.String("project", "project_name"),
			},

			expectedSpans: tracetest.SpanStubs{
				tracetest.SpanStub{
					Name:     "pubsublite.Publish",
					SpanKind: trace.SpanKindProducer,
					Attributes: []attribute.KeyValue{
						attribute.String("project", "project_name"),
						semconv.MessagingSystemKey.String("pubsublite"),
						semconv.MessagingDestinationKindTopic,
						semconv.MessagingMessageIDKey.String("msg-id"),
					},
					InstrumentationLibrary: instrumentation.Library{
						Name: "test",
					},
					Events: []sdktrace.Event{{
						Name: "exception",
						Attributes: []attribute.KeyValue{
							attribute.String("exception.type", "*errors.errorString"),
							attribute.String("exception.message", "failed processing message"),
						},
					}},
					Status: sdktrace.Status{
						Code:        codes.Error,
						Description: "failed processing message",
					},
				},
			},
			expectMetrics: metricdata.Metrics{
				Name:        "producer.messages.errored",
				Description: "The number of messages that failed to be produced",
				Unit:        "1",
				Data: metricdata.Sum[int64]{
					IsMonotonic: true,
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.DataPoint[int64]{{
						Value: 1,
						Attributes: attribute.NewSet(
							semconv.MessagingSystemKey.String("pubsublite"),
							semconv.MessagingDestinationKindTopic,
							attribute.String("project", "project_name"),
						),
					}},
				},
			},
		},
		{
			name: "success with otel attributes",
			msg: &pubsub.Message{
				Attributes: map[string]string{"key": "value"},
			},

			attributes: []attribute.KeyValue{
				attribute.String("project", "project_name"),
			},
			success: true, // succeed

			expectedSpans: tracetest.SpanStubs{
				tracetest.SpanStub{
					Name:     "pubsublite.Publish",
					SpanKind: trace.SpanKindProducer,
					Attributes: []attribute.KeyValue{
						attribute.String("project", "project_name"),
						semconv.MessagingSystemKey.String("pubsublite"),
						semconv.MessagingDestinationKindTopic,
						attribute.String("key", "value"),
						semconv.MessagingMessageIDKey.String("msg-id"),
					},
					InstrumentationLibrary: instrumentation.Library{
						Name: "test",
					},
					Status: sdktrace.Status{
						Code: codes.Ok,
					},
				},
			},
			expectMetrics: metricdata.Metrics{
				Name:        "producer.messages.produced",
				Description: "The number of messages produced",
				Unit:        "1",
				Data: metricdata.Sum[int64]{
					IsMonotonic: true,
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.DataPoint[int64]{{
						Value: 1,
						Attributes: attribute.NewSet(
							semconv.MessagingSystemKey.String("pubsublite"),
							semconv.MessagingDestinationKindTopic,
							attribute.String("project", "project_name"),
							attribute.String("key", "value"),
						),
					}},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rdr := sdkmetric.NewManualReader()
			pm, err := NewProducerMetrics(sdkmetric.NewMeterProvider(
				sdkmetric.WithReader(rdr),
			))
			require.NoError(t, err)

			res := pubsubabs.NewPublishResult()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := newPublisherFunc(func(ctx context.Context, msg *pubsub.Message) pubsubabs.PublishResult {
				return res
			})
			producer := NewProducer(client, tp.Tracer("test"), pm, tt.attributes)
			_ = producer.Publish(ctx, tt.msg)

			if tt.success {
				pubsubabs.SetPublishResult(res, "msg-id", nil)
			} else {
				pubsubabs.SetPublishResult(res, "msg-id", errors.New("failed processing message"))
			}

			// Wait for the async goroutine to finish runnning
			time.Sleep(time.Millisecond)

			spans := exp.GetSpans()
			for i := range spans {
				// Nullify data we don't use/can't set manually
				spans[i].SpanContext = spans[i].SpanContext.
					WithTraceID(tt.traceID).
					WithSpanID(tt.spanID).
					WithTraceFlags(0)
				spans[i].Parent = spans[i].Parent.
					WithTraceID(tt.traceID).
					WithSpanID(tt.parentSpanID).
					WithTraceFlags(0)
				spans[i].Resource = nil
				spans[i].StartTime = time.Time{}
				spans[i].EndTime = time.Time{}

				for k := range spans[i].Events {
					spans[i].Events[k].Time = time.Time{}
				}
			}

			assert.Equal(t, tt.expectedSpans, spans)

			// NOTE(marclop) for now, it is only possible to assert failures.
			var rm metricdata.ResourceMetrics
			assert.NoError(t, rdr.Collect(context.Background(), &rm))

			assert.Len(t, rm.ScopeMetrics[0].Metrics, 1)
			metricdatatest.AssertEqual(t,
				tt.expectMetrics,
				rm.ScopeMetrics[0].Metrics[0],
				metricdatatest.IgnoreTimestamp(),
			)
			// Reset for next tests
			exp.Reset()
		})
	}
}

type publishFunc func(ctx context.Context, msg *pubsub.Message) pubsubabs.PublishResult

func newPublisherFunc(f publishFunc) pubsubabs.Publisher { return pfunc(f) }

type pfunc publishFunc

// Error returns the producer stop error.
func (p pfunc) Error() error { return nil }

// Stop stops the producer.
func (p pfunc) Stop() {}

// Publish wraps the pubsublite publish.
func (p pfunc) Publish(ctx context.Context, msg *pubsub.Message) pubsubabs.PublishResult {
	return p(ctx, msg)
}
