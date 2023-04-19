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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/instrumentation"
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

		traceID      [16]byte
		spanID       [8]byte
		parentSpanID [8]byte

		expectedSpans tracetest.SpanStubs
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
					Name:     "pubsublite.Receive",
					SpanKind: trace.SpanKindConsumer,
					Attributes: []attribute.KeyValue{
						semconv.MessagingSystemKey.String("pubsublite"),
						semconv.MessagingSourceKindTopic,
						semconv.MessagingOperationProcess,
						semconv.MessagingMessageIDKey.String(""),
					},
					InstrumentationLibrary: instrumentation.Library{
						Name: "test",
					},
				},
			},
		},
		{
			name: "with attributes that don't match a parent span",
			msg: &pubsub.Message{
				Attributes: map[string]string{
					"hello": "world",
				},
			},
			attributes: []attribute.KeyValue{
				attribute.String("project", "project_name"),
			},
			expectedSpans: tracetest.SpanStubs{
				tracetest.SpanStub{
					Name:     "pubsublite.Receive",
					SpanKind: trace.SpanKindConsumer,
					Attributes: []attribute.KeyValue{
						attribute.String("project", "project_name"),
						semconv.MessagingSystemKey.String("pubsublite"),
						semconv.MessagingSourceKindTopic,
						semconv.MessagingOperationProcess,
						semconv.MessagingMessageIDKey.String(""),
					},
					InstrumentationLibrary: instrumentation.Library{
						Name: "test",
					},
				},
			},
		},
		{
			name: "with attributes that matches a parent span",
			msg: &pubsub.Message{
				Attributes: map[string]string{
					"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
				},
			},
			traceID: mustTraceIDFromHex(t, "4bf92f3577b34da6a3ce929d0e0e4736"),
			expectedSpans: tracetest.SpanStubs{
				tracetest.SpanStub{
					Name:     "pubsublite.Receive",
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
					},
					InstrumentationLibrary: instrumentation.Library{
						Name: "test",
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := Consumer(tp.Tracer("test"), func(ctx context.Context, msg *pubsub.Message) {
				// No need to do anything here
			}, tt.attributes)

			h(context.Background(), tt.msg)

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
			}

			assert.Equal(t, tt.expectedSpans, spans)

			// Reset for next tests
			exp.Reset()
		})
	}
}

func mustTraceIDFromHex(t *testing.T, s string) (tr trace.TraceID) {
	t.Helper()

	tr, err := trace.TraceIDFromHex(s)
	assert.NoError(t, err)
	return tr
}
