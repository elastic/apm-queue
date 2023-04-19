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
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/instrumentation"
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
	otel.SetTextMapPropagator(propagation.TraceContext{})

	for _, tt := range []struct {
		name string
		msg  *pubsub.Message

		traceID      [16]byte
		spanID       [8]byte
		parentSpanID [8]byte

		resServerID string
		resErr      error

		expectedSpans tracetest.SpanStubs
	}{
		{
			name: "with no message",

			expectedSpans: tracetest.SpanStubs{
				tracetest.SpanStub{
					Name:     "pubsublite.Publish",
					SpanKind: trace.SpanKindProducer,
					Attributes: []attribute.KeyValue{
						semconv.MessagingSystemKey.String("pubsub"),
						semconv.MessagingDestinationKindTopic,
						semconv.MessagingMessageIDKey.String(""),
					},
					InstrumentationLibrary: instrumentation.Library{
						Name: "test",
					},
					Events: []sdktrace.Event{
						sdktrace.Event{
							Name: "exception",
							Attributes: []attribute.KeyValue{
								attribute.String("exception.type", "*errors.errorString"),
								attribute.String("exception.message", "context canceled"),
							},
						},
					},
					Status: sdktrace.Status{
						Code:        codes.Error,
						Description: "context canceled",
					},
				},
			},
		},
		{
			name: "with no attributes",
			msg:  &pubsub.Message{},

			expectedSpans: tracetest.SpanStubs{
				tracetest.SpanStub{
					Name:     "pubsublite.Publish",
					SpanKind: trace.SpanKindProducer,
					Attributes: []attribute.KeyValue{
						semconv.MessagingSystemKey.String("pubsub"),
						semconv.MessagingDestinationKindTopic,
						semconv.MessagingMessageIDKey.String(""),
					},
					InstrumentationLibrary: instrumentation.Library{
						Name: "test",
					},
					Events: []sdktrace.Event{
						sdktrace.Event{
							Name: "exception",
							Attributes: []attribute.KeyValue{
								attribute.String("exception.type", "*errors.errorString"),
								attribute.String("exception.message", "context canceled"),
							},
						},
					},
					Status: sdktrace.Status{
						Code:        codes.Error,
						Description: "context canceled",
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res := &pubsub.PublishResult{}
			ctx, cancel := context.WithCancel(context.Background())
			_ = Publisher(ctx, tp.Tracer("test"), tt.msg, func(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult {
				return res
			})

			// Wait for the async goroutine to finish runnning
			cancel()
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

			// Reset for next tests
			exp.Reset()
		})
	}
}
