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

	"cloud.google.com/go/pubsub"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

type producerHandler = func(context.Context, *pubsub.Message) *pubsub.PublishResult

// Publisher adds telemetry data to messages published
func Publisher(ctx context.Context, tracer trace.Tracer, msg *pubsub.Message, h producerHandler) *pubsub.PublishResult {
	ctx, span := tracer.Start(ctx, "pubsublite.Publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("pubsub"),
			semconv.MessagingDestinationKindTopic,
		),
	)

	if msg == nil {
		msg = &pubsub.Message{}
	}

	if msg.Attributes == nil {
		msg.Attributes = make(map[string]string)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(msg.Attributes))

	res := h(ctx, msg)

	go func() {
		mid, err := res.Get(ctx)
		span.SetAttributes(semconv.MessagingMessageIDKey.String(mid))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}

		span.End()
	}()

	return res
}
