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

// Package telemetry allows setting up telemetry for pubsublite consumers and
// producers
package telemetry

import (
	"context"

	"cloud.google.com/go/pubsub"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

type consumerHandler = func(context.Context, *pubsub.Message)

// Consumer adds telemetry data to messages received
func Consumer(tracer trace.Tracer, h consumerHandler) consumerHandler {
	return func(ctx context.Context, msg *pubsub.Message) {
		if msg == nil {
			return
		}

		if msg.Attributes != nil {
			propagator := otel.GetTextMapPropagator()
			ctx = propagator.Extract(ctx, propagation.MapCarrier(msg.Attributes))
		}

		ctx, span := tracer.Start(ctx, "pubsublite.Receive",
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				semconv.FaaSTriggerPubsub,
				semconv.MessagingSystemKey.String("pubsublite"),
				semconv.MessagingDestinationKindTopic,
				semconv.MessagingOperationProcess,
				semconv.MessagingMessageIDKey.String(msg.ID),
			),
		)
		defer span.End()

		h(ctx, msg)
	}
}
