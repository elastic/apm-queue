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
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.opentelemetry.io/otel/trace"

	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/pubsublite/internal/pubsubabs"
)

const (
	msgFetchedKey = "consumer.messages.fetched"
	msgDelayKey   = "consumer.messages.delay"
)

// ConsumerMetrics holds the metrics that are recorded for consumers
type ConsumerMetrics struct {
	fetched     metric.Int64Counter
	queuedDelay metric.Int64Histogram
}

// NewConsumerMetrics instantiates the producer metrics.
func NewConsumerMetrics(mp metric.MeterProvider) (cm ConsumerMetrics, err error) {
	m := mp.Meter(instrumentName)
	cm.fetched, err = m.Int64Counter(msgFetchedKey,
		metric.WithDescription("The number of messages fetched"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return cm, formatMetricError(msgFetchedKey, err)
	}

	cm.queuedDelay, err = m.Int64Histogram(msgDelayKey,
		metric.WithDescription("The delay between producing messages and reading them"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return cm, formatMetricError(msgDelayKey, err)
	}
	return
}

func formatMetricError(name string, err error) error {
	return fmt.Errorf("telemetry: cannot create %s metric: %w", name, err)
}

// Consumer decorates an existing consumer with tracing and metering.
func Consumer(
	receive pubsubabs.ReceiveFunc,
	tracer trace.Tracer,
	metrics ConsumerMetrics,
	topic string,
	commonAttrs []attribute.KeyValue,
) pubsubabs.ReceiveFunc {
	commonAttrs = append(commonAttrs,
		semconv.MessagingSystem("pubsublite"),
		semconv.MessagingSourceKindTopic,
		semconv.MessagingOperationProcess,
	)
	// https://opentelemetry.io/docs/specs/otel/trace/semantic_conventions/messaging/#span-name
	operation := topic + " process"
	return func(ctx context.Context, msg *pubsub.Message) {
		if msg == nil {
			return
		}

		// Ensure cap == len to avoid mutating commonAttrs.
		attrs := commonAttrs[0:len(commonAttrs):len(commonAttrs)]
		if len(msg.Attributes) > 0 {
			_, hasTraceparent := msg.Attributes["traceparent"]
			if (hasTraceparent && len(msg.Attributes) > 1) || !hasTraceparent {
				attrs = append(make([]attribute.KeyValue, 0,
					len(commonAttrs)+len(msg.Attributes), // Create a new slice
				), commonAttrs...)
			}
			for key, v := range msg.Attributes {
				if key == "traceparent" || key == apmqueue.EventTimeKey { // Ignore traceparent and event time headers.
					continue
				}
				attrs = append(attrs, attribute.String(key, v))
			}
		}

		metrics.fetched.Add(context.Background(), 1, metric.WithAttributes(
			attrs...,
		))

		if msg.Attributes != nil {
			if msg.Attributes[apmqueue.EventTimeKey] != "" {
				if since, err := time.Parse(time.RFC3339, msg.Attributes[apmqueue.EventTimeKey]); err == nil {
					delay := time.Since(since).Milliseconds()
					metrics.queuedDelay.Record(context.Background(), delay, metric.WithAttributes(
						attrs...,
					))
				}
			}

			propagator := otel.GetTextMapPropagator()
			ctx = propagator.Extract(ctx, propagation.MapCarrier(msg.Attributes))
		}
		ctx, span := tracer.Start(ctx, operation, // PubSub name.
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(append(attrs,
				semconv.MessagingMessageIDKey.String(msg.ID),
				semconv.MessageUncompressedSize(len(msg.Data)),
			)...),
		)
		defer span.End()
		receive(ctx, msg)
	}
}
