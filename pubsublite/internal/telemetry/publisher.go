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
	"fmt"

	"cloud.google.com/go/pubsub"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/elastic/apm-queue/pubsublite/internal/pubsubabs"
)

const (
	producedKey = "producer.messages.produced"
	erroredKey  = "producer.messages.errored"

	instrumentName = "github.com/elastic/apm-queue/pubsublite"
)

// ProducerMetrics hold the metrics that are recorded for producers
type ProducerMetrics struct {
	produced metric.Int64Counter
	errored  metric.Int64Counter
}

// NewProducerMetrics instantiates the producer metrics.
func NewProducerMetrics(mp metric.MeterProvider) (ProducerMetrics, error) {
	m := mp.Meter(instrumentName)
	produced, err := m.Int64Counter(producedKey,
		metric.WithDescription("The number of messages produced"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return ProducerMetrics{}, fmt.Errorf(
			"telemetry: cannot create %s metric: %w", producedKey, err,
		)
	}
	errored, err := m.Int64Counter(erroredKey,
		metric.WithDescription("The number of messages that failed to be produced"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return ProducerMetrics{}, fmt.Errorf(
			"telemetry: cannot create %s metric: %w", erroredKey, err,
		)
	}
	return ProducerMetrics{
		produced: produced,
		errored:  errored,
	}, nil
}

var _ pubsubabs.Publisher = &Producer{}

// Producer wraps a publisher client to provider tracing and metering.
type Producer struct {
	client  pubsubabs.Publisher
	tracer  trace.Tracer
	metrics ProducerMetrics
	attrs   []attribute.KeyValue
}

// Error returns the producer stop error.
func (p *Producer) Error() error { return p.client.Error() }

// Stop stops the producer.
func (p *Producer) Stop() { p.client.Stop() }

// Publish wraps the pubsublite publish action with traces and metrics.
func (p *Producer) Publish(ctx context.Context, msg *pubsub.Message) pubsubabs.PublishResult {
	attrs := p.attrs
	if len(msg.Attributes) > 0 {
		attrs = make([]attribute.KeyValue, 0, len(p.attrs)+len(msg.Attributes))
		for key, v := range msg.Attributes {
			attrs = append(attrs, attribute.String(key, v))
		}
	}
	ctx, span := p.tracer.Start(ctx, "pubsublite.Publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	)
	if msg.Attributes == nil {
		msg.Attributes = make(map[string]string)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(msg.Attributes))
	res := p.client.Publish(ctx, msg)
	// This creates one goroutine for each message, which may cause overhead for
	// mid-high throughput producers.
	// See also https://github.com/googleapis/google-cloud-go/issues/2953
	go func() {
		mid, err := res.Get(ctx)
		span.SetAttributes(semconv.MessagingMessageIDKey.String(mid))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			p.metrics.errored.Add(ctx, 1, metric.WithAttributes(
				attrs...,
			))
		} else {
			span.SetStatus(codes.Ok, "success")
			p.metrics.produced.Add(ctx, 1, metric.WithAttributes(
				attrs...,
			))
		}
		span.End()
	}()
	return res
}

// NewProducer decorates an existing publisher with tracing and metering.
func NewProducer(
	client pubsubabs.Publisher,
	tracer trace.Tracer,
	metrics ProducerMetrics,
	attrs []attribute.KeyValue,
) *Producer {
	return &Producer{
		client:  client,
		tracer:  tracer,
		metrics: metrics,
		attrs: append(attrs,
			semconv.MessagingSystemKey.String("pubsublite"),
			semconv.MessagingDestinationKindTopic,
		),
	}
}
