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
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"

	apmqueue "github.com/elastic/apm-queue"
)

const (
	instrumentName = "github.com/elastic/apm-queue/kafka"

	unitCount = "1"

	msgProducedKey = "producer.messages.produced"
	msgErroredKey  = "producer.messages.errored"
	msgFetchedKey  = "consumer.messages.fetched"
	msgDelayKey    = "consumer.messages.delay"
)

type metricHooks struct {
	messageProduced metric.Int64Counter
	messageErrored  metric.Int64Counter
	messageFetched  metric.Int64Counter
	messageDelay    metric.Float64Histogram
}

func newKgoHooks(mp metric.MeterProvider) (*metricHooks, error) {
	m := mp.Meter(instrumentName)
	messageProducedCounter, err := m.Int64Counter(msgProducedKey,
		metric.WithDescription("The number of messages produced"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		return nil, formatMetricError(msgProducedKey, err)
	}
	messageErroredCounter, err := m.Int64Counter(msgErroredKey,
		metric.WithDescription("The number of messages that failed to be produced"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		return nil, formatMetricError(msgErroredKey, err)
	}
	messageFetchedCounter, err := m.Int64Counter(msgFetchedKey,
		metric.WithDescription("The number of messages that were fetched from a kafka topic"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		return nil, formatMetricError(msgFetchedKey, err)
	}

	messageDelayHistogram, err := m.Float64Histogram(msgDelayKey,
		metric.WithDescription("The delay between producing messages and reading them"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, formatMetricError(msgDelayKey, err)
	}

	return &metricHooks{
		// Producer
		messageProduced: messageProducedCounter,
		messageErrored:  messageErroredCounter,
		// Consumer
		messageFetched: messageFetchedCounter,
		messageDelay:   messageDelayHistogram,
	}, nil
}

func formatMetricError(name string, err error) error {
	return fmt.Errorf("cannot create %s metric: %w", name, err)
}

// OnProduceRecordUnbuffered records the number of produced / failed messages.
// https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#HookProduceRecordUnbuffered
func (h *metricHooks) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	attrs := attributesFromRecord(r)
	if err != nil {
		errorType := attribute.String("error", "other")
		if errors.Is(err, context.DeadlineExceeded) {
			errorType = attribute.String("error", "timeout")
		} else if errors.Is(err, context.Canceled) {
			errorType = attribute.String("error", "canceled")
		}

		h.messageErrored.Add(context.Background(), 1,
			metric.WithAttributes(append(attrs, errorType)...),
		)
		return
	}
	h.messageProduced.Add(context.Background(), 1,
		metric.WithAttributes(attrs...),
	)
}

// OnFetchRecordUnbuffered records the number of fetched messages.
// https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#HookFetchRecordUnbuffered
func (h *metricHooks) OnFetchRecordUnbuffered(r *kgo.Record, _ bool) {
	attrs := attributesFromRecord(r)

	h.messageFetched.Add(r.Context, 1,
		metric.WithAttributes(attrs...),
	)

	span := trace.SpanFromContext(r.Context)
	for _, v := range r.Headers {
		if v.Key == apmqueue.EventTimeKey {
			since, err := time.Parse(time.RFC3339, string(v.Value))
			if err != nil {
				span.RecordError(err)
				return
			}

			delay := time.Since(since).Seconds()
			span.SetAttributes(
				attribute.Float64(apmqueue.EventTimeKey, delay),
			)
			h.messageDelay.Record(r.Context, delay, metric.WithAttributes(
				attrs...,
			))
		}
	}
}

func attributesFromRecord(r *kgo.Record) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 5) // Preallocate 5 elements.
	attrs = append(attrs,
		semconv.MessagingDestinationName(r.Topic),
		semconv.MessagingKafkaDestinationPartition(int(r.Partition)),
	)
	for _, v := range r.Headers {
		if v.Key == "traceparent" || v.Key == apmqueue.EventTimeKey { // Ignore traceparent and event time headers.
			continue
		}
		attrs = append(attrs, attribute.String(v.Key, string(v.Value)))
	}
	return attrs
}
