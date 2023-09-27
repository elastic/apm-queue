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
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentName = "github.com/elastic/apm-queue/kafka"

	unitCount = "1"

	msgFetchedKey = "consumer.messages.fetched"
	msgDelayKey   = "consumer.messages.delay"
)

type metricHooks struct {
	namespace      string
	topicPrefix    string
	messageFetched metric.Int64Counter
	messageDelay   metric.Float64Histogram
}

func newKgoHooks(mp metric.MeterProvider, namespace, topicPrefix string) (*metricHooks, error) {
	m := mp.Meter(instrumentName)
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
		namespace:   namespace,
		topicPrefix: topicPrefix,
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
	attrs := attributesFromRecord(r,
		semconv.MessagingDestinationName(strings.TrimPrefix(r.Topic, h.topicPrefix)),
		semconv.MessagingKafkaDestinationPartition(int(r.Partition)),
	)
	if h.namespace != "" {
		attrs = append(attrs, attribute.String("namespace", h.namespace))
	}

	if err != nil {
		return
	}
}

// OnFetchRecordUnbuffered records the number of fetched messages.
// https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#HookFetchRecordUnbuffered
func (h *metricHooks) OnFetchRecordUnbuffered(r *kgo.Record, _ bool) {
	attrs := attributesFromRecord(r,
		semconv.MessagingSourceName(strings.TrimPrefix(r.Topic, h.topicPrefix)),
		semconv.MessagingKafkaSourcePartition(int(r.Partition)),
	)
	if h.namespace != "" {
		attrs = append(attrs, attribute.String("namespace", h.namespace))
	}

	h.messageFetched.Add(r.Context, 1,
		metric.WithAttributes(attrs...),
	)

	span := trace.SpanFromContext(r.Context)
	delay := time.Since(r.Timestamp).Seconds()
	span.SetAttributes(
		attribute.Float64(msgDelayKey, delay),
	)
	h.messageDelay.Record(r.Context, delay, metric.WithAttributes(
		attrs...,
	))
}

func attributesFromRecord(r *kgo.Record, extra ...attribute.KeyValue) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 5) // Preallocate 5 elements.
	attrs = append(attrs, semconv.MessagingSystem("kafka"))
	attrs = append(attrs, extra...)
	for _, v := range r.Headers {
		if v.Key == "traceparent" { // Ignore traceparent.
			continue
		}
		attrs = append(attrs, attribute.String(v.Key, string(v.Value)))
	}
	return attrs
}
