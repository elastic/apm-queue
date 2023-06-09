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

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

const (
	instrumentName = "github.com/elastic/apm-queue/kafka"

	unitCount = "1"

	messageProducedCounterKey = "producer.messages.produced"
	messageErroredCounterKey  = "producer.messages.errored"
)

type metricHooks struct {
	messageProduced metric.Int64Counter
	messageErrored  metric.Int64Counter
}

func newKgoHooks(mp metric.MeterProvider) (*metricHooks, error) {
	m := mp.Meter(instrumentName)

	messageProducedCounter, err := m.Int64Counter(
		messageProducedCounterKey,
		metric.WithDescription("The number of messages produced"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create %s metric: %w", messageProducedCounterKey, err)
	}

	messageErroredCounter, err := m.Int64Counter(
		messageErroredCounterKey,
		metric.WithDescription("The number of messages that failed to be produced"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create %s metric: %w", messageErroredCounterKey, err)
	}

	return &metricHooks{
		messageProduced: messageProducedCounter,
		messageErrored:  messageErroredCounter,
	}, nil
}

// https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#HookProduceRecordUnbuffered
func (h *metricHooks) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	attrs := make([]attribute.KeyValue, 0, 5) // Preallocate 5 elements.
	attrs = append(attrs,
		semconv.MessagingDestinationName(r.Topic),
		semconv.MessagingKafkaDestinationPartition(int(r.Partition)),
	)
	for _, v := range r.Headers {
		attrs = append(attrs, attribute.String(v.Key, string(v.Value)))
	}

	if err != nil {
		errorType := attribute.String("error", "other")
		if errors.Is(err, context.DeadlineExceeded) {
			errorType = attribute.String("error", "timeout")
		}
		if errors.Is(err, context.Canceled) {
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
