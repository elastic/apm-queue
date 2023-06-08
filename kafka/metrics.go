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
)

const (
	instrumentName = "github.com/elastic/apm-queue/kafka"

	unitCount = "1"

	messageProducedCounterKey = "message.produced"
	writeErrorCounterKey      = "write.error"
)

type instruments struct {
	messageProduced metric.Int64Counter
	writeErrors     metric.Int64Counter
}

type kgoHooks struct {
	instruments instruments
}

func NewKgoHooks(mp metric.MeterProvider) (*kgoHooks, error) {
	m := mp.Meter(instrumentName)

	messageProducedCounter, err := m.Int64Counter(
		messageProducedCounterKey,
		metric.WithDescription("The total number of message produced"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create %s metric: %w", messageProducedCounterKey, err)
	}

	writeErrorCounter, err := m.Int64Counter(
		writeErrorCounterKey,
		metric.WithDescription("The total number of error occurred on write"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create %s metric: %w", writeErrorCounterKey, err)
	}

	return &kgoHooks{
		instruments{
			messageProduced: messageProducedCounter,
			writeErrors:     writeErrorCounter,
		},
	}, nil
}

// https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#HookProduceRecordUnbuffered
func (h *kgoHooks) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	partitionDimension := attribute.Int("partition", int(r.Partition))
	topicDimension := attribute.String("topic", r.Topic)

	if err != nil {
		errorDimension := attribute.String("error", "other")

		if errors.Is(err, context.DeadlineExceeded) {
			errorDimension = attribute.String("error", "timeout")
		}
		if errors.Is(err, context.Canceled) {
			errorDimension = attribute.String("error", "cancelled")
		}

		h.instruments.writeErrors.Add(
			context.Background(),
			1,
			metric.WithAttributes(partitionDimension, topicDimension, errorDimension),
		)
		return
	}

	h.instruments.messageProduced.Add(
		context.Background(),
		1,
		metric.WithAttributes(partitionDimension, topicDimension))

}
