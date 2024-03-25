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
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

const (
	instrumentName = "github.com/elastic/apm-queue/kafka"

	unitCount = "1"

	msgProducedCountKey = "producer.messages.count"
	msgFetchedKey       = "consumer.messages.fetched"
	msgDelayKey         = "consumer.messages.delay"
	errorReasonKey      = "error_reason"
)

var (
	_ kgo.HookProduceBatchWritten     = new(metricHooks)
	_ kgo.HookFetchBatchRead          = new(metricHooks)
	_ kgo.HookFetchRecordUnbuffered   = new(metricHooks)
	_ kgo.HookProduceRecordUnbuffered = new(metricHooks)
)

// TopicAttributeFunc run on `kgo.HookProduceBatchWritten` and
// `kgo.HookFetchBatchRead` for each topic/partition. It can be
// used include additionaly dimensions for `consumer.messages.fetched`
// and `producer.messages.count` metrics.
type TopicAttributeFunc func(topic string) attribute.KeyValue

type metricHooks struct {
	namespace   string
	topicPrefix string

	messageProduced metric.Int64Counter
	messageFetched  metric.Int64Counter
	messageDelay    metric.Float64Histogram

	topicAttributeFunc TopicAttributeFunc
}

func newKgoHooks(mp metric.MeterProvider, namespace, topicPrefix string,
	topicAttributeFunc TopicAttributeFunc,
) (*metricHooks, error) {
	m := mp.Meter(instrumentName)
	messageProducedCounter, err := m.Int64Counter(msgProducedCountKey,
		metric.WithDescription("The number of messages produced"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		return nil, formatMetricError(msgProducedCountKey, err)
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
	if topicAttributeFunc == nil {
		topicAttributeFunc = func(topic string) attribute.KeyValue {
			return attribute.KeyValue{}
		}
	}

	return &metricHooks{
		namespace:   namespace,
		topicPrefix: topicPrefix,
		// Producer
		messageProduced: messageProducedCounter,
		// Consumer
		messageFetched: messageFetchedCounter,
		messageDelay:   messageDelayHistogram,

		topicAttributeFunc: topicAttributeFunc,
	}, nil
}

func formatMetricError(name string, err error) error {
	return fmt.Errorf("cannot create %s metric: %w", name, err)
}

// HookProduceBatchWritten is called when a batch has been produced.
func (h *metricHooks) OnProduceBatchWritten(_ kgo.BrokerMetadata,
	topic string, partition int32, m kgo.ProduceBatchMetrics,
) {
	attrs := make([]attribute.KeyValue, 0, 6) // Preallocate 5 elements.
	attrs = append(attrs, semconv.MessagingSystem("kafka"),
		semconv.MessagingDestinationName(strings.TrimPrefix(topic, h.topicPrefix)),
		semconv.MessagingKafkaDestinationPartition(int(partition)),
		attribute.String("outcome", "success"),
	)
	if kv := h.topicAttributeFunc(topic); kv != (attribute.KeyValue{}) {
		attrs = append(attrs, kv)
	}
	if h.namespace != "" {
		attrs = append(attrs, attribute.String("namespace", h.namespace))
	}
	h.messageProduced.Add(context.Background(), int64(m.NumRecords),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
}

// OnFetchBatchRead is called once per batch read from Kafka. Records
// `consumer.messages.fetched`.
func (h *metricHooks) OnFetchBatchRead(_ kgo.BrokerMetadata,
	topic string, partition int32, m kgo.FetchBatchMetrics,
) {
	attrs := make([]attribute.KeyValue, 0, 5) // Preallocate 5 elements.
	attrs = append(attrs, semconv.MessagingSystem("kafka"),
		semconv.MessagingSourceName(strings.TrimPrefix(topic, h.topicPrefix)),
		semconv.MessagingKafkaSourcePartition(int(partition)),
	)
	if kv := h.topicAttributeFunc(topic); kv != (attribute.KeyValue{}) {
		attrs = append(attrs, kv)
	}
	if h.namespace != "" {
		attrs = append(attrs, attribute.String("namespace", h.namespace))
	}
	h.messageFetched.Add(context.Background(), int64(m.NumRecords),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
}

// OnProduceRecordUnbuffered records the number of produced messages that were
// not produced due to errors. The successfully produced records is recorded by
// `OnProduceBatchWritten`.
// https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#HookProduceRecordUnbuffered
func (h *metricHooks) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	if err == nil {
		return // Covered by OnProduceBatchWritten.
	}
	attrs := attributesFromRecord(r,
		semconv.MessagingDestinationName(strings.TrimPrefix(r.Topic, h.topicPrefix)),
		semconv.MessagingKafkaDestinationPartition(int(r.Partition)),
		attribute.String("outcome", "failure"),
	)
	if h.namespace != "" {
		attrs = append(attrs, attribute.String("namespace", h.namespace))
	}

	if errors.Is(err, context.DeadlineExceeded) {
		attrs = append(attrs, attribute.String(errorReasonKey, "timeout"))
	} else if errors.Is(err, context.Canceled) {
		attrs = append(attrs, attribute.String(errorReasonKey, "canceled"))
	} else {
		attrs = append(attrs, attribute.String(errorReasonKey, "unknown"))
	}

	h.messageProduced.Add(context.Background(), 1, metric.WithAttributeSet(
		attribute.NewSet(attrs...),
	))
}

// OnFetchRecordUnbuffered records the message delay of fetched messages.
// https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#HookFetchRecordUnbuffered
func (h *metricHooks) OnFetchRecordUnbuffered(r *kgo.Record, polled bool) {
	if !polled {
		return // Record metrics when polled by `client.PollRecords()`.
	}
	attrs := attributesFromRecord(r,
		semconv.MessagingSourceName(strings.TrimPrefix(r.Topic, h.topicPrefix)),
		semconv.MessagingKafkaSourcePartition(int(r.Partition)),
	)
	if h.namespace != "" {
		attrs = append(attrs, attribute.String("namespace", h.namespace))
	}
	h.messageDelay.Record(context.Background(),
		time.Since(r.Timestamp).Seconds(),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
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
