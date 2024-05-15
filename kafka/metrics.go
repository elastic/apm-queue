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
	"net"
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
	unitBytes = "By"

	msgProducedCountKey             = "producer.messages.count"
	msgProducedBytesKey             = "producer.messages.bytes"
	msgProducedWireBytesKey         = "producer.messages.wire.bytes"
	msgProducedUncompressedBytesKey = "producer.messages.uncompressed.bytes"
	msgFetchedKey                   = "consumer.messages.fetched"
	msgDelayKey                     = "consumer.messages.delay"
	msgConsumedBytesKey             = "consumer.messages.bytes"
	msgConsumedWireBytesKey         = "consumer.messages.wire.bytes"
	msgConsumedUncompressedBytesKey = "consumer.messages.uncompressed.bytes"
	throttlingDurationKey           = "messaging.kafka.throttling.duration"
	errorReasonKey                  = "error_reason"
)

var (
	_ kgo.HookBrokerConnect           = new(metricHooks)
	_ kgo.HookBrokerDisconnect        = new(metricHooks)
	_ kgo.HookBrokerWrite             = new(metricHooks)
	_ kgo.HookBrokerRead              = new(metricHooks)
	_ kgo.HookProduceBatchWritten     = new(metricHooks)
	_ kgo.HookFetchBatchRead          = new(metricHooks)
	_ kgo.HookFetchRecordUnbuffered   = new(metricHooks)
	_ kgo.HookProduceRecordUnbuffered = new(metricHooks)
	_ kgo.HookBrokerThrottle          = new(metricHooks)
)

// TopicAttributeFunc run on `kgo.HookProduceBatchWritten` and
// `kgo.HookFetchBatchRead` for each topic/partition. It can be
// used include additionaly dimensions for `consumer.messages.fetched`
// and `producer.messages.count` metrics.
type TopicAttributeFunc func(topic string) attribute.KeyValue

type metricHooks struct {
	namespace   string
	topicPrefix string

	// kotel metrics

	connects    metric.Int64Counter
	connectErrs metric.Int64Counter
	disconnects metric.Int64Counter

	writeErrs  metric.Int64Counter
	writeBytes metric.Int64Counter

	readErrs  metric.Int64Counter
	readBytes metric.Int64Counter

	produceBytes   metric.Int64Counter
	produceRecords metric.Int64Counter
	fetchBytes     metric.Int64Counter
	fetchRecords   metric.Int64Counter

	// custom metrics
	messageProduced                  metric.Int64Counter
	messageProducedBytes             metric.Int64Counter
	messageProducedWireBytes         metric.Int64Counter
	messageProducedUncompressedBytes metric.Int64Counter
	messageFetched                   metric.Int64Counter
	messageFetchedBytes              metric.Int64Counter
	messageFetchedWireBytes          metric.Int64Counter
	messageFetchedUncompressedBytes  metric.Int64Counter
	messageDelay                     metric.Float64Histogram
	throttlingDuration               metric.Float64Histogram

	topicAttributeFunc TopicAttributeFunc
}

func newKgoHooks(mp metric.MeterProvider, namespace, topicPrefix string,
	topicAttributeFunc TopicAttributeFunc,
) (*metricHooks, error) {
	m := mp.Meter(instrumentName)

	// kotel metrics

	// connects and disconnects
	connects, err := m.Int64Counter(
		"messaging.kafka.connects.count",
		metric.WithUnit(unitCount),
		metric.WithDescription("Total number of connections opened, by broker"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connects instrument, %w", err)
	}

	connectErrs, err := m.Int64Counter(
		"messaging.kafka.connect_errors.count",
		metric.WithUnit(unitCount),
		metric.WithDescription("Total number of connection errors, by broker"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connectErrs instrument, %w", err)
	}

	disconnects, err := m.Int64Counter(
		"messaging.kafka.disconnects.count",
		metric.WithUnit(unitCount),
		metric.WithDescription("Total number of connections closed, by broker"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create disconnects instrument, %w", err)
	}

	// write

	writeErrs, err := m.Int64Counter(
		"messaging.kafka.write_errors.count",
		metric.WithUnit(unitCount),
		metric.WithDescription("Total number of write errors, by broker"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create writeErrs instrument, %w", err)
	}

	writeBytes, err := m.Int64Counter(
		"messaging.kafka.write_bytes",
		metric.WithUnit(unitBytes),
		metric.WithDescription("Total number of bytes written, by broker"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create writeBytes instrument, %w", err)
	}

	// read

	readErrs, err := m.Int64Counter(
		"messaging.kafka.read_errors.count",
		metric.WithUnit(unitCount),
		metric.WithDescription("Total number of read errors, by broker"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create readErrs instrument, %w", err)
	}

	readBytes, err := m.Int64Counter(
		"messaging.kafka.read_bytes.count",
		metric.WithUnit(unitBytes),
		metric.WithDescription("Total number of bytes read, by broker"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create readBytes instrument, %w", err)
	}

	// produce & consume

	produceBytes, err := m.Int64Counter(
		"messaging.kafka.produce_bytes.count",
		metric.WithUnit(unitBytes),
		metric.WithDescription("Total number of uncompressed bytes produced, by broker and topic"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create produceBytes instrument, %w", err)
	}

	produceRecords, err := m.Int64Counter(
		"messaging.kafka.produce_records.count",
		metric.WithUnit(unitCount),
		metric.WithDescription("Total number of produced records, by broker and topic"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create produceRecords instrument, %w", err)
	}

	fetchBytes, err := m.Int64Counter(
		"messaging.kafka.fetch_bytes.count",
		metric.WithUnit(unitBytes),
		metric.WithDescription("Total number of uncompressed bytes fetched, by broker and topic"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create fetchBytes instrument, %w", err)
	}

	fetchRecords, err := m.Int64Counter(
		"messaging.kafka.fetch_records.count",
		metric.WithUnit(unitCount),
		metric.WithDescription("Total number of fetched records, by broker and topic"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create fetchRecords instrument, %w", err)
	}

	// custom metrics
	messageProducedCounter, err := m.Int64Counter(msgProducedCountKey,
		metric.WithDescription("The number of messages produced"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		return nil, formatMetricError(msgProducedCountKey, err)
	}
	messageProducedBytes, err := m.Int64Counter(msgProducedBytesKey,
		metric.WithDescription("The number of bytes produced"),
		metric.WithUnit(unitBytes),
	)
	if err != nil {
		return nil, formatMetricError(msgProducedBytesKey, err)
	}
	messageProducedWireBytes, err := m.Int64Counter(msgProducedWireBytesKey,
		metric.WithDescription("The number of bytes produced"),
		metric.WithUnit(unitBytes),
	)
	if err != nil {
		return nil, formatMetricError(msgProducedBytesKey, err)
	}

	messageProducedUncompressedBytes, err := m.Int64Counter(msgProducedUncompressedBytesKey,
		metric.WithDescription("The number of uncompressed bytes produced"),
		metric.WithUnit(unitBytes),
	)
	if err != nil {
		return nil, formatMetricError(msgProducedUncompressedBytesKey, err)
	}

	messageFetchedCounter, err := m.Int64Counter(msgFetchedKey,
		metric.WithDescription("The number of messages that were fetched from a kafka topic"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		return nil, formatMetricError(msgFetchedKey, err)
	}
	messageFetchedBytes, err := m.Int64Counter(msgConsumedBytesKey,
		metric.WithDescription("The number of bytes consumed"),
		metric.WithUnit(unitBytes),
	)
	if err != nil {
		return nil, formatMetricError(msgConsumedBytesKey, err)
	}
	messageFetchedWireBytes, err := m.Int64Counter(msgConsumedWireBytesKey,
		metric.WithDescription("The number of bytes consumed"),
		metric.WithUnit(unitBytes),
	)
	if err != nil {
		return nil, formatMetricError(msgConsumedBytesKey, err)
	}

	messageFetchedUncompressedBytes, err := m.Int64Counter(msgConsumedUncompressedBytesKey,
		metric.WithDescription("The number of uncompressed bytes consumed"),
		metric.WithUnit(unitBytes),
	)
	if err != nil {
		return nil, formatMetricError(msgConsumedUncompressedBytesKey, err)
	}

	messageDelayHistogram, err := m.Float64Histogram(msgDelayKey,
		metric.WithDescription("The delay between producing messages and reading them"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, formatMetricError(msgDelayKey, err)
	}

	throttlingDurationHistogram, err := m.Float64Histogram(throttlingDurationKey,
		metric.WithDescription("The throttling interval imposed by the broker"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, formatMetricError(throttlingDurationKey, err)
	}

	if topicAttributeFunc == nil {
		topicAttributeFunc = func(topic string) attribute.KeyValue {
			return attribute.KeyValue{}
		}
	}

	return &metricHooks{
		namespace:   namespace,
		topicPrefix: topicPrefix,
		// kotel metrics
		connects:    connects,
		connectErrs: connectErrs,
		disconnects: disconnects,

		writeErrs:  writeErrs,
		writeBytes: writeBytes,

		readErrs:  readErrs,
		readBytes: readBytes,

		produceBytes:   produceBytes,
		produceRecords: produceRecords,
		fetchBytes:     fetchBytes,
		fetchRecords:   fetchRecords,

		// custom metrics

		// Producer
		messageProduced:                  messageProducedCounter,
		messageProducedBytes:             messageProducedBytes,
		messageProducedWireBytes:         messageProducedWireBytes,
		messageProducedUncompressedBytes: messageProducedUncompressedBytes,
		// Consumer
		messageFetched:                  messageFetchedCounter,
		messageFetchedBytes:             messageFetchedBytes,
		messageFetchedWireBytes:         messageFetchedWireBytes,
		messageFetchedUncompressedBytes: messageFetchedUncompressedBytes,
		messageDelay:                    messageDelayHistogram,
		throttlingDuration:              throttlingDurationHistogram,

		topicAttributeFunc: topicAttributeFunc,
	}, nil
}

func formatMetricError(name string, err error) error {
	return fmt.Errorf("cannot create %s metric: %w", name, err)
}

func (h *metricHooks) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	attrs := make([]attribute.KeyValue, 0, 2)
	attrs = append(attrs, semconv.MessagingSystem("kafka"))
	if h.namespace != "" {
		attrs = append(attrs, attribute.String("namespace", h.namespace))
	}
	if err != nil {
		h.connectErrs.Add(
			context.Background(),
			1,
			metric.WithAttributeSet(attribute.NewSet(attrs...)),
		)
		return
	}
	h.connects.Add(
		context.Background(),
		1,
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
}

func (h *metricHooks) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	attrs := make([]attribute.KeyValue, 0, 2)
	attrs = append(attrs, semconv.MessagingSystem("kafka"))
	if h.namespace != "" {
		attrs = append(attrs, attribute.String("namespace", h.namespace))
	}
	h.disconnects.Add(
		context.Background(),
		1,
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
}

func (h *metricHooks) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, _, _ time.Duration, err error) {
	attrs := make([]attribute.KeyValue, 0, 2)
	attrs = append(attrs, semconv.MessagingSystem("kafka"))
	if h.namespace != "" {
		attrs = append(attrs, attribute.String("namespace", h.namespace))
	}
	if err != nil {
		h.writeErrs.Add(
			context.Background(),
			1,
			metric.WithAttributeSet(attribute.NewSet(attrs...)),
		)
		return
	}
	h.writeBytes.Add(
		context.Background(),
		int64(bytesWritten),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
}

func (h *metricHooks) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, _, _ time.Duration, err error) {
	attrs := make([]attribute.KeyValue, 0, 2)
	attrs = append(attrs, semconv.MessagingSystem("kafka"))
	if h.namespace != "" {
		attrs = append(attrs, attribute.String("namespace", h.namespace))
	}
	if err != nil {
		h.readErrs.Add(
			context.Background(),
			1,
			metric.WithAttributeSet(attribute.NewSet(attrs...)),
		)
		return
	}
	h.readBytes.Add(
		context.Background(),
		int64(bytesRead),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
}

// HookProduceBatchWritten is called when a batch has been produced.
func (h *metricHooks) OnProduceBatchWritten(meta kgo.BrokerMetadata,
	topic string, partition int32, m kgo.ProduceBatchMetrics,
) {
	attrs := make([]attribute.KeyValue, 0, 7)
	attrs = append(attrs, semconv.MessagingSystem("kafka"),
		attribute.String("topic", topic),
		semconv.MessagingDestinationName(strings.TrimPrefix(topic, h.topicPrefix)),
		semconv.MessagingKafkaDestinationPartition(int(partition)),
		attribute.String("outcome", "success"),
		attribute.String("compression.codec", compressionFromCodec(m.CompressionType)),
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
	h.messageProducedBytes.Add(context.Background(), int64(m.CompressedBytes),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
	h.messageProducedWireBytes.Add(context.Background(), int64(m.CompressedBytes),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
	h.messageProducedUncompressedBytes.Add(context.Background(), int64(m.UncompressedBytes),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
	// kotel metrics
	h.produceBytes.Add(
		context.Background(),
		int64(m.UncompressedBytes),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
	h.produceRecords.Add(
		context.Background(),
		int64(m.NumRecords),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)

}

// OnFetchBatchRead is called once per batch read from Kafka. Records
// `consumer.messages.fetched`.
func (h *metricHooks) OnFetchBatchRead(meta kgo.BrokerMetadata,
	topic string, partition int32, m kgo.FetchBatchMetrics,
) {
	attrs := make([]attribute.KeyValue, 0, 6)
	attrs = append(attrs, semconv.MessagingSystem("kafka"),
		attribute.String("topic", topic),
		semconv.MessagingSourceName(strings.TrimPrefix(topic, h.topicPrefix)),
		semconv.MessagingKafkaSourcePartition(int(partition)),
		attribute.String("compression.codec", compressionFromCodec(m.CompressionType)),
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
	h.messageFetchedBytes.Add(context.Background(), int64(m.CompressedBytes),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
	h.messageFetchedUncompressedBytes.Add(context.Background(), int64(m.UncompressedBytes),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)

	h.fetchBytes.Add(
		context.Background(),
		int64(m.UncompressedBytes),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
	h.fetchRecords.Add(
		context.Background(),
		int64(m.NumRecords),
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
		attribute.String("topic", r.Topic),
		semconv.MessagingDestinationName(strings.TrimPrefix(r.Topic, h.topicPrefix)),
		semconv.MessagingKafkaDestinationPartition(int(r.Partition)),
		attribute.String("outcome", "failure"),
	)
	if kv := h.topicAttributeFunc(r.Topic); kv != (attribute.KeyValue{}) {
		attrs = append(attrs, kv)
	}
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
		attribute.String("topic", r.Topic),
		semconv.MessagingSourceName(strings.TrimPrefix(r.Topic, h.topicPrefix)),
		semconv.MessagingKafkaSourcePartition(int(r.Partition)),
	)
	if kv := h.topicAttributeFunc(r.Topic); kv != (attribute.KeyValue{}) {
		attrs = append(attrs, kv)
	}
	if h.namespace != "" {
		attrs = append(attrs, attribute.String("namespace", h.namespace))
	}
	h.messageDelay.Record(context.Background(),
		time.Since(r.Timestamp).Seconds(),
		metric.WithAttributeSet(attribute.NewSet(attrs...)),
	)
}

func (h *metricHooks) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, throttledAfterResponse bool) {
	attrs := make([]attribute.KeyValue, 0, 2)
	attrs = append(attrs, semconv.MessagingSystem("kafka"))
	if h.namespace != "" {
		attrs = append(attrs, attribute.String("namespace", h.namespace))
	}
	h.throttlingDuration.Record(context.Background(),
		throttleInterval.Seconds(),
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

func compressionFromCodec(c uint8) string {
	// CompressionType signifies which algorithm the batch was compressed
	// with.
	//
	// 0 is no compression, 1 is gzip, 2 is snappy, 3 is lz4, and 4 is
	// zstd.
	switch c {
	case 0:
		return "none"
	case 1:
		return "gzip"
	case 2:
		return "snappy"
	case 3:
		return "lz4"
	case 4:
		return "zstd"
	default:
		return "unknown"
	}
}
