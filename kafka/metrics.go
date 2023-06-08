package kafka

import (
	"context"
	"runtime"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentName = "github.com/elastic/apm-queue/kafka"
const unitCount = "1"

type instruments struct {
	MessageBuffered metric.Int64Counter
	MessageProduced metric.Int64Counter
	WriteErrors     metric.Int64Counter
}

type kgoHooks struct {
	instruments instruments
}

func NewKgoHooks(mp metric.MeterProvider) *kgoHooks {
	m := mp.Meter(instrumentName)

	a, err := m.Int64Counter(
		"message.produced",
		metric.WithDescription("The total number of message produced"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		panic(err)
	}

	b, err := m.Int64Counter(
		"write.error.count",
		metric.WithDescription("The total number of error occurred on write"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		panic(err)
	}

	c, err := m.Int64Counter(
		"message.buffered",
		metric.WithDescription("The total number of message buffered from produce batches"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		panic(err)
	}

	return &kgoHooks{
		instruments{
			MessageBuffered: c,
			MessageProduced: a,
			WriteErrors:     b,
		},
	}
}

// https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#HookProduceBatchWritten
func (h *kgoHooks) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, partition int32, metrics kgo.ProduceBatchMetrics) {
	runtime.Breakpoint()
	h.instruments.MessageProduced.Add(
		context.Background(),
		int64(metrics.NumRecords),
		metric.WithAttributes(
			attribute.Int("node", int(meta.NodeID)),
			attribute.Int("partition", int(partition)),
			attribute.String("host", meta.Host),
			attribute.String("topic", topic),
		))
}

func (h *kgoHooks) OnProduceRecordBuffered(r *kgo.Record) {
	runtime.Breakpoint()
	h.instruments.MessageBuffered.Add(
		context.Background(),
		1,
		metric.WithAttributes(
			attribute.Int("partition", int(r.Partition)),
			attribute.String("topic", r.Topic),
		),
	)
}

// https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#HookBrokerWrite
func (h *kgoHooks) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, _ int, writeWait, _ time.Duration, err error) {
	if err != nil {
		h.instruments.WriteErrors.Add(
			context.Background(),
			1,
			metric.WithAttributes(
				attribute.Int("node", int(meta.NodeID)),
				attribute.String("host", meta.Host),
			))
		return
	}
}
