package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentName = "github.com/elastic/apm-queue/kafka"
const unitCount = "1"

type instruments struct {
	MessageProduced metric.Int64Counter
	WriteErrors     metric.Int64Counter
	WriteTimeout    metric.Int64Counter
}

type kgoHooks struct {
	instruments instruments
}

func NewKgoHooks(mp metric.MeterProvider) *kgoHooks {
	m := mp.Meter(instrumentName)

	a, err := m.Int64Counter(
		"message.produced.count",
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
		"write.timeout.count",
		metric.WithDescription("The total number of messages not produced due to timeout"),
		metric.WithUnit(unitCount),
	)
	if err != nil {
		panic(err)
	}

	return &kgoHooks{
		instruments{
			MessageProduced: a,
			WriteErrors:     b,
			WriteTimeout:    c,
		},
	}
}

func (h *kgoHooks) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			h.instruments.WriteTimeout.Add(
				context.Background(),
				1,
				metric.WithAttributes(
					attribute.Int("partition", int(r.Partition)),
					attribute.String("topic", r.Topic),
				))
		}
		h.instruments.WriteErrors.Add(
			context.Background(),
			1,
			metric.WithAttributes(
				attribute.Int("partition", int(r.Partition)),
				attribute.String("topic", r.Topic),
			))
		return
	}

	h.instruments.MessageProduced.Add(
		context.Background(),
		1,
		metric.WithAttributes(
			attribute.Int("partition", int(r.Partition)),
			attribute.String("topic", r.Topic),
		))

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
