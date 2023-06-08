//go:build metrics
// +build metrics

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// run this test with:
// go test --tags=metrics -run
// go test -v -run TestProducerMetrics --tags=metrics
func TestProducerMetrics(t *testing.T) {
	rdr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr))

	otel.SetMeterProvider(mp)

	topic := apmqueue.Topic("default-topic")
	_, brokers := newClusterWithTopics(t, topic)
	codec := json.JSON{}
	producer, err := NewProducer(ProducerConfig{
		CommonConfig: CommonConfig{
			Brokers:        brokers,
			Logger:         zap.NewNop(),
			TracerProvider: trace.NewNoopTracerProvider(),
			MeterProvider:  mp,
		},
		Sync:    true,
		Encoder: codec,
		TopicRouter: func(event model.APMEvent) apmqueue.Topic {
			return topic
		},
	})
	require.NoError(t, err)
	require.NotNil(t, producer)

	// NOTE: these tests are flaky
	// Using go-cmp instead of metricdatatest.AssertEqual allow to bypass
	// checkes on all metric data fields, which in some cases mey result in
	// flakiness (i.e. attribute changing value).
	// But that's not enough to remove all flakiness: as Producer is using 2
	// meter provider, their order in the rm.ScopeMetrics slice may change,
	// thus making these tests fails.
	// You can verify this with:
	// go test -v -run TestProducerMetrics --tags=metrics -race -count=100 ./kafka/

	// This custom comparer checks for equality only in Name and first
	// datapoint value.
	opt := cmp.Comparer(func(x, y metricdata.Metrics) bool {
		equalName := x.Name == y.Name
		equalValue := x.Data.(metricdata.Sum[int64]).DataPoints[0].Value ==
			y.Data.(metricdata.Sum[int64]).DataPoints[0].Value
		return equalName && equalValue

	})

	t.Run("deadline exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0*time.Second)
		defer cancel()

		err = producer.ProcessBatch(ctx, &model.Batch{
			model.APMEvent{Transaction: &model.Transaction{ID: "1"}},
		})
		assert.NoError(t, err)

		var rm metricdata.ResourceMetrics
		assert.NoError(t, rdr.Collect(context.Background(), &rm))

		metric := rm.ScopeMetrics[1].Metrics[1]
		want := metricdata.Metrics{
			Name: "write.timeout.count",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 1,
					},
				},
			},
		}

		assert.True(t, cmp.Equal(want, metric, opt))
	})

	t.Run("produced", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		err = producer.ProcessBatch(ctx, &model.Batch{
			model.APMEvent{Transaction: &model.Transaction{ID: "1"}},
		})
		assert.NoError(t, err)

		var rm metricdata.ResourceMetrics
		assert.NoError(t, rdr.Collect(context.Background(), &rm))

		metric := rm.ScopeMetrics[1].Metrics[0]
		want := metricdata.Metrics{
			Name: "message.produced.count",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 1,
					},
				},
			},
		}

		assert.True(t, cmp.Equal(want, metric, opt))
	})
}
