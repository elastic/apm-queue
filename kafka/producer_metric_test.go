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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
)

func TestProducerMetrics_deadlineExceeded(t *testing.T) {
	producer, rdr := setupTestProducer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	err := producer.ProcessBatch(ctx, &model.Batch{
		model.APMEvent{Transaction: &model.Transaction{ID: "1"}},
		model.APMEvent{Transaction: &model.Transaction{ID: "2"}},
		model.APMEvent{Span: &model.Span{ID: "3"}},
	})
	assert.NoError(t, err)

	var rm metricdata.ResourceMetrics
	assert.NoError(t, rdr.Collect(context.Background(), &rm))

	metrics := extractMetrics(t, rm.ScopeMetrics)
	assert.Len(t, metrics, 1)
	metric := metrics[0]

	want := metricdata.Metrics{
		Name: "write.error",
		Data: metricdata.Sum[int64]{
			DataPoints: []metricdata.DataPoint[int64]{
				{Value: 3},
			},
		},
	}

	testMetric(t, want, metric)
}

func TestProducerMetrics_produced(t *testing.T) {
	producer, rdr := setupTestProducer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := producer.ProcessBatch(ctx, &model.Batch{
		model.APMEvent{Transaction: &model.Transaction{ID: "1"}},
		model.APMEvent{Transaction: &model.Transaction{ID: "2"}},
		model.APMEvent{Span: &model.Span{ID: "3"}},
	})
	assert.NoError(t, err)

	var rm metricdata.ResourceMetrics
	assert.NoError(t, rdr.Collect(context.Background(), &rm))

	metrics := extractMetrics(t, rm.ScopeMetrics)
	assert.Len(t, metrics, 1)
	metric := metrics[0]

	want := metricdata.Metrics{
		Name: "message.produced",
		Data: metricdata.Sum[int64]{
			DataPoints: []metricdata.DataPoint[int64]{
				{Value: 3},
			},
		},
	}

	testMetric(t, want, metric)
}

func extractMetrics(t *testing.T, sm []metricdata.ScopeMetrics) []metricdata.Metrics {
	t.Helper()

	for _, m := range sm {
		if m.Scope.Name == instrumentName {
			return m.Metrics
		}
	}

	return []metricdata.Metrics{}
}

func testMetric(t *testing.T, want metricdata.Metrics, actual metricdata.Metrics) {
	t.Helper()

	assert.Equal(t, want.Name, actual.Name)

	wantValue := want.Data.(metricdata.Sum[int64]).DataPoints[0].Value
	metricValue := actual.Data.(metricdata.Sum[int64]).DataPoints[0].Value
	assert.Equal(t, wantValue, metricValue)
}

func setupTestProducer(t *testing.T) (*Producer, sdkmetric.Reader) {
	t.Helper()

	rdr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr))

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

	return producer, rdr
}
