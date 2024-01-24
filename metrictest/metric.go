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

// Package metrictest provides helpers for metric testing.
package metrictest

import (
	"context"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestMetric returns all the necessary for tests.
type TestMetric struct {
	Reader        sdkmetric.Reader
	MeterProvider *sdkmetric.MeterProvider
	Meter         metric.Meter
}

// New creates a manual reader, meter provider and a meter from it.
func New() (tm TestMetric) {
	tm.Reader = sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
		func(ik sdkmetric.InstrumentKind) metricdata.Temporality {
			return metricdata.DeltaTemporality
		},
	))
	tm.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(tm.Reader))
	tm.Meter = tm.MeterProvider.Meter("test")
	return
}

// Collect returns the metrics from the reader.
func (tm TestMetric) Collect(ctx context.Context) (
	rm metricdata.ResourceMetrics, err error,
) {
	err = tm.Reader.Collect(ctx, &rm)
	return
}

// Key holds the key for the metric name and unit.
type Key struct{ Name, Unit string }

// KV holds the key and value for a dimension
type KV struct{ K, V string }

// Int64Metrics defines a metric to dimension mapping for int64 metrics.
type Int64Metrics map[Key]Dimension

// Dimension defines a generic dimension.
type Dimension map[KV]int64

// GatherInt64Metric gathers the observed int64 metrics grouped by dimension.
func GatherInt64Metric(ms []metricdata.Metrics) Int64Metrics {
	observed := Int64Metrics{}
	for _, m := range ms {
		valueMap := Dimension{}
		switch data := m.Data.(type) {
		case metricdata.Sum[int64]:
			for _, dp := range data.DataPoints {
				if dp.Attributes.Len() == 0 {
					valueMap[KV{}] += int64(dp.Value)
				}
				iter := dp.Attributes.Iter()
				for iter.Next() {
					attr := iter.Attribute()
					valueMap[KV{string(attr.Key), attr.Value.Emit()}] += int64(dp.Value)
				}
			}
		default:
			continue
		}
		observed[Key{Name: m.Name, Unit: m.Unit}] = valueMap
	}
	return observed
}
