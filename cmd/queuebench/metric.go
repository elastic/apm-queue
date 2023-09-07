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

package main

import (
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// taken from https://github.com/elastic/apm-agent-go/blob/8041dd706d18cb72693f15534c54b390050f0a54/module/apmotel/gatherer_config.go#L25
var customHistogramBoundaries = []float64{
	0.00390625, 0.00552427, 0.0078125, 0.0110485, 0.015625, 0.0220971, 0.03125,
	0.0441942, 0.0625, 0.0883883, 0.125, 0.176777, 0.25, 0.353553, 0.5, 0.707107,
	1, 1.41421, 2, 2.82843, 4, 5.65685, 8, 11.3137, 16, 22.6274, 32, 45.2548, 64,
	90.5097, 128, 181.019, 256, 362.039, 512, 724.077, 1024, 1448.15, 2048,
	2896.31, 4096, 5792.62, 8192, 11585.2, 16384, 23170.5, 32768, 46341.0, 65536,
	92681.9, 131072,
}

func metering() (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	rdr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(rdr),
		sdkmetric.WithView(
			sdkmetric.NewView(
				sdkmetric.Instrument{
					Name:  "consumer.messages.delay",
					Scope: instrumentation.Scope{Name: "github.com/elastic/apm-queue/kafka"},
				},
				sdkmetric.Stream{
					Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
						Boundaries: customHistogramBoundaries,
					},
				},
			),
		),
	)
	return mp, rdr
}

func sum(dps []metricdata.DataPoint[int64]) (val int64) {
	for _, dp := range dps {
		val += dp.Value
	}
	return val
}

func getSumInt64Metric(instrument string, metric string, rm metricdata.ResourceMetrics) int64 {
	metrics := filterMetrics(instrument, rm.ScopeMetrics)
	if len(metrics) == 0 {
		return 0
	}

	for _, m := range metrics {
		if m.Name == metric {
			return sum(m.Data.(metricdata.Sum[int64]).DataPoints)
		}
	}

	return 0
}

type HistogramResult struct {
	Min   float64
	Max   float64
	Avg   float64
	Sum   float64
	Count uint64

	Boundaries []float64
	Counts     []uint64
}

func getHistogramFloat64Metric(instrument, metric string, rm metricdata.ResourceMetrics) HistogramResult {
	metrics := filterMetrics(instrument, rm.ScopeMetrics)
	if len(metrics) == 0 {
		return HistogramResult{}
	}

	var values metricdata.Histogram[float64]
	for _, m := range metrics {
		if m.Name == metric {
			values = m.Data.(metricdata.Histogram[float64])
		}
	}

	data := values.DataPoints[0]

	getValueOrEmpty := func(value metricdata.Extrema[float64]) float64 {
		if v, ok := value.Value(); ok {
			return v
		}
		return 0.0
	}
	avg := func(a metricdata.Extrema[float64], b metricdata.Extrema[float64]) float64 {
		return (getValueOrEmpty(a) + getValueOrEmpty(b)) / 2
	}

	return HistogramResult{
		Min:        getValueOrEmpty(data.Min),
		Max:        getValueOrEmpty(data.Max),
		Avg:        avg(data.Min, data.Max),
		Sum:        data.Sum,
		Count:      data.Count,
		Boundaries: data.Bounds,
		Counts:     data.BucketCounts,
	}
}
