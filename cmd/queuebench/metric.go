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
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

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
