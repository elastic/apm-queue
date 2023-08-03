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
	"fmt"
	"strings"

	"github.com/gosuri/uitable"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func display(rm metricdata.ResourceMetrics) {
	franzMetrics := filterMetrics("github.com/twmb/franz-go/plugin/kotel", rm.ScopeMetrics)
	if len(franzMetrics) == 0 {
		panic("there should be something")
	}

	kafkaMetrics := filterMetrics("github.com/elastic/apm-queue/kafka", rm.ScopeMetrics)
	if len(kafkaMetrics) == 0 {
		panic("there should be something")
	}

	table := uitable.New()
	table.Wrap = true
	table.MaxColWidth = 80

	table.AddRow("SECTION", "VALUE", "ATTRS")

	tablelize := func(metrics []metricdata.Metrics, n string) {
		m, found := getMetric(metrics, n)
		if found {
			var v any

			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				v = displayDatapoints(d)
			case metricdata.Histogram[float64]:
				v = displayHistogram(d)
			}

			table.AddRow(n, v)
		} else {
			table.AddRow(n, 0)
		}
	}

	tablelize(franzMetrics, "messaging.kafka.produce_records.count")
	tablelize(franzMetrics, "messaging.kafka.fetch_records.count")
	tablelize(franzMetrics, "messaging.kafka.produce_bytes.count")
	tablelize(franzMetrics, "messaging.kafka.fetch_bytes.count")
	tablelize(kafkaMetrics, "producer.messages.produced")
	tablelize(kafkaMetrics, "consumer.messages.fetched")
	tablelize(kafkaMetrics, "consumer.messages.delay")

	fmt.Println()
	fmt.Println(table)
	fmt.Println()
}

func filterMetrics(instrumentName string, sm []metricdata.ScopeMetrics) []metricdata.Metrics {
	for _, m := range sm {
		if m.Scope.Name == instrumentName {
			return m.Metrics
		}
	}
	return []metricdata.Metrics{}
}

func getMetric(o []metricdata.Metrics, name string) (metricdata.Metrics, bool) {
	for _, m := range o {
		if m.Name == name {
			return m, true
		}
	}

	return metricdata.Metrics{}, false
}

func displayDatapoints(m metricdata.Sum[int64]) int {
	var tot int64
	tot = 0

	for _, dp := range m.DataPoints {
		tot += dp.Value
	}

	return int(tot)
}

func displayHistogram(m metricdata.Histogram[float64]) string {
	s := strings.Builder{}

	maybeWriteValue := func(s *strings.Builder, n string, e metricdata.Extrema[float64]) {
		if v, ok := e.Value(); ok {
			s.WriteString(fmt.Sprintf("%s: %.3f\n", n, v))
		}
	}

	avg := func(a metricdata.Extrema[float64], b metricdata.Extrema[float64]) float64 {
		av, aok := a.Value()
		bv, bok := b.Value()

		if !aok {
			av = 0.0
		}
		if !bok {
			bv = 0.0
		}

		return (av + bv) / 2
	}

	for _, dp := range m.DataPoints {
		maybeWriteValue(&s, "min", dp.Min)
		maybeWriteValue(&s, "max", dp.Max)
		s.WriteString(fmt.Sprintf("avg: %.3f\n", avg(dp.Min, dp.Max)))
		s.WriteString(fmt.Sprintf("sum: %.3f\n", dp.Sum))

		top := strings.Builder{}
		bottom := strings.Builder{}
		for i, b := range dp.Bounds {
			a := fmt.Sprintf("%.f\t", b)
			top.WriteString(a)
			width := fmt.Sprintf("%d", len(a)-1)
			bottom.WriteString(fmt.Sprintf("%"+width+"d\t", dp.BucketCounts[i]))
		}
		s.WriteString(fmt.Sprintf("bounds: %s\n", top.String()))
		s.WriteString(fmt.Sprintf("count : %s\n", bottom.String()))
	}

	return s.String()
}
