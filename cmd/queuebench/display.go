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
	"log"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func display(rm metricdata.ResourceMetrics) error {
	getAttrs := func(a attribute.Set) string {
		s := strings.Builder{}
		for _, v := range a.ToSlice() {
			s.WriteString(fmt.Sprintf("(%s:%s)", v.Key, v.Value.AsString()))
		}
		return s.String()
	}

	franzMetrics := filterMetrics("github.com/twmb/franz-go/plugin/kotel", rm.ScopeMetrics)
	if len(franzMetrics) == 0 {
		return fmt.Errorf("expected some franz metrics, found none")
	}

	for _, m := range franzMetrics {
		log.Println(m.Name)

		for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
			log.Printf("  %s | %s\n", m.Name, getAttrs(dp.Attributes))
			log.Printf("  %s: value: %d\n", m.Name, dp.Value)
		}
	}

	kafkaMetrics := filterMetrics("github.com/elastic/apm-queue/kafka", rm.ScopeMetrics)
	if len(kafkaMetrics) == 0 {
		return fmt.Errorf("expected some kafka metrics, found none")
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

	for _, m := range kafkaMetrics {
		log.Println(m.Name)

		if md, ok := m.Data.(metricdata.Sum[int64]); ok {
			for _, dp := range md.DataPoints {
				log.Printf("  %s | %s\n", m.Name, getAttrs(dp.Attributes))
				log.Printf("  %s: value: %d\n", m.Name, dp.Value)
			}
		}
		if md, ok := m.Data.(metricdata.Histogram[float64]); ok {
			for _, dp := range md.DataPoints {
				log.Printf("  %s | %s", m.Name, getAttrs(dp.Attributes))

				if v, ok := dp.Min.Value(); ok {
					log.Printf("  %s: min   : %.3f\n", m.Name, v)
				}
				if v, ok := dp.Max.Value(); ok {
					log.Printf("  %s: max   : %.3f\n", m.Name, v)
				}
				log.Printf("  %s: avg   : %.3f\n", m.Name, avg(dp.Min, dp.Max))
				log.Printf("  %s: sum   : %.3f\n", m.Name, dp.Sum)
				log.Printf("  %s: total : %d\n", m.Name, dp.Count)

				top := strings.Builder{}
				bottom := strings.Builder{}
				for i, b := range dp.Bounds {
					a := fmt.Sprintf("%f\t", b)
					top.WriteString(a)
					width := fmt.Sprintf("%d", len(a)-1)
					bottom.WriteString(fmt.Sprintf("%"+width+"d\t", dp.BucketCounts[i]))
				}
				log.Printf("  %s: bounds: %s\n", m.Name, top.String())
				log.Printf("  %s: count : %s\n", m.Name, bottom.String())
			}
		}
	}

	return nil
}

func filterMetrics(instrumentName string, sm []metricdata.ScopeMetrics) []metricdata.Metrics {
	for _, m := range sm {
		if m.Scope.Name == instrumentName {
			return m.Metrics
		}
	}
	return []metricdata.Metrics{}
}
