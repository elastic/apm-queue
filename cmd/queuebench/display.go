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
	"time"

	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type metrics struct {
	disconnects   float64
	writeT, readT float64
}

func display(rm metricdata.ResourceMetrics, cd, pd time.Duration) (metrics, error) {
	franzMetrics := filterMetrics("github.com/twmb/franz-go/plugin/kotel", rm.ScopeMetrics)
	if len(franzMetrics) == 0 {
		return metrics{}, fmt.Errorf("expected some franz metrics, found none")
	}

	var res metrics
	for _, m := range franzMetrics {
		// log.Println(m.Name)
		var total int64
		for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
			// log.Printf("  %s | %s\n", m.Name, getAttrs(dp.Attributes))
			// log.Printf("  %s: value: %d\n", m.Name, dp.Value)
			total += dp.Value
		}
		d := pd
		if strings.Contains(m.Name, "read") || strings.Contains(m.Name, "fetch") {
			d = cd
		}
		switch m.Name {
		case "messaging.kafka.disconnects.count":
			res.disconnects = float64(total) / cd.Seconds()
		case "messaging.kafka.write_bytes":
			res.writeT = float64(total/1024/1024) / pd.Seconds()
		case "messaging.kafka.read_bytes.count":
			res.readT = float64(total/1024/1024) / cd.Seconds()
		}

		if strings.Contains(m.Name, "byte") {
			log.Printf("%s total: %02.fMB/s\n", m.Name, float64(total/1024/1024)/d.Seconds())
		} else {
			log.Printf("%s total: %02.f/s\n", m.Name, float64(total)/d.Seconds())
		}
	}

	// kafkaMetrics := filterMetrics("github.com/elastic/apm-queue/kafka", rm.ScopeMetrics)
	// if len(kafkaMetrics) == 0 {
	// 	return fmt.Errorf("expected some kafka metrics, found none")
	// }

	// avg := func(a metricdata.Extrema[float64], b metricdata.Extrema[float64]) float64 {
	// 	av, aok := a.Value()
	// 	bv, bok := b.Value()

	// 	if !aok {
	// 		av = 0.0
	// 	}
	// 	if !bok {
	// 		bv = 0.0
	// 	}

	// 	return (av + bv) / 2
	// }

	// for _, m := range kafkaMetrics {
	// 	log.Println(m.Name)

	// 	if md, ok := m.Data.(metricdata.Sum[int64]); ok {
	// 		for _, dp := range md.DataPoints {
	// 			log.Printf("  %s | %s\n", m.Name, getAttrs(dp.Attributes))
	// 			log.Printf("  %s: value: %d\n", m.Name, dp.Value)
	// 		}
	// 	}
	// 	if md, ok := m.Data.(metricdata.Histogram[float64]); ok {
	// 		for _, dp := range md.DataPoints {
	// 			log.Printf("  %s | %s", m.Name, getAttrs(dp.Attributes))

	// 			if v, ok := dp.Min.Value(); ok {
	// 				log.Printf("  %s: min   : %.3f\n", m.Name, v)
	// 			}
	// 			if v, ok := dp.Max.Value(); ok {
	// 				log.Printf("  %s: max   : %.3f\n", m.Name, v)
	// 			}
	// 			log.Printf("  %s: avg   : %.3f\n", m.Name, avg(dp.Min, dp.Max))
	// 			log.Printf("  %s: sum   : %.3f\n", m.Name, dp.Sum)
	// 			log.Printf("  %s: total : %d\n", m.Name, dp.Count)

	// 			top := strings.Builder{}
	// 			bottom := strings.Builder{}
	// 			for i, b := range dp.Bounds {
	// 				a := fmt.Sprintf("%.f\t", b)
	// 				top.WriteString(a)
	// 				width := fmt.Sprintf("%d", len(a)-1)
	// 				bottom.WriteString(fmt.Sprintf("%"+width+"d\t", dp.BucketCounts[i]))
	// 			}
	// 			log.Printf("  %s: bounds: %s\n", m.Name, top.String())
	// 			log.Printf("  %s: count : %s\n", m.Name, bottom.String())
	// 		}
	// 	}
	// }

	return res, nil
}

var times int
var usage float64
var max float64
var available int64

func displayMemUsage(rm metricdata.ResourceMetrics) {
	times++
	hostMetrics := filterMetrics(host.ScopeName, rm.ScopeMetrics)
	for _, m := range hostMetrics {
		switch m.Name {
		case "system.memory.usage":
			for _, dp := range m.Data.(metricdata.Gauge[int64]).DataPoints {
				switch getAttrs(dp.Attributes) {
				case "(state:available)":
					if available < dp.Value {
						available = dp.Value
					}
				}
			}
		case "system.memory.utilization":
			for _, dp := range m.Data.(metricdata.Gauge[float64]).DataPoints {
				switch getAttrs(dp.Attributes) {
				case "(state:used)":
					usage += dp.Value
					if max < dp.Value {
						max = dp.Value
					}
				}
			}
		}
	}
}

func getAttrs(a attribute.Set) string {
	s := strings.Builder{}
	for _, v := range a.ToSlice() {
		s.WriteString(fmt.Sprintf("(%s:%s)", v.Key, v.Value.AsString()))
	}
	return s.String()
}

func filterMetrics(instrumentName string, sm []metricdata.ScopeMetrics) []metricdata.Metrics {
	for _, m := range sm {
		if m.Scope.Name == instrumentName {
			return m.Metrics
		}
	}
	return []metricdata.Metrics{}
}
