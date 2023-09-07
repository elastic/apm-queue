package main

import (
	"math"

	"github.com/elastic/apm-queue/cmd/queuebench/pkg/benchmark"
)

func P(p float64, r benchmark.Histogram) float64 {
	if p < 0 || p > 100 {
		return math.NaN()
	}

	total := 0.0
	for _, v := range r.Counts {
		total += float64(v)
	}
	counts := r.Counts
	boundaries := r.Values

	percentile := 0.0
	for i, v := range counts {
		percentage := float64(v) * 100 / total
		if percentile+percentage > p {
			return boundaries[i]
		}
		percentile += percentage
	}
	return boundaries[len(boundaries)-1]
}
