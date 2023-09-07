package main

import (
	"testing"

	"github.com/elastic/apm-queue/cmd/queuebench/pkg/benchmark"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestP(t *testing.T) {
	data := fixture(t, "testdata/1.json")

	assert.Equal(t, 750.0, P(50, data.ConsumptionDelay))
	assert.Equal(t, 1000.0, P(90, data.ConsumptionDelay))
	assert.Equal(t, 2500.0, P(95, data.ConsumptionDelay))
}

func fixture(t *testing.T, name string) benchmark.Result {
	t.Helper()
	data, err := benchmark.LoadFromJSON(name)
	require.NoError(t, err)
	return data
}
