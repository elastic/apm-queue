package benchmark

import (
	"time"
)

// Result represent all information for a successful benchmark run.
// NOTE: this struct is ES compatible.
type Result struct {
	// Meta contains metadata about the benchmark run.
	Meta Meta `json:"meta"`
	// Duration contains time based information on the benchmark run.
	Duration Duration `json:"duration"`
	// Produced is the number of records produced during the benchmark, as reported by otel metric github.com/elastic/apm-queue/kafka.producer.messages.produced.
	Produced int64 `json:"produced"`
	// ProducedBytes is the number of bytes produced during the benchmark, as reported by otel metric github.com/twmb/franz-go/plugin/kotel.messaging.kafka.produce_bytes.count.
	ProducedBytes int64 `json:"produced_bytes"`
	// Consumed is the number of records consumed during the benchmark, as reported by otel metric github.com/elastic/apm-queue/kafka.producer.messages.consumed.
	Consumed int64 `json:"consumed"`
	// ConsumedBytes is the number of bytes consumed during the benchmark, as reported by otel metric github.com/twmb/franz-go/plugin/kotel.messaging.kafka.fetch_bytes.count.
	ConsumedBytes int64 `json:"consumed_bytes"`
	// Leftover is the number of records produced and not consumed.
	Leftover int64 `json:"leftover"`
	// ConsumptionDelay contains consumer delay information for the benchmark run, as reported by otel metric github.com/elastic/apm-queue/kafka.consumer.messages.delay.
	ConsumptionDelay    Histogram `json:"consumption_delay"`
	MinConsumptionDelay float64   `json:"min"`
	MaxConsumptionDelay float64   `json:"max"`
	SumConsumptionDelay float64   `json:"sum"`
	// Total is the sum of all samples count.
	ConsumptionDelayTotalCount uint64 `json:"count"`

	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P95 float64 `json:"p95"`
}

type Meta struct {
	// RunID is the unique run ID for a specific benchmark run.
	RunID string `json:"run_id"`
	// StartTime is the UTC time the benchmark started.
	StartTime time.Time `json:"start_time"`
	// EndTime is the UTC time the benchmark ended.
	EndTime time.Time `json:"end_time"`
	// Config contains the configuration provided to the benchmark run.
	Config Config `json:"config"`
}

type Config struct {
	// EventSize is the size in bytes of the record's payload the benchmark run used.
	EventSize int `json:"event_size"`
	// Partitions is the number of partitions per topic the benchmark run used.
	Partitions int `json:"partitions"`
	// Duration is the configured production duration in seconds.
	Duration float64 `json:"duration"`
	// Timeout is the configured timeout in seconds.
	Timeout float64 `json:"timeout"`
}

type Duration struct {
	// Total is the total number of seconds the benchmark run took.
	Total float64 `json:"total"`
	// Production is the numer of seconds the benchmark spent producing records.
	Production float64 `json:"production"`
	// Consumption is the number of seconds the benchmark spent consuming records.
	Consumption float64 `json:"consumption"`
}

// Histogram represent a histogram in a ES compatible struct.
// Values and Counts can be used
type Histogram struct {
	// Values is the list of boundaries gathered.
	Values []float64 `json:"values"`
	// Counts is the list of value counts per each boundary.
	Counts []uint64 `json:"counts"`
}
