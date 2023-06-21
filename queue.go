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

// Package apmqueue provides an abstraction layer for producing and consuming
// Records from and to Kafka and GCP PubSub Lite.
package apmqueue

import (
	"context"
	"errors"
)

var (
	// ErrConsumerAlreadyRunning is returned by consumer.Run if it has already
	// been called.
	ErrConsumerAlreadyRunning = errors.New("consumer.Run: consumer already running")
)

const (
	// AtMostOnceDeliveryType acknowledges the message as soon as it's received
	// and decoded, without waiting for the message to be processed.
	AtMostOnceDeliveryType DeliveryType = iota
	// AtLeastOnceDeliveryType acknowledges the message after it has been
	// processed. It may or may not create duplicates, depending on how batches
	// are processed by the underlying Processor.
	AtLeastOnceDeliveryType

	// EventTimeKey is a header added to every record which stores the time at
	// which the event was produced, in RFC3339 format.
	EventTimeKey = "timestamp"
)

// DeliveryType for the consumer. For more details See the supported DeliveryTypes.
type DeliveryType uint8

// Consumer wraps the implementation details of the consumer implementation.
// Consumer implementations must support the defined delivery types.
type Consumer interface {
	// Run executes the consumer in a blocking manner. Returns
	// ErrConsumerAlreadyRunning when it has already been called.
	Run(ctx context.Context) error
	// Healthy returns an error if the consumer isn't healthy.
	Healthy(ctx context.Context) error
	// Close closes the consumer.
	Close() error
}

// Producer wraps the producer implementation details. Producer implementations
// must support sync and async production.
type Producer interface {
	// Produce produces N records. If the Producer is synchronous, waits until
	// all records are produced, otherwise, returns as soon as the records are
	// stored in the producer buffer, or when the records are produced to the
	// queue if sync producing is configured.
	// If the context has been enriched with metadata, each entry will be added
	// as a record's header.
	// Produce takes ownership of Record and any modifications after Produce is
	// called may cause an unhandled exception.
	Produce(ctx context.Context, rs ...Record) error
	// Healthy returns an error if the producer isn't healthy.
	Healthy(ctx context.Context) error
	// Close closes the producer.
	Close() error
}

// Record wraps a record's value with the topic where it's produced / consumed.
type Record struct {
	// Topics holds the topic where the record will be produced.
	Topic Topic
	// Value holds the record's content. It must not be mutated after Produce.
	Value []byte
}

// Processor defines record processing signature.
type Processor interface {
	// Process processes one or more records within the passed context.
	// Process takes ownership of the passed records, callers must not mutate
	// a record after Process has been called.
	Process(context.Context, ...Record) error
}

// ProcessorFunc is a function type that implements the Processor interface.
type ProcessorFunc func(context.Context, ...Record) error

// Process returns f(ctx, records...).
func (f ProcessorFunc) Process(ctx context.Context, rs ...Record) error {
	return f(ctx, rs...)
}

// Topic represents a destination topic where to produce a message/record.
type Topic string
