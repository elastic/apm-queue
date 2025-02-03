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
// Records from and to Kafka.
package apmqueue

import (
	"context"
	"errors"
	"sync"
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
type Producer[T any] interface {
	// Produce produces N records. If the Producer is synchronous, waits until
	// all records are produced, otherwise, returns as soon as the records are
	// stored in the producer buffer, or when the records are produced to the
	// queue if sync producing is configured.
	// If the context has been enriched with metadata, each entry will be added
	// as a record's header.
	// Produce takes ownership of Record and any modifications after Produce is
	// called may cause an unhandled exception.
	Produce(ctx context.Context, rs ...Record[T]) error
	// Healthy returns an error if the producer isn't healthy.
	Healthy(ctx context.Context) error
	// Close closes the producer.
	Close() error
}

// Record wraps a record's value with the topic where it's produced / consumed.
type Record[T any] struct {
	// OrderingKey is an optional field that is hashed to map to a partition.
	// Records with same ordering key are routed to the same partition.
	OrderingKey []byte
	// Value holds the record's content. It must not be mutated after Produce.
	Value []byte
	// Topics holds the topic where the record will be produced.
	Topic Topic
	// Partition identifies the partition ID where the record was polled from.
	// It is optional and only used for consumers.
	// When not specified, the zero value for int32 (0) identifies the only partition.
	Partition int32
	// Offset identifies the offset of the record in the partition.
	Offset int64
	// Must be set by the consumer implementation on each record that is consumed from
	// the queue. It is used to keep track of the offsets that have been processed, and
	// obtain the next offset that is safe to commit.
	Tracker *OffsetTracker[T]
}

// Processor defines record processing signature.
type Processor interface {
	// Process processes one or more records within the passed context.
	// Process takes ownership of the passed records, callers must not mutate
	// a record after Process has been called.
	Process(context.Context, Record) error
}

// ProcessorFunc is a function type that implements the Processor interface.
type ProcessorFunc func(context.Context, Record) error

// Process returns f(ctx, records...).
func (f ProcessorFunc) Process(ctx context.Context, rs Record) error {
	return f(ctx, rs)
}

// Topic represents a destination topic where to produce a message/record.
type Topic string

// TopicConsumer is used to monitor a set of consumer topics.
type TopicConsumer struct {
	// Optional topic to monitor.
	Topic Topic
	// Optional regex expression to match topics for monitoring.
	Regex string
	// Required consumer name.
	Consumer string
}

type OffsetStatus[T any] struct {
	T      T
	Offset int64

	done bool
}

// OffsetTracker can be shared by the processor to acknowledge offsets.
type OffsetTracker[T any] struct {
	mu      sync.RWMutex
	offsets map[int64]OffsetStatus[T] // offset -> done status
	// highestRegistered int64
	lowestRegistered int64
	safeToCommit     int64 // new field to store last committed offset
}

// NewOffsetTracker returns a new OffsetTracker.
func NewOffsetTracker[T any]() *OffsetTracker[T] {
	return &OffsetTracker[T]{offsets: make(map[int64]OffsetStatus[T])}
}

// SafeOffset returns the offset that is safe to commit.
func (t *OffsetTracker[T]) SafeOffset() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// Return the last committed offset instead of the next offset
	return t.safeToCommit
}

// RegisterOffset registers an offset to be tracked.
func (t *OffsetTracker[T]) RegisterOffset(off OffsetStatus[T]) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.offsets[off.Offset]; !exists {
		t.offsets[off.Offset] = OffsetStatus[T]{
			t:      off.t,
			done:   false,
			Offset: off.Offset,
		} // Register the offset as not done.
	}
	// If this is the first registered offset, initialize lowest and last commit.
	if len(t.offsets) == 1 && t.lowestRegistered <= 0 {
		t.lowestRegistered = off.Offset
		t.safeToCommit = off.Offset
	} else if off.Offset < t.lowestRegistered {
		t.lowestRegistered = off.Offset
		t.safeToCommit = off.Offset
	}
}

// MarkDone marks an offset as done and returns the new safe to commit offset.
func (t *OffsetTracker[T]) MarkDone(off OffsetStatus[T]) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.offsets[off.Offset]; !exists {
		return t.safeToCommit
	}
	t.offsets[off.Offset] = OffsetStatus[T]{}

	// Loop through the offsets in order, starting from the lowest registered
	// offset, and advance the lowestRegistered to the latest registered "done"
	// offset.
	for {
		done, exists := t.offsets[t.lowestRegistered]
		// If the offset is not done, or doesn't exist, returns the current safe
		// offset. if the offset is done, advances the lowestRegistered offset
		// and continues the loop to check the next offset.
		if !exists || !done {
			break
		}
		delete(t.offsets, t.lowestRegistered) // Remove to free up memory.
		// Update safeToCommit to the current offset candidate.
		t.safeToCommit = t.lowestRegistered
		t.lowestRegistered++ // Advance to the next offset candidate.
	}
	return t.safeToCommit
}
