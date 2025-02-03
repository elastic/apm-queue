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
	// Must be called by the consumer implementation after processing each record.
	//
	// It is used to mark each of the records as processed, and to help the consumer
	// implementation to keep track of the offsets that are safe to commit.
	//
	// In general, implementations should call this method after the record has been
	// processes (regardless of the outcome of the processing).
	Done func()
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

type offsetStatus[T any] struct {
	data T
	done bool
}

// OffsetTracker can be shared by the processor to acknowledge offsets. This
// implementation is thread-safe, but tracks offsets for a single partition,
// and MUST NOT be shared across multiple partitions.
type OffsetTracker[T any] struct {
	mu      sync.RWMutex
	offsets map[int64]offsetStatus[T] // offset -> done status

	lowestRegistered int64
	safeOffset       int64 // new field to store last committed offset
	safeData         T     // Stores the type information.
}

// NewOffsetTracker returns a new OffsetTracker meant to track offsets for a
// single partition.
func NewOffsetTracker[T any]() *OffsetTracker[T] {
	return &OffsetTracker[T]{
		offsets:          make(map[int64]offsetStatus[T]),
		lowestRegistered: -1,
		safeOffset:       -1, // initialize to -1 so that offset 0 is properly counted
	}
}

// SafeOffset returns the offset that is safe to commit. If if returns -1, it
// means that there are no offsets to commit.
//
// Safe offsets are defined as the lowest registered offset that is marked as
// done. This means that all offsets lower than the safe offset have been
// processed and acknowledged, and can be safely committed.
func (t *OffsetTracker[T]) SafeOffset() (T, int64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// Return the last committed offset instead of the next offset
	return t.safeData, t.safeOffset
}

// RegisterOffset registers an offset to be tracked.
func (t *OffsetTracker[T]) RegisterOffset(offset int64, data T) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.offsets[offset]; !exists {
		// Register the offset as not done.
		t.offsets[offset] = offsetStatus[T]{data: data}
	}
	// If this is the first registered offset (lowestRegistered == -1), update
	// lowestRegistered and safeToCommit.
	if t.lowestRegistered < 0 {
		t.lowestRegistered = offset
	} else if offset < t.lowestRegistered {
		t.lowestRegistered = offset
	}
}

// MarkDone marks an offset as done and returns the new safe to commit offset.
// If it returns -1, there are no safe offsets to commit.
func (t *OffsetTracker[T]) MarkDone(offset int64) (T, int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	status, exists := t.offsets[offset]
	if !exists {
		return t.safeData, t.safeOffset
	}
	status.done = true
	t.offsets[offset] = status

	// Advance lowestRegistered as long as the next offset is registered and done.
	for {
		s, exists := t.offsets[t.lowestRegistered]
		if !exists || !s.done {
			break
		}
		// Update safe offset and associated data.
		t.safeOffset = t.lowestRegistered
		t.safeData = s.data
		delete(t.offsets, t.lowestRegistered)
		t.lowestRegistered++
	}
	return status.data, t.safeOffset
}

// WindowOffsetTracker tracks offsets for a single partition using a sliding window.
// It is optimized for high-throughput and contiguous offsets.
type WindowOffsetTracker[T any] struct {
	mu sync.Mutex
	// safeOffset is the last offset that has been fully acknowledged (committed).
	safeOffset int64
	// safeData holds the data associated with safeOffset.
	safeData T

	// baseOffset is the starting offset corresponding to window[0].
	// Offsets lower than baseOffset are already committed.
	baseOffset int64
	// window holds the status for offsets starting from baseOffset.
	// For example, window[i] corresponds to offset = baseOffset + int64(i).
	window []offsetStatus[T]
}

// NewWindowOffsetTracker returns a new OffsetTracker instance.
// Initially, no offsets are registered so baseOffset and safeOffset are set to -1.
func NewWindowOffsetTracker[T any]() *WindowOffsetTracker[T] {
	return &WindowOffsetTracker[T]{
		safeOffset: -1,
		baseOffset: -1,
	}
}

// SafeOffset returns the safe-to-commit offset along with its associated data.
// A safe offset of -1 indicates that no offsets are available to commit.
func (t *WindowOffsetTracker[T]) SafeOffset() (T, int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.safeData, t.safeOffset
}

// RegisterOffset registers an offset (with its associated data) to be tracked.
// For the first registration, the sliding window is initialized with baseOffset = offset.
// For registrations earlier than the current baseOffset, the window is expanded backwards.
// For offsets later than the current window, it is extended forward.
// Offsets that have already been registered are overwritten.
func (t *WindowOffsetTracker[T]) RegisterOffset(offset int64, data T) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.baseOffset < 0 {
		// First registration initializes the tracker.
		t.baseOffset = offset
		t.window = []offsetStatus[T]{{data: data, done: false}}
		return
	}

	if offset < t.baseOffset {
		// Expand the window backwards: prepend slots.
		diff := t.baseOffset - offset
		newWindow := make([]offsetStatus[T], diff)
		// Prepend default (undone) slots.
		t.window = append(newWindow, t.window...)
		t.baseOffset = offset
		// Register at the new index 0.
		t.window[0] = offsetStatus[T]{data: data, done: false}
		return
	}

	index := offset - t.baseOffset
	if index >= int64(len(t.window)) {
		needed := int(index) - len(t.window) + 1
		newSlots := make([]offsetStatus[T], needed)
		t.window = append(t.window, newSlots...)
	}
	// Overwrite existing entry (or an empty slot) with the data.
	t.window[index] = offsetStatus[T]{data: data, done: false}
}

// MarkDone marks a registered offset as done and tries to advance the safe offset.
// When called for an offset that is not registered, or out of range,
// it returns the current safe values unchanged.
// If the marked offset is the current baseOffset, it advances the sliding window
// by removing consecutive done entries, updating safeOffset and safeData.
// MarkDone returns the data associated with the marked offset along with the safe offset.
func (t *WindowOffsetTracker[T]) MarkDone(offset int64) (T, int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// If offset is outside the current tracked window, ignore.
	if offset < t.baseOffset || offset >= t.baseOffset+int64(len(t.window)) {
		return t.safeData, t.safeOffset
	}

	idx := offset - t.baseOffset
	// Mark the offset as done.
	t.window[idx].done = true
	// Capture the data for the offset being marked.
	currentData := t.window[idx].data

	// If the marked offset is at the beginning of the window,
	// try to advance the contiguous done region.
	if offset == t.baseOffset {
		for len(t.window) > 0 && t.window[0].done {
			t.safeOffset = t.baseOffset
			t.safeData = t.window[0].data
			t.window = t.window[1:]
			t.baseOffset++
		}
	}
	return currentData, t.safeOffset
}
