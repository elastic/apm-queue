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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type info struct {
	a int64
}

func TestOffsetTrackerAsync(t *testing.T) {
	tracker := NewOffsetTracker[info]()
	defer func() {
		if t.Failed() {
			t.Logf("Tracker: %+v", tracker)
		}
	}()

	length := int64(2048)
	offsets := make([]int64, length)
	for i := int64(0); i < length; i++ {
		offsets[i] = i
	}
	// Register offsets and spawn goroutines to mark them done. This ensures
	// that the SafeOffset is updated correctly.
	for _, offset := range offsets {
		tracker.RegisterOffset(offset, info{a: offset})
		go func(off int64, jitter time.Duration) {
			time.Sleep(time.Millisecond + jitter)
			tracker.MarkDone(off)
		}(offset, time.Duration(rand.Intn(200))*time.Millisecond)
	}
	assert.Eventually(t, func() bool {
		_, offset := tracker.SafeOffset()
		return offset == offsets[len(offsets)-1]
	}, time.Second, time.Millisecond)
}

func TestOffsetTrackerSequentialCommits(t *testing.T) {
	for i := 0; i < 10; i++ {
		// Start at different offsets to ensure the tracker can handle any starting point.
		t.Run(fmt.Sprintf("start_%d", i), func(t *testing.T) {
			t.Run("Ascending", func(t *testing.T) {
				tracker := NewOffsetTracker[info]()
				// Register offsets 0 through 5 sequentially
				for i := int64(0); i <= 5; i++ {
					tracker.RegisterOffset(i, info{a: i})
				}
				for i := int64(0); i <= 5; i++ {
					_, safe := tracker.SafeOffset()
					// assert.Equal()
					inf, offset := tracker.MarkDone(i)
					t.Log(safe, i, offset)
					assert.Equal(t, i, offset)
					assert.Equal(t, info{a: i}, inf)
				}
			})
			t.Run("Descending", func(t *testing.T) {
				tracker := NewOffsetTracker[info]()
				// Register offsets 5 through 0 sequentially
				for i := int64(5); i >= 0; i-- {
					tracker.RegisterOffset(i, info{a: i})
				}
				for i := int64(5); i >= 0; i-- {
					wantOffset := int64(-1)
					if i == 0 {
						wantOffset = 5
					}
					inf, offset := tracker.MarkDone(i)
					assert.Equal(t, wantOffset, offset, i)
					assert.Equal(t, info{a: i}, inf)
				}
			})
		})
	}
}

func TestOffsetTrackerNonSequentialCommits(t *testing.T) {
	tracker := NewOffsetTracker[info]()
	// Register offsets 10 through 14
	for i := int64(10); i <= 14; i++ {
		tracker.RegisterOffset(i, info{a: i})
	}
	// MarkDone on offset 11, then 12, skipping 10
	_, offset := tracker.MarkDone(11)
	assert.Equal(t, int64(-1), offset)
	_, offset = tracker.MarkDone(12)
	assert.Equal(t, int64(-1), offset)
	// Now mark offset 10, which sets the safe offset to 12.
	_, offset = tracker.MarkDone(10)
	assert.Equal(t, int64(12), offset)
	// MarkDone on offset 14, then 13, which will set the safe offset to 14.
	_, offset = tracker.MarkDone(14)
	assert.Equal(t, int64(12), offset)
	_, offset = tracker.MarkDone(13)
	assert.Equal(t, int64(14), offset)
	_, offset = tracker.SafeOffset()
	assert.Equal(t, int64(14), offset)

	// Register offsets 15 through 20
	for i := offset + 1; i <= 20; i++ {
		tracker.RegisterOffset(i, info{a: i})
	}
	// MarkDone on offset 20, then 19, 17, 15, 16.
	_, offset = tracker.MarkDone(20)
	assert.Equal(t, int64(14), offset)
	_, offset = tracker.MarkDone(19)
	assert.Equal(t, int64(14), offset)
	_, offset = tracker.MarkDone(17)
	assert.Equal(t, int64(14), offset)
	_, offset = tracker.MarkDone(16)
	assert.Equal(t, int64(14), offset)
	// MarkDone 14, the safe offset must be 17 (since 18 is not yet done).
	_, offset = tracker.MarkDone(15)
	assert.Equal(t, int64(17), offset)

	// Last, mark 18 as done, which will take the safe offset to 20.
	_, offset = tracker.MarkDone(18)
	assert.Equal(t, int64(20), offset)
	_, offset = tracker.SafeOffset()
	assert.Equal(t, int64(20), offset)
}

func TestOffsetTrackerMarkDoneNonExisting(t *testing.T) {
	tracker := NewOffsetTracker[info]()
	// Register offsets 5 through 5
	for i := int64(0); i <= 5; i++ {
		tracker.RegisterOffset(i, info{a: i})
	}
	inf, offset := tracker.MarkDone(10)
	assert.Equal(t, int64(-1), offset)
	assert.Equal(t, info{a: 0}, inf)
	inf, offset = tracker.MarkDone(50)
	assert.Equal(t, int64(-1), offset)
	assert.Equal(t, info{a: 0}, inf)
}
