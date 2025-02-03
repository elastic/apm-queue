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

func TestOffsetTrackerAsync(t *testing.T) {
	tracker := NewOffsetTracker()
	length := int64(2048)
	offsets := make([]int64, length)
	for i := int64(0); i < length; i++ {
		offsets[i] = i
	}
	// Register offsets and spawn goroutines to mark them done. This ensures
	// that the SafeOffset is updated correctly.
	for _, offset := range offsets {
		tracker.RegisterOffset(offset)
		go func(off int64, jitter time.Duration) {
			time.Sleep(time.Millisecond + jitter)
			tracker.MarkDone(off)
		}(offset, time.Duration(rand.Intn(200))*time.Millisecond)
	}
	assert.Eventually(t, func() bool {
		return tracker.SafeOffset() == offsets[len(offsets)-1]
	}, time.Second, time.Millisecond)
}

func TestOffsetTrackerSequentialCommits(t *testing.T) {
	for i := 0; i < 10; i++ {
		// Start at different offsets to ensure the tracker can handle any starting point.
		t.Run(fmt.Sprintf("start_%d", i), func(t *testing.T) {
			t.Run("Ascending", func(t *testing.T) {
				tracker := NewOffsetTracker()
				// Register offsets 0 through 5 sequentially
				for i := int64(0); i <= 5; i++ {
					tracker.RegisterOffset(i)
				}
				for i := int64(0); i <= 5; i++ {
					assert.Equal(t, i, tracker.MarkDone(i))
				}
			})
			t.Run("Descending", func(t *testing.T) {
				tracker := NewOffsetTracker()
				// Register offsets 5 through 0 sequentially
				for i := int64(5); i >= 0; i-- {
					tracker.RegisterOffset(i)
				}
				for i := int64(5); i >= 0; i-- {
					wantOffset := int64(0)
					if i == 0 {
						wantOffset = 5
					}
					assert.Equal(t, wantOffset, tracker.MarkDone(i), i)
				}
			})
		})
	}
}

func TestOffsetTrackerNonSequentialCommits(t *testing.T) {
	tracker := NewOffsetTracker()
	// Register offsets 10 through 14
	for i := int64(10); i <= 14; i++ {
		tracker.RegisterOffset(i)
	}
	// MarkDone on offset 11, then 12, skipping 10
	assert.Equal(t, int64(10), tracker.MarkDone(11))
	assert.Equal(t, int64(10), tracker.MarkDone(12))
	// Now mark offset 10, which sets the safe offset to 12.
	assert.Equal(t, int64(12), tracker.MarkDone(10))
	// MarkDone on offset 14, then 13, which will set the safe offset to 14.
	assert.Equal(t, int64(12), tracker.MarkDone(14))
	assert.Equal(t, int64(14), tracker.MarkDone(13))
	assert.Equal(t, int64(14), tracker.SafeOffset())

	// Register offsets 15 through 20
	for i := tracker.SafeOffset() + 1; i <= 20; i++ {
		tracker.RegisterOffset(i)
	}
	// MarkDone on offset 20, then 19, 17, 15, 16.
	assert.Equal(t, int64(14), tracker.MarkDone(20))
	assert.Equal(t, int64(14), tracker.MarkDone(19))
	assert.Equal(t, int64(14), tracker.MarkDone(17))
	assert.Equal(t, int64(14), tracker.MarkDone(16))
	// MarkDone 14, the safe offset must be 17 (since 18 is not yet done).
	assert.Equal(t, int64(17), tracker.MarkDone(15))

	// Last, mark 18 as done, which will take the safe offset to 20.
	assert.Equal(t, int64(20), tracker.MarkDone(18))
	assert.Equal(t, int64(20), tracker.SafeOffset())
}

func TestOffsetTrackerMarkDoneNonExisting(t *testing.T) {
	tracker := NewOffsetTracker()
	// Register offsets 5 through 5
	for i := int64(0); i <= 5; i++ {
		tracker.RegisterOffset(i)
	}
	assert.Equal(t, int64(0), tracker.MarkDone(10))
	assert.Equal(t, int64(0), tracker.MarkDone(50))
}
