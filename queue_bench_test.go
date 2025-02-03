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

package apmqueue

import (
	"fmt"
	"testing"
)

// func BenchmarkOffsetTrackerRegister(b *testing.B) {
// 	tracker := NewOffsetTracker[info]()

// 	length := int64(20480)
// 	offsets := make([]int64, length)
// 	for i := int64(0); i < length; i++ {
// 		offsets[i] = i
// 	}
// 	b.ResetTimer()

// 	b.RunParallel(func(pb *testing.PB) {
// 		for pb.Next() {
// 			for _, offset := range offsets {
// 				tracker.RegisterOffset(offset, info{a: offset})
// 			}
// 		}
// 	})
// }

type Tracker interface {
	RegisterOffset(offset int64, info info)
	MarkDone(offset int64) (info, int64)
	SafeOffset() (info, int64)
}

func BenchmarkTrackerRegister(b *testing.B) {
	trackers := []Tracker{NewOffsetTracker[info](), NewWindowOffsetTracker[info]()}
	b.ResetTimer()
	for _, tracker := range trackers {
		b.Run(fmt.Sprintf("%T", tracker), func(b *testing.B) {
			b.Run("Ascending", func(b *testing.B) {
				b.RunParallel(func(pb *testing.PB) {
					offsetC := make(chan int64, 1024)
					done := make(chan struct{})
					go func() {
						defer close(done)
						for offset := range offsetC {
							tracker.MarkDone(offset)
						}
					}()
					var offset int64
					for pb.Next() {
						tracker.RegisterOffset(offset, info{a: offset})
						offsetC <- offset
						offset++
					}
					close(offsetC)
					<-done
				})
			})
			b.Run("Descending", func(b *testing.B) {
				b.RunParallel(func(pb *testing.PB) {
					offsetC := make(chan int64, 1024)
					done := make(chan struct{})
					go func() {
						defer close(done)
						for offset := range offsetC {
							tracker.MarkDone(offset)
						}
					}()
					var offset int64 = 1_000_000
					for pb.Next() {
						if offset == -1 {
							offset = 1_000_000
						}
						tracker.RegisterOffset(offset, info{a: offset})
						offsetC <- offset
						offset--
					}
					close(offsetC)
					<-done
				})
			})
		})
	}
}
