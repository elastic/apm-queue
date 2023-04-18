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

package systemtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

var logger *zap.SugaredLogger

func init() {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	logger = l.Sugar()
}

// NoLevelLogger returns a logger that asserts that no entries are observed for
// a particular log level. This is useful to assert that tests don't have any
// Error or Warn entries for example.
func NoLevelLogger(t testing.TB, l zapcore.Level) *zap.Logger {
	core, logs := observer.New(l)
	t.Cleanup(func() {
		entries := logs.TakeAll()
		if !assert.Len(t, entries, 0) {
			if len(entries) > 10 {
				entries = entries[len(entries)-10:]
			}
			for _, entry := range entries {
				t.Error(entry.Message)
			}
		}
	})
	return zap.New(core)
}
