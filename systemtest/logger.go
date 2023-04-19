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
	"strings"
	"sync"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var loggerInstanceMu sync.Mutex
var loggerInstance *zap.SugaredLogger
var logger = func() *zap.SugaredLogger {
	loggerInstanceMu.Lock()
	defer loggerInstanceMu.Unlock()
	if loggerInstance != nil {
		return loggerInstance
	}
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	loggerInstance = l.Sugar()
	return loggerInstance
}

// NoLevelLogger returns a logger that asserts that no entries are observed for
// a particular log level. This is useful to assert that tests don't have any
// Error or Warn entries for example.
func NoLevelLogger(t testing.TB, l zapcore.Level) *zap.Logger {
	return zap.New(noLevelCore{t, l,
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
	})
}

type noLevelCore struct {
	t   testing.TB
	l   zapcore.Level
	enc zapcore.Encoder
}

func (c noLevelCore) Enabled(l zapcore.Level) bool      { return true }
func (c noLevelCore) With([]zapcore.Field) zapcore.Core { return c }
func (c noLevelCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return ce.AddCore(e, c)
}

func (c noLevelCore) Write(entry zapcore.Entry, f []zapcore.Field) error {
	if entry.Level >= c.l {
		if b, err := c.enc.EncodeEntry(entry, f); err == nil {
			c.t.Error(strings.Trim(b.String(), "\n"))
		}
	}
	return nil
}

func (noLevelCore) Sync() error { return nil }
