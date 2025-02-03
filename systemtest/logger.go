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
	"go.uber.org/zap/zaptest"
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

// TestLogger creates a new zap.Logger meant to be used for tests. It only
// prints logs when the tests fails, or the logger calls t.Error
func TestLogger(t testing.TB) *zap.Logger {
	writer := newTestingWriter(t)
	t.Cleanup(func() {
		defer writer.Reset()
		if t.Failed() || testing.Verbose() {
			for _, line := range writer.buf.Lines() {
				// Strip trailing newline because t.Log always adds one.
				t.Log(strings.TrimRight(line, "\n"))
			}
		}
	})
	return zap.New(
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
			writer,
			zapcore.InfoLevel,
		),
		zap.ErrorOutput(writer.withMarkFailed(false)),
	)
}

// testingWriter is a WriteSyncer that writes to the given testing.TB.
type testingWriter struct {
	mu  sync.Mutex
	t   testing.TB
	buf *zaptest.Buffer
	// If true, the test will be marked as failed.
	markFailed bool
}

func newTestingWriter(t testing.TB) *testingWriter {
	return &testingWriter{t: t, buf: new(zaptest.Buffer)}
}

// withMarkFailed returns a copy of this testingWriter with markFailed set to
// the provided value.
func (w *testingWriter) withMarkFailed(v bool) *testingWriter {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.markFailed = v
	return w
}

func (w *testingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(p)
	if w.markFailed {
		w.t.Fail()
	}
	return len(p), nil
}

func (w *testingWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Sync()
}

func (w *testingWriter) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Reset()
}
