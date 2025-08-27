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

package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestHookLogsFailedDial(t *testing.T) {
	assertLogs := func(t *testing.T,
		logs *observer.ObservedLogs,
		expectedLevel zapcore.Level,
		expectedErr string,
	) {
		observedLogs := logs.FilterMessage("failed to connect to broker").TakeAll()
		// Franz-go will retry once to connect to the broker, so we might see either one or two log lines.
		assert.GreaterOrEqual(t, len(observedLogs), 1,
			"expected one or two log lines, got %#v", observedLogs,
		)
		// The error message should contain the error message from the dialer.
		expectedErr = fmt.Sprintf("unable to dial: %s", expectedErr)
		assert.EqualValues(t, observedLogs[0].ContextMap()["error"], expectedErr)
		assert.Contains(t, observedLogs[0].ContextMap(), "event.duration")
		assert.Equal(t, observedLogs[0].Level, expectedLevel)
	}
	t.Run("context.Canceled", func(t *testing.T) {
		cluster, cfg := newFakeCluster(t)
		t.Cleanup(cluster.Close)

		core, logs := observer.New(zap.WarnLevel)
		cfg.Logger = zap.New(core)
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		cfg.Dialer = func(context.Context, string, string) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		// Calling newClient triggers the metadata refresh, forcing a connection to the fake cluster
		// using the broken dialer.
		c, err := cfg.newClientWithOpts([]Opts{
			WithTopicAttributeFunc(func(string) attribute.KeyValue { return attribute.String("k", "v") }),
		})
		require.NoError(t, err)

		<-time.After(time.Millisecond)

		// The dialer will return context.Canceled, which should be logged as a warning.
		assert.Error(t, c.Ping(ctx))

		assertLogs(t, logs, zap.WarnLevel, context.DeadlineExceeded.Error())
	})
	t.Run("busted dialer", func(t *testing.T) {
		cluster, cfg := newFakeCluster(t)
		t.Cleanup(cluster.Close)

		core, logs := observer.New(zap.ErrorLevel)
		cfg.Logger = zap.New(core)
		// Simulate returning an error when dialing the broker.
		const errorMsg = "busted"
		cfg.Dialer = func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New(errorMsg)
		}

		// Calling newClient triggers the metadata refresh, forcing a connection to the fake cluster
		// using the broken dialer.
		c, err := cfg.newClientWithOpts([]Opts{
			WithTopicAttributeFunc(func(string) attribute.KeyValue { return attribute.String("k", "v") }),
		})
		require.NoError(t, err)
		assert.Error(t, c.Ping(context.Background()))

		assertLogs(t, logs, zap.ErrorLevel, errorMsg)
	})
}
