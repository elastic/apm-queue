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
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// Compile-time check that loggerHook implements the HookBrokerConnect interface.
var _ kgo.HookBrokerConnect = new(loggerHook)

type loggerHook struct {
	logger *zap.Logger
}

// OnBrokerConnect implements the kgo.HookBrokerConnect interface.
func (l *loggerHook) OnBrokerConnect(meta kgo.BrokerMetadata, dialDur time.Duration, _ net.Conn, err error) {
	if err != nil {
		fields := []zap.Field{
			zap.Error(err),
			zap.Duration("event.duration", dialDur),
			zap.String("host", meta.Host),
			zap.Int32("port", meta.Port),
			zap.Stack("stack"),
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			l.logger.Warn("failed to connect to broker", fields...)
			return
		}
		l.logger.Error("failed to connect to broker", fields...)
	}
}
