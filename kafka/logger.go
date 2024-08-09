package kafka

import (
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
		l.logger.Error("failed to connect to broker",
			zap.Error(err),
			zap.String("duration", dialDur.String()),
			zap.String("host", meta.Host),
			zap.Int32("port", meta.Port),
		)
	}
}
