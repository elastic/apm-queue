package kafka

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestHookLogsFailedDial(t *testing.T) {
	cluster, cfg := newFakeCluster(t)
	t.Cleanup(cluster.Close)

	core, logs := observer.New(zap.ErrorLevel)
	cfg.Logger = zap.New(core)
	//cfg.hooks = []kgo.Hook{&loggerHook{logger: cfg.Logger}}
	// Simulate returning an error when dialing the broker.
	const errorMsg = "busted"
	cfg.Dialer = func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New(errorMsg)
	}

	// Calling newClient triggers the metadata refresh, forcing a connection to the fake cluster
	// using the broken dialer.
	c, err := cfg.newClient(func(string) attribute.KeyValue { return attribute.String("k", "v") })
	require.NoError(t, err)
	assert.Error(t, c.Ping(context.Background()))

	observedLogs := logs.FilterMessage("failed to connect to broker").TakeAll()
	assert.Equal(t, 1, len(observedLogs))

	// The error message should contain the error message from the dialer.
	assert.EqualValues(t, observedLogs[0].ContextMap()["error"], errorMsg)
	assert.Contains(t, observedLogs[0].ContextMap(), "duration")
}
