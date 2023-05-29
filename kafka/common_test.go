package kafka

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCommonConfig(t *testing.T) {
	assertValid := func(t *testing.T, expected, in CommonConfig) {
		t.Helper()
		err := in.finalize()
		require.NoError(t, err)
		assert.Equal(t, expected, in)
	}
	assertErrors := func(t *testing.T, cfg CommonConfig, errors ...string) {
		t.Helper()
		err := cfg.finalize()
		assert.EqualError(t, err, strings.Join(errors, "\n"))
	}

	t.Run("invalid", func(t *testing.T) {
		assertErrors(t, CommonConfig{},
			"kafka: at least one broker must be set",
			"kafka: logger must be set",
		)
	})

	t.Run("tls_or_dialer", func(t *testing.T) {
		assertErrors(t, CommonConfig{
			Brokers: []string{"broker"},
			Logger:  zap.NewNop(),
			TLS:     &tls.Config{},
			Dialer:  func(ctx context.Context, network, address string) (net.Conn, error) { panic("unreachable") },
		}, "kafka: only one of TLS or Dialer can be set")
	})

	t.Run("valid", func(t *testing.T) {
		cfg := CommonConfig{
			Brokers: []string{"broker"},
			Logger:  zap.NewNop(),
		}
		err := cfg.finalize()
		assert.NoError(t, err)
		assert.Equal(t, CommonConfig{
			Brokers: []string{"broker"},
			Logger:  zap.NewNop(),
		}, cfg)
	})

	t.Run("brokers_from_environment", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "a,b,c")
		assertValid(t, CommonConfig{
			Brokers: []string{"a", "b", "c"},
			Logger:  zap.NewNop(),
		}, CommonConfig{Logger: zap.NewNop()})
	})

	t.Run("saslplain_from_environment", func(t *testing.T) {
		t.Setenv("KAFKA_USERNAME", "kafka_username")
		t.Setenv("KAFKA_PASSWORD", "kafka_password")
		cfg := CommonConfig{
			Brokers: []string{"broker"},
			Logger:  zap.NewNop(),
		}
		require.NoError(t, cfg.finalize())
		assert.NotNil(t, cfg.SASL)
		assert.Equal(t, "PLAIN", cfg.SASL.Name())
		_, message, err := cfg.SASL.Authenticate(context.Background(), "host")
		require.NoError(t, err)
		assert.Equal(t, []byte("\x00kafka_username\x00kafka_password"), message)
	})
}
