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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"go.uber.org/zap"
)

func init() {
	// Set plaintext as the default for all tests.
	// Individual tests may clear this.
	os.Setenv("KAFKA_PLAINTEXT", "true")
}

func TestCommonConfig(t *testing.T) {
	assertValid := func(t *testing.T, expected, in CommonConfig) {
		t.Helper()
		err := in.finalize()
		require.NoError(t, err)
		in.TopicAttributeFunc = nil
		in.TopicLogFieldFunc = nil
		in.hooks = nil
		assert.Equal(t, expected, in)
	}
	assertErrors := func(t *testing.T, cfg CommonConfig, errors ...string) {
		t.Helper()
		err := cfg.finalize()
		assert.EqualError(t, err, strings.Join(errors, "\n"))
	}

	t.Run("invalid", func(t *testing.T) {
		assertErrors(t, CommonConfig{},
			"kafka: logger must be set",
			"kafka: at least one broker must be set",
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
		assertValid(t, CommonConfig{
			Brokers: []string{"broker"},
			Logger:  zap.NewNop().Named("kafka"),
		}, CommonConfig{
			Brokers: []string{"broker"},
			Logger:  zap.NewNop(),
		})
	})

	t.Run("brokers_from_environment", func(t *testing.T) {
		t.Setenv("KAFKA_BROKERS", "a,b,c")
		assertValid(t, CommonConfig{
			Brokers: []string{"a", "b", "c"},
			Logger:  zap.NewNop().Named("kafka"),
		}, CommonConfig{Logger: zap.NewNop()})
	})

	t.Run("saslplain_from_environment", func(t *testing.T) {
		// KAFKA_SASL_MECHANISM is inferred
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

	t.Run("saslaws_from_environment", func(t *testing.T) {
		t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials")) // ensure ~/.aws/credentials isn't read
		t.Setenv("KAFKA_SASL_MECHANISM", "AWS_MSK_IAM")
		cfg := CommonConfig{
			Brokers: []string{"broker"},
			Logger:  zap.NewNop(),
		}
		require.NoError(t, cfg.finalize())
		assert.NotNil(t, cfg.SASL)
		assert.Equal(t, "AWS_MSK_IAM", cfg.SASL.Name())

		t.Run("access_key_env", func(t *testing.T) {
			t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
			for _, accessKeyID := range []string{"id1", "id2"} {
				t.Setenv("AWS_ACCESS_KEY_ID", accessKeyID)
				_, message, err := cfg.SASL.Authenticate(context.Background(), "foo.us-east1.amazonaws.com:1234")
				require.NoError(t, err)
				assert.Contains(t, string(message), `"x-amz-credential":"`+accessKeyID)
			}
		})
		t.Run("credentials_file", func(t *testing.T) {
			credentialsFilePath := filepath.Join(t.TempDir(), "credentials")
			err := os.WriteFile(credentialsFilePath, []byte(`[default]
aws_access_key_id=AKIAIOSFODNN7EXAMPLE
aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
aws_session_token=IQoJb3JpZ2luX2IQoJb3JpZ2luX2IQoJb3JpZ2luX2IQoJb3JpZ2luX2IQoJb3JpZVERYLONGSTRINGEXAMPLE`), 0644)
			require.NoError(t, err)
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credentialsFilePath)
			_, message, err := cfg.SASL.Authenticate(context.Background(), "foo.us-east1.amazonaws.com:1234")
			require.NoError(t, err)
			assert.Contains(t, string(message), `"x-amz-credential":"AKIAIOSFODNN7EXAMPLE`)
		})
	})

	t.Run("tls_from_environment", func(t *testing.T) {
		// We set KAFKA_PLAINTEXT=true for all tests,
		// clear it out for this test.
		t.Setenv("KAFKA_PLAINTEXT", "")

		t.Run("plaintext", func(t *testing.T) {
			t.Setenv("KAFKA_PLAINTEXT", "true")
			assertValid(t, CommonConfig{
				Brokers: []string{"broker"},
				Logger:  zap.NewNop().Named("kafka"),
			}, CommonConfig{
				Brokers: []string{"broker"},
				Logger:  zap.NewNop(),
			})
		})

		t.Run("tls_default", func(t *testing.T) {
			assertValid(t, CommonConfig{
				Brokers: []string{"broker"},
				Logger:  zap.NewNop().Named("kafka"),
				TLS:     &tls.Config{},
			}, CommonConfig{
				Brokers: []string{"broker"},
				Logger:  zap.NewNop(),
			})
		})

		t.Run("tls_insecure", func(t *testing.T) {
			t.Setenv("KAFKA_TLS_INSECURE", "true")
			assertValid(t, CommonConfig{
				Brokers: []string{"broker"},
				Logger:  zap.NewNop().Named("kafka"),
				TLS:     &tls.Config{InsecureSkipVerify: true},
			}, CommonConfig{
				Brokers: []string{"broker"},
				Logger:  zap.NewNop(),
			})
		})
	})

	t.Run("configfile_from_env", func(t *testing.T) {
		configFilePath := writeConfigFile(t, ``)
		t.Setenv("KAFKA_CONFIG_FILE", configFilePath)
		assertValid(t, CommonConfig{
			ConfigFile: configFilePath,
			Brokers:    []string{"broker"},
			Logger:     zap.NewNop().Named("kafka"),
		}, CommonConfig{
			Brokers: []string{"broker"},
			Logger:  zap.NewNop(),
		})
	})

	t.Run("brokers_from_configfile", func(t *testing.T) {
		configFilePath := writeConfigFile(t, `
bootstrap:
  servers: from_file`)
		assertValid(t, CommonConfig{
			ConfigFile: configFilePath,
			Brokers:    []string{"from_file"},
			Logger:     zap.NewNop().Named("kafka"),
		}, CommonConfig{
			ConfigFile: configFilePath,
			Brokers:    []string{"from_env"}, // ignored, file takes precedence
			Logger:     zap.NewNop(),
		})
	})

	t.Run("sasl_from_configfile", func(t *testing.T) {
		type mockSASL struct{ sasl.Mechanism }
		configFilePath := writeConfigFile(t, `
sasl:
  username: kafka_username
  password: kafka_password`)
		cfg := CommonConfig{
			ConfigFile: configFilePath,
			Brokers:    []string{"broker"},
			Logger:     zap.NewNop().Named("kafka"),
			SASL:       &mockSASL{}, // ignored, file takes precedence
		}
		require.NoError(t, cfg.finalize())
		assert.NotNil(t, cfg.SASL)
		assert.Equal(t, "PLAIN", cfg.SASL.Name())
		_, message, err := cfg.SASL.Authenticate(context.Background(), "host")
		require.NoError(t, err)
		assert.Equal(t, []byte("\x00kafka_username\x00kafka_password"), message)

		// sasl.username and sasl.password are reloaded from the config file
		// on every invocation of cfg.SASL.Authenticate.
		err = os.WriteFile(configFilePath, []byte(`
sasl:
  username: new_kafka_username
  password: new_kafka_password`), 0644)
		require.NoError(t, err)
		_, message, err = cfg.SASL.Authenticate(context.Background(), "host")
		require.NoError(t, err)
		assert.Equal(t, []byte("\x00new_kafka_username\x00new_kafka_password"), message)
	})
}

func TestCommonConfigFileHook(t *testing.T) {
	cluster, err := kfake.NewCluster()
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	configFilePath := writeConfigFile(t, `bootstrap: {servers: testing.invalid}`)
	cfg := CommonConfig{
		ConfigFile: configFilePath,
		Logger:     zap.NewNop(),
	}
	require.NoError(t, cfg.finalize())
	assert.Equal(t, []string{"testing.invalid"}, cfg.Brokers)

	client, err := cfg.newClient(nil)
	require.NoError(t, err)
	defer client.Close()

	// Update the file, so that the seed brokers are updated when Ping is called.
	err = os.WriteFile(
		configFilePath,
		[]byte(fmt.Sprintf(`bootstrap: {servers: %q}`, strings.Join(cluster.ListenAddrs(), ","))),
		0644,
	)
	require.NoError(t, err)

	// The first Ping should fail because bootstrap.servers is initially invalid.
	err = client.Ping(context.Background())
	require.Error(t, err)

	// The hook should have been invoked, causing the config file to be reloaded
	// and bootstrap.servers to be reevaluated.
	err = client.Ping(context.Background())
	require.NoError(t, err)
}

func newClusterAddrWithTopics(t testing.TB, partitions int32, topics ...string) []string {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.SeedTopics(partitions, topics...))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	return cluster.ListenAddrs()
}

func newClusterWithTopics(t testing.TB, partitions int32, topics ...string) (*kgo.Client, []string) {
	t.Helper()
	addrs := newClusterAddrWithTopics(t, partitions, topics...)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		// Reduce the max wait time to speed up tests.
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	require.NoError(t, err)

	return client, addrs
}

func TestTopicFieldFunc(t *testing.T) {
	t.Run("nil func", func(t *testing.T) {
		topic := topicFieldFunc(nil)("a")
		assert.Equal(t, zap.Skip(), topic)
	})
	t.Run("empty field", func(t *testing.T) {
		topic := topicFieldFunc(func(topic string) zap.Field {
			return zap.Field{}
		})("b")
		assert.Equal(t, zap.Skip(), topic)
	})
	t.Run("actual topic field", func(t *testing.T) {
		topic := topicFieldFunc(func(topic string) zap.Field {
			return zap.String("topic", topic)
		})("c")
		assert.Equal(t, zap.String("topic", "c"), topic)
	})
}

// generateValidCACert creates a valid self-signed CA certificate in PEM format.
func generateValidCACert(t testing.TB) []byte {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "Test CA"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageKeyEncipherment,
		IsCA:         true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
}

func TestTLSCACertPath(t *testing.T) {
	t.Run("valid cert", func(t *testing.T) {
		t.Setenv("KAFKA_PLAINTEXT", "") // clear plaintext mode

		tempFile := filepath.Join(t.TempDir(), "ca_cert.pem")
		err := os.WriteFile(tempFile, generateValidCACert(t), 0644)
		require.NoError(t, err)

		t.Setenv("KAFKA_TLS_CA_CERT_PATH", tempFile)
		cfg := CommonConfig{Brokers: []string{"broker"}, Logger: zap.NewNop()}
		require.NoError(t, cfg.finalize())
		require.NotNil(t, cfg.TLS)
		require.NotNil(t, cfg.TLS.RootCAs)
	})
	t.Run("missing file", func(t *testing.T) {
		t.Setenv("KAFKA_PLAINTEXT", "")
		tempFile := filepath.Join(t.TempDir(), "nonexistent_cert.pem")
		t.Setenv("KAFKA_TLS_CA_CERT_PATH", tempFile)
		cfg := CommonConfig{Brokers: []string{"broker"}, Logger: zap.NewNop()}
		err := cfg.finalize()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to read CA cert")
	})
	t.Run("invalid cert", func(t *testing.T) {
		t.Setenv("KAFKA_PLAINTEXT", "")
		tempFile := filepath.Join(t.TempDir(), "invalid_cert.pem")
		err := os.WriteFile(tempFile, []byte("invalid pem data"), 0644)
		require.NoError(t, err)

		t.Setenv("KAFKA_TLS_CA_CERT_PATH", tempFile)
		cfg := CommonConfig{Brokers: []string{"broker"}, Logger: zap.NewNop()}
		err = cfg.finalize()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to append CA cert")
	})
}
