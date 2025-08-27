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
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
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
	t.Run("invalid KAFKA_TLS_INSECURE and KAFKA_TLS_CA_CERT_PATH", func(t *testing.T) {
		t.Setenv("KAFKA_TLS_INSECURE", "true")
		t.Setenv("KAFKA_TLS_CA_CERT_PATH", "ca_cert.pem")
		t.Setenv("KAFKA_PLAINTEXT", "")
		assertErrors(t, CommonConfig{
			Brokers: []string{"broker"},
			Logger:  zap.NewNop(),
		},
			"kafka: cannot set KAFKA_TLS_INSECURE when either of KAFKA_TLS_CA_CERT_PATH, KAFKA_TLS_CERT_PATH, or KAFKA_TLS_KEY_PATH are set",
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

		t.Run("tls_override_server_name", func(t *testing.T) {
			t.Setenv("KAFKA_TLS_SERVER_NAME", "overriden.server.name")
			assertValid(t, CommonConfig{
				Brokers: []string{"broker"},
				Logger:  zap.NewNop().Named("kafka"),
				TLS:     &tls.Config{ServerName: "overriden.server.name"},
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
	t.Run("configfile_with_sasl.mechanism.empty", func(t *testing.T) {
		configFilePath := writeConfigFile(t, `
bootstrap:
  servers: from_file
sasl:
  mechanism: `)

		cfg := CommonConfig{
			ConfigFile: configFilePath,
			Logger:     zap.NewNop(),
		}
		require.NoError(t, cfg.finalize())
		assert.Empty(t, cfg.SASL)
	})
	t.Run("configfile_with_sasl.mechanism but with mTLS env vars set", func(t *testing.T) {
		t.Setenv("KAFKA_PLAINTEXT", "false")
		// Generate a valid CA and client certificate
		caKey, caCert, caCertPEM := generateCA(t)
		clientKey, _, clientCertPEM := generateClientCert(t, caCert, caKey)
		clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})

		tempDir := t.TempDir()
		caFile := filepath.Join(tempDir, "ca.pem")
		certFile := filepath.Join(tempDir, "client.pem")
		keyFile := filepath.Join(tempDir, "key.pem")

		// Write initial valid files
		require.NoError(t, os.WriteFile(caFile, caCertPEM, 0644))
		require.NoError(t, os.WriteFile(certFile, clientCertPEM, 0644))
		require.NoError(t, os.WriteFile(keyFile, clientKeyPEM, 0644))

		t.Setenv("KAFKA_TLS_CA_CERT_PATH", caFile)
		t.Setenv("KAFKA_TLS_CERT_PATH", certFile)
		t.Setenv("KAFKA_TLS_KEY_PATH", keyFile)
		configFilePath := writeConfigFile(t, `
bootstrap:
  servers: abc
sasl:
  mechanism: `)

		cfg := CommonConfig{
			ConfigFile: configFilePath,
			Logger:     zap.NewNop(),
		}
		require.NoError(t, cfg.finalize())
		// Ensure SASL is not set because we are using mTLS.
		assert.Empty(t, cfg.SASL)
		assert.NotNil(t, cfg.Dialer)
		assert.Equal(t, "abc", cfg.Brokers[0])
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

	client, err := cfg.newClientWithOpts(nil)
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

	template := newCATemplate()
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
}

func TestTLSCACertPath(t *testing.T) {
	t.Run("valid cert", func(t *testing.T) {
		t.Setenv("KAFKA_PLAINTEXT", "") // clear plaintext mode

		tempFile := filepath.Join(t.TempDir(), "ca_cert.pem")
		validCert := generateValidCACert(t)
		err := os.WriteFile(tempFile, validCert, 0644)
		require.NoError(t, err)

		t.Setenv("KAFKA_TLS_CA_CERT_PATH", tempFile)
		cfg := CommonConfig{Brokers: []string{"broker"}, Logger: zap.NewNop()}
		require.NoError(t, cfg.finalize())
		require.NotNil(t, cfg.Dialer)
		require.Nil(t, cfg.TLS)
	})
	t.Run("missing file", func(t *testing.T) {
		t.Setenv("KAFKA_PLAINTEXT", "")
		tempFile := filepath.Join(t.TempDir(), "nonexistent_cert.pem")
		t.Setenv("KAFKA_TLS_CA_CERT_PATH", tempFile)
		cfg := CommonConfig{Brokers: []string{"broker"}, Logger: zap.NewNop()}
		err := cfg.finalize()
		require.Error(t, err)
		require.Contains(t, err.Error(), "kafka: error creating dialer with CA cert")
		require.Contains(t, err.Error(), "no such file or directory")
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
		require.Contains(t, err.Error(), "kafka: error creating dialer with CA cert")
	})
}

func TestTLSCAHotReload(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	// Get the certificate from the test server and encode it in PEM format.
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: srv.TLS.Certificates[0].Certificate[0],
	})
	tempFile := filepath.Join(t.TempDir(), "cert.pem")
	require.NoError(t, os.WriteFile(tempFile, certPEM, 0644))

	dialFunc, err := newCertReloadingDialer(tempFile, "", "", time.Millisecond, &tls.Config{})
	require.NoError(t, err)

	var wg sync.WaitGroup
	addr := srv.Listener.Addr()
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < runtime.GOMAXPROCS(0)*4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Millisecond):
					conn, err := dialFunc(ctx, addr.Network(), addr.String())
					if err != nil && !errors.Is(err, io.EOF) {
						select {
						case <-ctx.Done():
							return
						default:
						}
						// Ensure no TLS errors occur.
						require.NoError(t, err)
					}
					if conn != nil {
						conn.Close()
					}
				}
			}
		}()
	}

	<-time.After(200 * time.Millisecond)

	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		// Update the file, so that the CA cert is reloaded when dialer is called again.
		require.NoError(t, os.WriteFile(tempFile, certPEM, 0644))
		<-time.After(50 * time.Millisecond)
	}

	cancel()  // allow go routines to exit
	wg.Wait() // wait for all go routines to finish
}

func TestTLSClientCertHotReload(t *testing.T) {
	// Generate a proper CA certificate
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	caTemplate := newCATemplate()
	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caCertDER)
	require.NoError(t, err)
	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Generate a server certificate signed by the CA
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	serverTemplate := newLocalMTLSServerTemplate()
	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, caCert, &serverKey.PublicKey, caKey)
	require.NoError(t, err)
	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})

	// Generate a client certificate signed by the CA
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	clientTemplate := newClientCert("Initial Cert")
	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, caCert, &clientKey.PublicKey, caKey)
	require.NoError(t, err)
	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})

	// Write PEM files for hot reload testing
	certFile := filepath.Join(t.TempDir(), "client-cert.pem")
	keyFile := filepath.Join(t.TempDir(), "client-key.pem")
	caFile := filepath.Join(t.TempDir(), "ca-cert.pem")

	require.NoError(t, os.WriteFile(certFile, clientCertPEM, 0644))
	require.NoError(t, os.WriteFile(keyFile, clientKeyPEM, 0644))
	require.NoError(t, os.WriteFile(caFile, caCertPEM, 0644))

	// Create test server with proper TLS configuration
	serverCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	require.NoError(t, err)

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caCertPEM)

	// Setup the server that requires client certificates
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Configure server to require client certificates
	var clientCertMap = map[string]struct{}{}
	var mu sync.Mutex
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		// Verify different client certificates are used.
		VerifyConnection: func(cs tls.ConnectionState) error {
			mu.Lock()
			for _, cert := range cs.PeerCertificates {
				clientCertMap[cert.Subject.CommonName] = struct{}{}
			}
			mu.Unlock()
			return nil
		},
	}

	srv.StartTLS()
	t.Cleanup(srv.Close)

	// Create dialer with client certificate hot reloading
	dialFunc, err := newCertReloadingDialer(caFile, certFile, keyFile, time.Millisecond*50, &tls.Config{})
	require.NoError(t, err)

	// Test the dialer with multiple concurrent connections
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	addr := srv.Listener.Addr()
	for i := 0; i < runtime.GOMAXPROCS(0)*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Millisecond):
					conn, err := dialFunc(ctx, addr.Network(), addr.String())
					if err != nil && !errors.Is(err, io.EOF) {
						select {
						case <-ctx.Done():
							return
						default:
						}
						// Ensure no TLS errors occur.
						require.NoError(t, err)
					}
					if conn != nil {
						conn.Close()
					}
				}
			}
		}()
	}

	// Allow some time for connections to be made
	<-time.After(200 * time.Millisecond)

	// Update client certificate multiple times to test hot reloading
	for i := 0; i < 3; i++ {
		// Create new client cert with same key
		newClientTemplate := newClientCert(fmt.Sprintf("Test Client %d", i+1))

		newClientCertDER, err := x509.CreateCertificate(rand.Reader, &newClientTemplate, caCert, &clientKey.PublicKey, caKey)
		require.NoError(t, err)
		newClientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: newClientCertDER})

		// Update cert file
		require.NoError(t, os.WriteFile(certFile, newClientCertPEM, 0644))
		<-time.After(200 * time.Millisecond)
	}

	cancel()  // allow goroutines to exit
	wg.Wait() // wait for all goroutines to finish

	mu.Lock()
	assert.Equal(t, 4, len(clientCertMap))
	assert.Equal(t, clientCertMap, map[string]struct{}{
		clientTemplate.Subject.CommonName: {},
		"Test Client 1":                   {},
		"Test Client 2":                   {},
		"Test Client 3":                   {},
	})
	mu.Unlock()
}

func TestCertificateExpired(t *testing.T) {
	// Generate a CA cert
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTemplate := newCATemplate()
	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caCertDER)
	require.NoError(t, err)

	// Create an expired client certificate
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	clientTemplate := newClientCert("Client")
	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, caCert, &clientKey.PublicKey, caKey)
	require.NoError(t, err)

	// Create PEM files
	tempDir := t.TempDir()
	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})

	caFile := filepath.Join(tempDir, "ca.pem")
	certFile := filepath.Join(tempDir, "client.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	require.NoError(t, os.WriteFile(caFile, caCertPEM, 0644))
	require.NoError(t, os.WriteFile(certFile, clientCertPEM, 0644))
	require.NoError(t, os.WriteFile(keyFile, clientKeyPEM, 0644))

	// This should succeed since we're just loading files
	dialFunc, err := newCertReloadingDialer(caFile, certFile, keyFile, time.Millisecond*10, &tls.Config{})
	require.NoError(t, err)

	// But when we use it to connect to a server, it should fail due to expired cert
	// Create a simple test server
	srvTpl := newLocalMTLSServerTemplate()
	srvTpl.NotAfter = time.Now().Add(-time.Minute)
	serverCert, serverKey, err := generateServerCert(caCert, caKey, srvTpl)
	require.NoError(t, err)
	srv := createTLSServer(t, caCertPEM, serverCert, serverKey, tls.RequireAndVerifyClientCert)
	defer srv.Close()

	// Try to connect - should fail with expired certificate
	ctx := context.Background()
	addr := srv.Listener.Addr()
	_, err = dialFunc(ctx, addr.Network(), addr.String())
	require.Error(t, err)
	require.Contains(t, err.Error(), "certificate has expired")
}

// Test that missing or corrupted files are handled properly during hot reload
func TestCertificateHotReloadErrors(t *testing.T) {
	// Generate a valid CA and client certificate
	caKey, caCert, caCertPEM := generateCA(t)
	clientKey, _, clientCertPEM := generateClientCert(t, caCert, caKey)
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})

	tempDir := t.TempDir()
	caFile := filepath.Join(tempDir, "ca.pem")
	certFile := filepath.Join(tempDir, "client.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	// Write initial valid files
	require.NoError(t, os.WriteFile(caFile, caCertPEM, 0644))
	require.NoError(t, os.WriteFile(certFile, clientCertPEM, 0644))
	require.NoError(t, os.WriteFile(keyFile, clientKeyPEM, 0644))

	// Create test server that requires client auth
	serverCert, serverKey, err := generateServerCert(caCert, caKey, newLocalMTLSServerTemplate())
	require.NoError(t, err)
	srv := createTLSServer(t, caCertPEM, serverCert, serverKey, tls.RequireAndVerifyClientCert)
	defer srv.Close()

	// Create dialer with client certificate hot reloading
	dialFunc, err := newCertReloadingDialer(caFile, certFile, keyFile, time.Millisecond*10, &tls.Config{})
	require.NoError(t, err)

	// First connection should succeed
	ctx := context.Background()
	addr := srv.Listener.Addr()
	conn, err := dialFunc(ctx, addr.Network(), addr.String())
	require.NoError(t, err)
	conn.Close()

	t.Run("corrupt_cert_file", func(t *testing.T) {
		// Corrupt the certificate file
		require.NoError(t, os.WriteFile(certFile, []byte("invalid cert data"), 0644))

		// Give time for the hot reload to detect the change
		time.Sleep(100 * time.Millisecond)

		// Next connection should still work using cached cert
		conn, err := dialFunc(ctx, addr.Network(), addr.String())
		require.Error(t, err, "failed to load new certificate: tls: failed to find any PEM data in certificate input")
		if conn != nil {
			conn.Close()
		}

		// Restore valid certificate
		require.NoError(t, os.WriteFile(certFile, clientCertPEM, 0644))
	})

	t.Run("key_cert_mismatch", func(t *testing.T) {
		// Generate a different key pair
		_, _, newClientCertPEM := generateClientCert(t, caCert, caKey)

		// Update cert but not key - creating a mismatch
		require.NoError(t, os.WriteFile(certFile, newClientCertPEM, 0644))

		// Give time for the hot reload to detect the change
		time.Sleep(100 * time.Millisecond)

		// Connection should fail due to key mismatch
		_, err := dialFunc(ctx, addr.Network(), addr.String())
		require.Error(t, err)

		// Restore matching key and certificate
		require.NoError(t, os.WriteFile(certFile, clientCertPEM, 0644))
	})
}

// Helper functions for certificate generation
func generateCA(t testing.TB) (*rsa.PrivateKey, *x509.Certificate, []byte) {
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTemplate := newCATemplate()
	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caCertDER)
	require.NoError(t, err)

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	return caKey, caCert, caCertPEM
}

func generateClientCert(t testing.TB, caCert *x509.Certificate, caKey *rsa.PrivateKey) (*rsa.PrivateKey, *x509.Certificate, []byte) {
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	clientTemplate := newClientCert("Test Client")
	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, caCert, &clientKey.PublicKey, caKey)
	require.NoError(t, err)
	clientCert, err := x509.ParseCertificate(clientCertDER)
	require.NoError(t, err)

	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})
	return clientKey, clientCert, clientCertPEM
}

func generateServerCert(caCert *x509.Certificate, caKey *rsa.PrivateKey, serverTemplate x509.Certificate) ([]byte, []byte, error) {
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})

	return serverCertPEM, serverKeyPEM, nil
}

func createTLSServer(t testing.TB,
	caCertPEM, serverCertPEM, serverKeyPEM []byte,
	clientAuth tls.ClientAuthType,
) *httptest.Server {
	serverCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	require.NoError(t, err)

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caCertPEM)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    certPool,
		ClientAuth:   clientAuth,
	}

	srv.StartTLS()
	return srv
}

func newCATemplate() x509.Certificate {
	return x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
}

func newLocalMTLSServerTemplate() x509.Certificate {
	return x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
}

func newClientCert(name string) x509.Certificate {
	return x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
}
