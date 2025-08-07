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
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/sasl/plain"
	"go.uber.org/zap"
)

// Helper to read and parse Kafka env vars.
type envConfig struct {
	logger     *zap.Logger
	configFile string
	brokers    []string
	plainText  bool
	tls        *tlsConfig
	sasl       SASLMechanism
}

type tlsConfig struct {
	Config  *tls.Config // Not embedded, since we want to copy later.
	KeyPair keyPair
	Dialer  func(ctx context.Context, network, address string) (net.Conn, error)
}

func (t *tlsConfig) isMutual() bool {
	return t.KeyPair.certPath != "" && t.KeyPair.keyPath != ""
}

func (e *envConfig) hasCert() bool {
	return os.Getenv("KAFKA_TLS_CA_CERT_PATH") != "" ||
		os.Getenv("KAFKA_TLS_CERT_PATH") != "" ||
		os.Getenv("KAFKA_TLS_KEY_PATH") != ""
}

func loadEnvConfig(logger *zap.Logger, configFile string) (*envConfig, error) {
	cfg := &envConfig{
		logger:     logger,
		configFile: configFile,
		tls:        &tlsConfig{},
	}

	// Only set config file via env vars, if not set explicitly.
	if cfg.configFile == "" {
		cfg.configFile = os.Getenv("KAFKA_CONFIG_FILE")
	}

	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.brokers = strings.Split(v, ",")
	}

	// Check for secure connection (mTLS).
	if os.Getenv("KAFKA_PLAINTEXT") == "true" {
		cfg.plainText = true
	} else if cfg.tls.Config == nil && cfg.tls.Dialer == nil {
		tlsConfig, err := cfg.loadTLSConfig()
		if err != nil {
			return cfg, err
		}
		cfg.tls = tlsConfig
	}

	// Only configure SASL when there is no intention to configure mTLS.
	if !cfg.tls.isMutual() {
		saslMech, err := cfg.loadSASLConfig()
		if err != nil {
			return cfg, err
		}
		cfg.sasl = saslMech
	}

	return cfg, nil
}

func (e *envConfig) loadTLSConfig() (*tlsConfig, error) {
	cfg := &tlsConfig{
		KeyPair: keyPair{
			caPath:   os.Getenv("KAFKA_TLS_CA_CERT_PATH"),
			certPath: os.Getenv("KAFKA_TLS_CERT_PATH"),
			keyPath:  os.Getenv("KAFKA_TLS_KEY_PATH"),
		},
		Config: &tls.Config{},
	}

	// Override server name if env var is set.
	if name, exists := os.LookupEnv("KAFKA_TLS_SERVER_NAME"); exists {
		e.logger.Debug("overriding TLS server name", zap.String("server_name", name))
		cfg.Config.ServerName = name
	}

	if os.Getenv("KAFKA_TLS_INSECURE") == "true" {
		if e.hasCert() {
			return cfg, fmt.Errorf(
				"kafka: cannot set KAFKA_TLS_INSECURE when either of " +
					"KAFKA_TLS_CA_CERT_PATH, KAFKA_TLS_CERT_PATH, or KAFKA_TLS_KEY_PATH are set",
			)
		}
		cfg.Config.InsecureSkipVerify = true
	}

	// Set a dialer that reloads the CA cert when the file changes.
	if e.hasCert() {
		dialFn, err := newCertReloadingDialer(
			cfg.KeyPair.caPath,
			cfg.KeyPair.certPath,
			cfg.KeyPair.keyPath,
			30*time.Second,
			cfg.Config,
		)
		if err != nil {
			return cfg, fmt.Errorf("kafka: error creating dialer with CA cert: %w", err)
		}
		cfg.Dialer = dialFn
		cfg.Config = nil
	}

	return cfg, nil
}

func (e *envConfig) loadSASLConfig() (SASLMechanism, error) {
	saslConfig := saslConfigProperties{
		Mechanism: os.Getenv("KAFKA_SASL_MECHANISM"),
		Username:  os.Getenv("KAFKA_USERNAME"),
		Password:  os.Getenv("KAFKA_PASSWORD"),
	}

	if err := saslConfig.finalize(); err != nil {
		return nil, fmt.Errorf("kafka: error configuring SASL: %w", err)
	}

	var saslMech SASLMechanism
	switch saslConfig.Mechanism {
	case "PLAIN":
		plainAuth := plain.Auth{
			User: saslConfig.Username,
			Pass: saslConfig.Password,
		}
		if plainAuth != (plain.Auth{}) {
			saslMech = plainAuth.AsMechanism()
		}
	case "AWS_MSK_IAM":
		var err error
		saslMech, err = newAWSMSKIAMSASL()
		if err != nil {
			return nil, fmt.Errorf("kafka: error configuring SASL/AWS_MSK_IAM: %w", err)
		}
	}

	return saslMech, nil
}
