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
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type configFileHook struct {
	filepath string
	logger   *zap.Logger
	client   *kgo.Client

	mu                   sync.RWMutex
	lastBootstrapServers string
}

// newConfigFileHook returns a configFileHook, along with a list of seed brokers and
// possibly a sasl.Mechanism if `sasl.mechanism` is defined in the file.
func newConfigFileHook(filepath string, logger *zap.Logger) (_ *configFileHook, brokers []string, _ sasl.Mechanism, _ error) {
	config, err := loadConfigFile(filepath)
	if err != nil {
		return nil, nil, nil, err
	}
	if config.Bootstrap.Servers != "" {
		brokers = strings.Split(config.Bootstrap.Servers, ",")
	}
	var saslMechanism sasl.Mechanism
	switch config.SASL.Mechanism {
	case "PLAIN":
		var lastPlainAuthMu sync.Mutex
		var lastPlainAuth plain.Auth
		saslMechanism = plain.Plain(func(context.Context) (plain.Auth, error) {
			config, err := loadConfigFile(filepath)
			if err != nil {
				return plain.Auth{}, fmt.Errorf("failed to reload kafka config: %w", err)
			}
			lastPlainAuthMu.Lock()
			defer lastPlainAuthMu.Unlock()
			plainAuth := plain.Auth{
				User: config.SASL.Username,
				Pass: config.SASL.Password,
			}
			if plainAuth != lastPlainAuth {
				lastPlainAuth = plainAuth
				logger.Info(
					"updated SASL/PLAIN credentials from kafka config file",
					zap.String("username", plainAuth.User),
				)
			}
			return plainAuth, nil
		})
	case "AWS_MSK_IAM":
		saslMechanism, err = newAWSMSKIAMSASL()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("kafka: error configuring SASL/AWS_MSK_IAM: %w", err)
		}
	}
	h := &configFileHook{
		filepath:             filepath,
		logger:               logger,
		lastBootstrapServers: config.Bootstrap.Servers,
	}
	return h, brokers, saslMechanism, nil
}

func (h *configFileHook) OnNewClient(client *kgo.Client) {
	h.client = client
}

func (h *configFileHook) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err == nil {
		return
	}

	// Failed to connect, reload config in case the bootstrap servers have changed.
	h.logger.Debug("kafka broker connection failed, reloading kafka config")

	newConfig, err := loadConfigFile(h.filepath)
	if err != nil {
		h.logger.Warn("failed to reload kafka config", zap.Error(err))
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if newConfig.Bootstrap.Servers != h.lastBootstrapServers {
		bootstrapServers := strings.Split(newConfig.Bootstrap.Servers, ",")
		if err := h.client.UpdateSeedBrokers(bootstrapServers...); err != nil {
			h.logger.Warn(
				"error updating kafka seed brokers",
				zap.Strings("addresses", bootstrapServers),
				zap.Error(err),
			)
			return
		}
		h.logger.Info("updated kafka seed brokers", zap.Strings("addresses", bootstrapServers))
		h.lastBootstrapServers = newConfig.Bootstrap.Servers
	}
}

type configProperties struct {
	Bootstrap struct {
		Servers string `yaml:"servers"`
	} `yaml:"bootstrap"`

	SASL saslConfigProperties `yaml:"sasl"`
}

type saslConfigProperties struct {
	Mechanism string `yaml:"mechanism"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

func (s *saslConfigProperties) finalize() error {
	switch s.Mechanism {
	case "":
		if s.Username != "" {
			s.Mechanism = "PLAIN"
		}
	case "PLAIN", "AWS_MSK_IAM":
	default:
		return fmt.Errorf("kafka: unsupported SASL mechanism %q", s.Mechanism)
	}
	return nil
}

func loadConfigFile(filepath string) (configProperties, error) {
	var config configProperties
	data, err := os.ReadFile(filepath)
	if err != nil {
		return config, fmt.Errorf("error reading kafka config file: %w", err)
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return config, fmt.Errorf("error parsing kafka config file %q: %w", filepath, err)
	}
	if err := config.SASL.finalize(); err != nil {
		return configProperties{}, err
	}
	return config, nil
}
