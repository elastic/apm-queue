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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/kafka"
)

const (
	redpandaContainerName  = "redpanda-apm-queue"
	redpandaContainerImage = "docker.redpanda.com/redpandadata/redpanda:v23.1.11"
)

var (
	kafkaBrokers     []string
	redpandaHostPort string
)

// InitKafka initialises Kafka configuration, and returns a pair of
// functions for provisioning and destroying a Kafka cluster.
//
// If KAFKA_BROKERS is set, provisioning and destroying are skipped,
// and Kafka clients will be configured to communicate with those brokers.
func InitKafka() (ProvisionInfraFunc, DestroyInfraFunc, error) {
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		logger().Infof("KAFKA_BROKERS is set (%q), skipping Kafka cluster provisioning", brokers)
		kafkaBrokers = strings.Split(brokers, ",")
		nop := func(context.Context) error { return nil }
		return nop, nil, nil
	}
	//logger().Infof("managing Redpanda in namespace %q", kafkaNamespace)
	return ProvisionKafka, DestroyKafka, nil
}

// ProvisionKafka starts a single node Redpanda broker running as a local
// Docker container, and configures Kafka clients to communicate with the
// broker by forwarding the necessary port(s).
func ProvisionKafka(ctx context.Context) error {
	if err := execCommand(ctx, "docker", "rm", "-f", redpandaContainerName); err != nil {
		return fmt.Errorf("failed to delete Redpanda container: %w", err)
	}

	if err := execCommand(ctx,
		"docker", "run", "--detach", "--name", redpandaContainerName,
		"--publish=0:9093", "--health-cmd", "rpk cluster health | grep 'Healthy:.*true'",
		redpandaContainerImage, "redpanda", "start",
		"--kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:9093",
		"--smp=1", "--memory=1G",
		"--mode=dev-container",
	); err != nil {
		return fmt.Errorf("failed to create Redpanda container: %w", err)
	}

	logger().Info("waiting for Redpanda to be ready...")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		cmd := exec.CommandContext(ctx, "docker", "inspect", redpandaContainerName)
		cmd.Stderr = os.Stderr
		output, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("'docker inspect' failed: %w", err)
		}
		var containers []struct {
			State struct {
				Health struct {
					Status string
				}
			}
			NetworkSettings struct {
				Ports map[string][]struct {
					HostIP   string
					HostPort string
				}
			}
		}
		if err := json.Unmarshal(output, &containers); err != nil {
			return fmt.Errorf("failed to decode 'docker inspect' output: %w", err)
		}
		if n := len(containers); n != 1 {
			return fmt.Errorf("expected 1 container, got %d", n)
		}
		c := containers[0]
		if c.State.Health.Status == "healthy" {
			portMapping, ok := c.NetworkSettings.Ports["9093/tcp"]
			if !ok || len(portMapping) == 0 {
				return errors.New("missing port mapping in 'docker inspect' output")
			}
			redpandaHostPort = portMapping[0].HostPort
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"context cancelled while waiting for Redpanda to become healthy: %w",
				ctx.Err(),
			)
		case <-ticker.C:
		}
	}
}

// DestroyKafka destroys the Redpanda Docker container.
func DestroyKafka(ctx context.Context) error {
	if err := execCommand(ctx, "docker", "rm", "-f", redpandaContainerName); err != nil {
		return fmt.Errorf("failed to delete Redpanda container: %w", err)
	}
	return nil
}

// CreateKafkaTopics interacts with the Kafka broker to create topics,
// deleting them when the test completes.
//
// Topics are created with 1 partition and 1 hour of retention.
func CreateKafkaTopics(ctx context.Context, t testing.TB, topics ...apmqueue.Topic) {
	manager, err := NewKafkaManager(t)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, manager.Close())
	})

	topicCreator, err := manager.NewTopicCreator(kafka.TopicCreatorConfig{
		PartitionCount: 1,
		TopicConfigs: map[string]string{
			"retention.ms": strconv.FormatInt(time.Hour.Milliseconds(), 10),
		},
	})
	require.NoError(t, err)

	err = topicCreator.CreateTopics(ctx, topics...)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = manager.DeleteTopics(context.Background(), topics...)
		require.NoError(t, err)
	})
}

func execCommand(ctx context.Context, command string, args ...string) error {
	return execCommandStdin(ctx, nil, command, args...)
}

func execCommandStdin(ctx context.Context, stdin io.Reader, command string, args ...string) error {
	var buf bytes.Buffer
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	cmd.Stdin = stdin
	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"%s command failed: %w (%s)",
			command, err, strings.TrimSpace(buf.String()),
		)
	}
	return nil
}

// NewKafkaManager returns a new kafka.Manager for the configured broker.
func NewKafkaManager(t testing.TB) (*kafka.Manager, error) {
	return kafka.NewManager(kafka.ManagerConfig{
		CommonConfig: KafkaCommonConfig(t, kafka.CommonConfig{
			Logger: defaultCfg.loggerF(t),
		}),
	})
}

// KafkaCommonConfig returns a kafka.CommonConfig suitable for connecting to
// the configured Kafka broker in tests.
//
// When Redpanda is running locally in Docker, this will ignore the advertised
// address and use the forwarded port.
func KafkaCommonConfig(t testing.TB, cfg kafka.CommonConfig) kafka.CommonConfig {
	cfg.Brokers = append([]string{}, kafkaBrokers...)
	if len(cfg.Brokers) == 0 {
		brokerAddress := fmt.Sprintf("127.0.0.1:%s", redpandaHostPort)
		netDialer := &net.Dialer{Timeout: 10 * time.Second}
		cfg.Brokers = []string{brokerAddress}
		cfg.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			// The advertised broker address is not reachable from
			// the host; replace it with the port-forwarded address.
			addr = brokerAddress
			return netDialer.DialContext(ctx, network, addr)
		}
	}
	return cfg
}
