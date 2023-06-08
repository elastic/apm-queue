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
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/apm-queue/kafka"
	"github.com/elastic/apm-queue/systemtest/portforwarder"

	apmqueue "github.com/elastic/apm-queue"
)

var (
	kafkaBrokers     []string
	kafkaNamespace   = "kafka"
	kafkaClusterName = "kafka"
)

// ProvisionKafka provisions a Kafka cluster in the current Kubernetes
// context, and configures Kafka clients to communicate with the broker
// by forwarding the necessary port(s).
//
// If KAFKA_BROKERS is set, provisioning is skipped and Kafka clients
// will be configured to communicate with those brokers.
func ProvisionKafka(ctx context.Context) error {
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		logger().Infof("KAFKA_BROKERS is set (%q), skipping Kafka cluster provisioning", brokers)
		kafkaBrokers = strings.Split(brokers, ",")
		return nil
	}

	if v := os.Getenv("KAFKA_NAME"); v != "" {
		kafkaClusterName = v
	}
	if v := os.Getenv("KAFKA_NAMESPACE"); v != "" {
		kafkaNamespace = v
	}

	logger().Infof("provisioning Kafka cluster %q in namespace %q", kafkaClusterName, kafkaNamespace)
	RegisterDestroy("strimzi", func() {
		logger().Info("destroying provisioned Kafka infrastructure...")
		if err := execCommand(context.Background(),
			"kubectl", "delete", "--ignore-not-found", "namespace", kafkaNamespace,
		); err != nil {
			logger().Errorf("error deleting Kafka namespace %q: %w", kafkaNamespace, err)
		}
	})

	// Create Kafka cluster. This assumes Strimzi is already installed in the cluster.
	if err := execCommand(ctx,
		"helm", "upgrade", "--install", "--wait",
		"--create-namespace",
		"--namespace", kafkaNamespace,
		"--set", "name="+kafkaClusterName,
		"--set", "namespace="+kafkaNamespace,
		"--set", "topicOperator=null",
		kafkaClusterName, "../infra/k8s/kafka",
	); err != nil {
		return fmt.Errorf("failed to create Kafka cluster: %w", err)
	}
	logger().Info("waiting for Kafka cluster to be ready...")
	if err := execCommand(ctx,
		"kubectl", "--namespace", kafkaNamespace,
		"wait", "--timeout=240s",
		"--for=condition=Ready=True",
		"kafka", kafkaClusterName,
	); err != nil {
		return fmt.Errorf("error waiting for Kafka broker to be provisioned by Strimzi: %w", err)
	}

	logger().Info("Kafka infastructure fully provisioned!")
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
	var buf bytes.Buffer
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"%s command failed: %w (%s)",
			command, err, strings.TrimSpace(buf.String()),
		)
	}
	return nil
}

// portforwardKafka forwards an ephemeral port to the Kafka broker running
// in Kubernetes, and returns the localhost address.
func portforwardKafka(t testing.TB) string {
	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	pfReq := portforwarder.Request{
		KubeCfg: getEnvOrDefault("KUBE_CONFIG_PATH", "~/.kube/config"),
		// NOTE(marclop) this service name assumes we're using the Strimzi operator
		ServiceName: fmt.Sprintf("%s-kafka-bootstrap", kafkaClusterName),
		Namespace:   kafkaNamespace,
		PortMapping: "0:9093",
	}
	pf, err := pfReq.New(context.Background(), stopCh)
	require.NoError(t, err)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := pf.ForwardPorts(); err != nil {
			t.Fatalf("port forwarder terminated unexpectedly: %v", err)
		}
	}()
	t.Cleanup(func() {
		close(stopCh)
		wg.Wait()
	})

	// wait for port forward to be ready
	<-pf.Ready
	ports, err := pf.GetPorts()
	require.NoError(t, err)
	fmt.Println(ports[0])
	return fmt.Sprintf("127.0.0.1:%d", ports[0].Local)
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
// When Kafka is running in Kubernetes, this will take care of forwarding the
// necessary port(s) to connect to the broker, and clean up on test completion.
func KafkaCommonConfig(t testing.TB, cfg kafka.CommonConfig) kafka.CommonConfig {
	cfg.Brokers = append([]string{}, kafkaBrokers...)
	if len(cfg.Brokers) == 0 {
		brokerAddress := portforwardKafka(t)
		tlsDialer := &tls.Dialer{
			// running locally, should be fast
			NetDialer: &net.Dialer{Timeout: time.Second},
			Config:    &tls.Config{InsecureSkipVerify: true},
		}
		cfg.Brokers = []string{brokerAddress}
		cfg.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			// The advertised broker address is only reachable within
			// the Kubernetes cluster; replace it with the localhost
			// port-forwarded address.
			addr = brokerAddress
			return tlsDialer.DialContext(ctx, network, addr)
		}
	}
	return cfg
}

func getEnvOrDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
