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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/foxcpp/go-mockdns"
	"github.com/hashicorp/terraform-exec/tfexec"
)

const (
	// KafkaDeploymentTypeKey is the key to access the deployment type from
	// the Terraform output.
	KafkaDeploymentTypeKey = "deployment_type"
	// KafkaBrokersKey is the key to access the list of kafka brokers from the
	// Terraform output.
	KafkaBrokersKey = "kafka_brokers"
)

var brokersMu sync.RWMutex
var kafkaBrokers = []string{"localhost:9093"}

var (
	runOnce      sync.Once
	mockResolver *net.Resolver
)

// KafkaConfig for Kafka cluster provisioning with topics.
type KafkaConfig struct {
	// TFPath is the path where the terraform files are present.
	TFPath string
	// Namespace to pass down as the `namespace` Terraform var.
	Namespace string
	// Namespace to pass down as the `name` Terraform var.
	Name string
	// Topics to create in the Kafka cluster. It's passed down as the
	// `topics` Terraform var.
	Topics []string
	// PortForward. If set to true, it forwards traffic to PortForwardResource
	// using PortForwardMapping.
	PortForward bool
	// PortForwardResource specifies the resource to which the traffic
	// is forwarded.
	PortForwardResource string
	// PortForwardMapping holds the port forward mapping for the specified
	// resource.
	PortForwardMapping string
}

func newLocalKafkaConfig(topics ...string) KafkaConfig {
	return KafkaConfig{
		Topics:      topics,
		PortForward: true,
		TFPath:      filepath.Join("tf", "kafka"),
	}
}

// ProvisionKafka provisions a Kafka cluster. The specified terraform path
// must accept at least these variables:
// - namespace (string)
// - name (string)
// - topics (list(string))
// The terraform module should output at least these variables:
// - deployment_type (string)
// - kafka_brokers (list(string))
//
// If `deployment_type` output is "k8s", `kubectl port-forward` will be run
// targeting the `PortForwardResource` with `PortForwardMapping`.
func ProvisionKafka(ctx context.Context, cfg KafkaConfig) error {
	if cfg.Name == "" {
		if n := os.Getenv("KAFKA_NAME"); n != "" {
			cfg.Name = n
		} else {
			cfg.Name = "kafka"
		}
	}
	if cfg.Namespace == "" {
		if n := os.Getenv("KAFKA_NAMESPACE"); n != "" {
			cfg.Namespace = n
		} else {
			cfg.Namespace = "kafka"
		}
	}
	logger().Infof("provisioning Kafka infrastructure with config: %+v", cfg)
	tf, err := NewTerraform(ctx, cfg.TFPath)
	if err != nil {
		return err
	}
	if err := tf.Init(ctx, tfexec.Upgrade(true)); err != nil {
		return err
	}
	// Create a dummy topic so the infrastructure awaits until all the required
	// infrastructure is available. It will be destroyed during the first test.
	if len(cfg.Topics) == 0 {
		cfg.Topics = append(cfg.Topics, SuffixTopics("dummy")...)
	}
	jsonTopics, err := json.Marshal(cfg.Topics)
	if err != nil {
		return err
	}
	namespaceVar := tfexec.Var(fmt.Sprintf("namespace=%s", cfg.Namespace))
	nameVar := tfexec.Var(fmt.Sprintf("name=%s", cfg.Name))
	topicsVar := tfexec.Var(fmt.Sprintf("topics=%s", jsonTopics))
	// Ensure terraform destroy runs once per TF path.
	RegisterDestroy(cfg.TFPath, func() {
		logger().Info("destroying provisioned Kafka infrastructure...")
		tf.Destroy(ctx, topicsVar, namespaceVar, nameVar)
	})
	if err := tf.Apply(ctx, topicsVar, namespaceVar, nameVar); err != nil {
		return err
	}
	tfOutput, err := tf.Output(ctx)
	if err != nil {
		return err
	}
	if raw, ok := tfOutput[KafkaBrokersKey]; ok {
		var brokers []string
		if err := json.Unmarshal(raw.Value, &brokers); err != nil {
			return err
		}
		if len(brokers) > 0 {
			SetKafkaBrokers(brokers...)
		}
	}
	dt := string(bytes.TrimSpace(tfOutput[KafkaDeploymentTypeKey].Value))
	// If PortForward is set to true, and the deployment_type is k8s, start
	// forwarding traffic to/from kafka to localhost.
	if cfg.PortForward && dt == `"k8s"` {
		var err error
		runOnce.Do(func() {
			// NOTE(marclop) These assume terraform uses the strimzi operator.
			if cfg.PortForwardResource == "" {
				cfg.PortForwardResource = fmt.Sprintf("service/%s-kafka-bootstrap", cfg.Name)
			}
			if cfg.PortForwardMapping == "" {
				cfg.PortForwardMapping = "9093:9093"
			}
			err = kubectlForwardKafka(cfg.Namespace,
				cfg.Name,
				cfg.PortForwardResource,
				cfg.PortForwardMapping,
			)
		})
		if err != nil {
			return err
		}
	}
	logger().Info("Kafka infastructure fully provisioned!")
	return nil
}

func kubectlForwardKafka(ns, name, service, mapping string) error {
	cmd := exec.Command("kubectl", "port-forward", "-n", ns, service, mapping)
	cmdStr := strings.Join(cmd.Args, " ")
	logger().Infof("Running %s...", cmdStr)
	if err := cmd.Start(); err != nil {
		return err
	}
	// Patch zone for Kafka client calls to be correctly resolved.
	srv, err := mockdns.NewServerWithLogger(map[string]mockdns.Zone{
		// NOTE(marclop) Assumes terraform uses the strimzi operator.
		fmt.Sprintf("%s-kafka-0.%s-kafka-brokers.%s.svc.", name, name, ns): {
			A: []string{"127.0.0.1"},
		},
	}, log.New(io.Discard, "", 0), false)
	if err != nil {
		return err
	}
	if mockResolver == nil {
		resolver := new(net.Resolver)
		srv.PatchNet(resolver)
		mockResolver = resolver
	}
	c := make(chan struct{})
	go func() {
		err := cmd.Wait() // Run the forwarder
		if err == nil {
			return
		}
		select {
		case <-c:
		default:
			logger().Errorf("%s exited before tests were completed: %v",
				cmdStr, err.Error(),
			)
		}
	}()
	RegisterDestroy(ns+service+mapping, func() {
		logger().Infof("Stopping %s...", cmdStr)
		srv.Close()
		close(c)
		cmd.Process.Kill()
		cmd.Process.Wait()
	})
	return nil
}

func newKafkaTLSDialer() *tls.Dialer {
	return &tls.Dialer{
		NetDialer: &net.Dialer{Resolver: mockResolver},
		Config:    &tls.Config{InsecureSkipVerify: true},
	}
}

// KafkaBrokers returns the Kafka brokers to use for tests.
func KafkaBrokers() []string {
	brokersMu.RLock()
	defer brokersMu.RUnlock()
	return append(kafkaBrokers)
}

// SetKafkaBrokers sets the kafka brokers.
func SetKafkaBrokers(brokers ...string) {
	brokersMu.Lock()
	defer brokersMu.Unlock()
	kafkaBrokers = append(brokers)
}
