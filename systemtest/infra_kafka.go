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
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/foxcpp/go-mockdns"
	"github.com/hashicorp/terraform-exec/tfexec"
)

var kafkaBrokers = []string{"localhost:9093"}

var (
	runOnce      sync.Once
	mockResolver *net.Resolver
)

// KafkaConfig for Kafka cluster provisioning with topics.
type KafkaConfig struct {
	// TFPath is the path where the terraform files are present. If it's not
	// `tf/kafka`, PortForward won't work
	TFPath string
	// Topics to create in the Kafka cluster.
	Topics []string
	// PortForward. If set to true, it forwards traffic to the PortForwardResource
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
// - namespace
// - topics
func ProvisionKafka(ctx context.Context, cfg KafkaConfig) error {
	logger.Infof("provisioning Kafka infrastructure with config: %+v", cfg)
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
	namespace := "kafka"
	topicsVar := tfexec.Var(fmt.Sprintf("topics=%s", jsonTopics))
	namespaceVar := tfexec.Var(fmt.Sprintf("namespace=%s", namespace))
	// Ensure terraform destroy runs once per TF path.
	RegisterDestroy(cfg.TFPath, func() {
		logger.Info("destroying provisioned Kafka infrastructure...")
		tf.Destroy(ctx, topicsVar, namespaceVar)
	})
	if err := tf.Apply(ctx, topicsVar, namespaceVar); err != nil {
		return err
	}
	logger.Info("Kafka infastructure fully provisioned!")
	if cfg.PortForward {
		var err error
		runOnce.Do(func() {
			// NOTE(marclop) These are hardcoded and assume the used terraform
			// uses the strimzi operator.
			if cfg.PortForwardResource == "" {
				cfg.PortForwardResource = "service/kafka-kafka-bootstrap"
			}
			if cfg.PortForwardMapping == "" {
				cfg.PortForwardMapping = "9093:9093"
			}
			err = kubectlForwardKafka(namespace,
				cfg.PortForwardResource,
				cfg.PortForwardMapping,
			)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func kubectlForwardKafka(ns, service, mapping string) error {
	cmd := exec.Command("kubectl", "port-forward", "-n", ns, service, mapping)
	logger.Infof("Running %s...", strings.Join(cmd.Args, " "))
	if err := cmd.Start(); err != nil {
		return err
	}
	// Patch zone for Kafka client calls to be correctly resolved.
	srv, err := mockdns.NewServerWithLogger(map[string]mockdns.Zone{
		// NOTE(marclop) The entry is hardcoded and assumes the used terraform
		// uses the strimzi operator, and the cluster name is "kafka".
		fmt.Sprintf("kafka-kafka-0.kafka-kafka-brokers.%s.svc.", ns): {
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
	go cmd.Wait() // Run the forwarder
	RegisterDestroy(ns+service+mapping, func() {
		logger.Infof("Stopping %s...", strings.Join(cmd.Args, " "))
		srv.Close()
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
