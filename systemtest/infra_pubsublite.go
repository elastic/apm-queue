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
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/hashicorp/terraform-exec/tfexec"

	apmqueue "github.com/elastic/apm-queue"
)

var (
	googleProject string
	googleRegion  string
)

// PubSubLiteConfig for PubSubLite topics, subscriptions, reservations
type PubSubLiteConfig struct {
	// TFPath is the path where the terraform files are present.
	TFPath string
	// Project is the GCP Project name.
	Project string
	// Region is the GCP region.
	Region string
	// Topics (and subscriptions) to create.
	Topics []apmqueue.Topic
	// ReservationSuffix to use.
	ReservationSuffix string
}

func newPubSubLiteConfig(topics ...apmqueue.Topic) PubSubLiteConfig {
	return PubSubLiteConfig{
		Topics:  topics,
		Project: googleProject,
		Region:  googleRegion,
		TFPath:  filepath.Join("tf", "pubsublite"),
	}
}

// ProvisionPubSubLite provisions the PubSubLite resources. The specified
// terraform path must accept at least these variables:
// - project
// - region
// - suffix (used to suffix the reservation)
// - topics
func ProvisionPubSubLite(ctx context.Context, cfg PubSubLiteConfig) error {
	if cfg.ReservationSuffix == "" {
		cfg.ReservationSuffix = persistentSuffix
	}
	logger().Infof("provisioning PubSubLite infrastructure with config: %+v", cfg)
	tf, err := NewTerraform(ctx, cfg.TFPath)
	if err != nil {
		return fmt.Errorf("failed to create terraform: %w", err)
	}
	if err := tf.Init(ctx, tfexec.Upgrade(true)); err != nil {
		return fmt.Errorf("failed to run terraform init: %w", err)
	}

	jsonTopics, err := json.Marshal(cfg.Topics)
	if err != nil {
		return fmt.Errorf("failed to marshal topics: %w", err)
	}
	projectVar := tfexec.Var(fmt.Sprintf("project=%s", cfg.Project))
	regionVar := tfexec.Var(fmt.Sprintf("region=%s", cfg.Region))
	suffixVar := tfexec.Var(fmt.Sprintf("suffix=%s", cfg.ReservationSuffix))
	topicsVar := tfexec.Var(fmt.Sprintf("topics=%s", jsonTopics))
	// Ensure terraform destroy runs once per TF path.
	RegisterDestroy(cfg.TFPath, func() {
		logger().Info("destroying provisioned PubSubLite infrastructure...")
		tf.Destroy(ctx, projectVar, regionVar, suffixVar, topicsVar)
	})
	if err := tf.Apply(ctx, projectVar, regionVar, suffixVar, topicsVar); err != nil {
		return fmt.Errorf("failed to run terraform apply: %w", err)
	}
	logger().Info("PubSubLite infastructure fully provisioned!")
	return nil
}
