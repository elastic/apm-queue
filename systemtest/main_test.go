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

// Package systemtest contains the queue slow system tests
package systemtest

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

var skipKafka, skipPubsublite bool

func testMain(m *testing.M) (returnCode int) {
	var destroyOnly, skipDestroy bool
	flag.BoolVar(&destroyOnly, "destroy-only", false, "only destroy provisioned infrastructure, do not provision or run tests")
	flag.BoolVar(&skipDestroy, "skip-destroy", false, "do not destroy the provisioned infrastructure after the tests finish")
	flag.BoolVar(&skipKafka, "skip-kafka", false, "skip kafka tests")
	flag.BoolVar(&skipPubsublite, "skip-pubsublite", false, "skip pubsublite tests")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	var destroyFuncs []DestroyInfraFunc
	type initInfraFunc func() (ProvisionInfraFunc, DestroyInfraFunc, error)
	initInfra := func(name string, f initInfraFunc) error {
		provision, destroy, err := f()
		if err != nil {
			return fmt.Errorf("failed to initialize %s: %w", name, err)
		}
		if !destroyOnly {
			g.Go(func() error {
				logger().Infof("provisioning %s infrastructure", name)
				if err := provision(ctx); err != nil {
					return fmt.Errorf("failed to provision %s infrastructure: %w", name, err)
				}
				logger().Infof("finished provisioning %s infrastructure", name)
				return nil
			})
		}
		destroyFuncs = append(destroyFuncs, func(ctx context.Context) error {
			logger().Infof("destroying %s infrastructure", name)
			if err := destroy(ctx); err != nil {
				return fmt.Errorf("failed to destroy %s infrastructure: %w", name, err)
			}
			logger().Infof("finished destroying %s infrastructure", name)
			return nil
		})
		return nil
	}
	if !skipKafka {
		if err := initInfra("kafka", InitKafka); err != nil {
			logger().Error(err)
			return 1
		}
	}
	if !skipPubsublite {
		if err := initInfra("pubsublite", InitPubSubLite); err != nil {
			logger().Error(err)
			return 1
		}
	}
	if !skipDestroy {
		defer func() {
			for _, destroy := range destroyFuncs {
				if err := destroy(context.Background()); err != nil {
					logger().Error(err)
					returnCode = 1
				}
			}
		}()
	}
	if err := g.Wait(); err != nil {
		logger().Error(err)
		return 1
	}
	if destroyOnly {
		// Only destroy infrastructure (in the defer). This is used for teardown in CI.
		return 0
	}
	logger().Info("running system tests...")
	return m.Run()
}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}
