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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/pubsublite"
)

var (
	googleProject string
	googleRegion  string
	googleAccount string

	pubsubliteReservationPrefix string
	pubsubliteReservation       string
	pubsubliteManager           *pubsublite.Manager
)

// InitPubSubLite initialises Pub/Sub Lite configuration, and returns a pair
// of functions for provisioning and destroying a Pub/Sub Lite throughput
// reservation and associated resources.
func InitPubSubLite() (ProvisionInfraFunc, DestroyInfraFunc, error) {
	initGCloud()
	if googleProject == "" {
		return nil, nil, errors.New("could not determine Google Cloud project; gcloud not initialized")
	}
	if googleRegion == "" {
		return nil, nil, errors.New("could not determine Google Cloud region; GOOGLE_REGION not set")
	}
	if googleAccount == "" {
		return nil, nil, errors.New("could not determine Google Cloud account; gcloud not initialized")
	}

	pubsubliteReservationPrefix = os.Getenv("PUBSUBLITE_RESERVATION_PREFIX")
	if pubsubliteReservationPrefix != "" {
		// Allow CI to specify a prefix, so it can automatically and safely clean up.
		logger().Infof("$PUBSUBLITE_RESERVATION_PREFIX set to %q", pubsubliteReservationPrefix)
	} else {
		pubsubliteReservationPrefix = "systemtest-" + sanitizePubSubSuffix(googleAccount) + "-"
	}

	pubsubliteReservation = pubsubliteReservationPrefix + RandomSuffix()
	logger().Infof(
		"managing Pub/Sub Lite throughput reservation %q in project %q, region %q, account %q",
		pubsubliteReservation, googleProject, googleRegion, googleAccount,
	)
	return ProvisionPubSubLite, DestroyPubSubLite, nil
}

// ProvisionPubSubLite provisions a Pub/Sub Lite throughput reservation
// using configuration taken from `gcloud` and $GOOGLE_REGION.
func ProvisionPubSubLite(ctx context.Context) error {
	manager, err := pubsublite.NewManager(pubsublite.ManagerConfig{
		CommonConfig: PubSubLiteCommonConfig(pubsublite.CommonConfig{
			Logger: logger().Desugar().Named("pubsublite"),
		}),
	})
	if err != nil {
		return err
	}
	pubsubliteManager = manager

	// Create the reservation if it doesn't already exist. We generate a random
	// reservation name for the process's lifetime, since resource names cannot
	// be reused within an hour of being destroyed.
	const throughputCapacity = 2
	if err := manager.CreateReservation(ctx, pubsubliteReservation, throughputCapacity); err != nil {
		return err
	}
	return nil
}

func DestroyPubSubLite(ctx context.Context) error {
	manager, err := pubsublite.NewManager(pubsublite.ManagerConfig{
		CommonConfig: PubSubLiteCommonConfig(pubsublite.CommonConfig{
			Logger: logger().Desugar().Named("pubsublite"),
		}),
	})
	if err != nil {
		return err
	}
	defer manager.Close()
	if err := destroyPubsubResources(ctx, manager, pubsubliteReservationPrefix); err != nil {
		return fmt.Errorf("failed to destroy pubsublite resources: %w", err)
	}
	return nil
}

// PubSubLiteCommonConfig returns a pubsublite.CommonConfig suitable for
// using to construct pubsublite resources.
func PubSubLiteCommonConfig(cfg pubsublite.CommonConfig) pubsublite.CommonConfig {
	cfg.Project = googleProject
	cfg.Region = googleRegion
	return cfg
}

// CreatePubsubTopics interacts with the Google Cloud API to create
// Pub/Sub Lite topics.
func CreatePubsubTopics(ctx context.Context, t testing.TB, partitions int, topics ...apmqueue.Topic) {
	topicCreator, err := pubsubliteManager.NewTopicCreator(pubsublite.TopicCreatorConfig{
		Reservation:                pubsubliteReservation,
		PartitionCount:             partitions,
		PublishCapacityMiBPerSec:   4,
		SubscribeCapacityMiBPerSec: 4,
		PerPartitionBytes:          30 * 1024 * 1024 * 1024,
		RetentionDuration:          time.Hour,
	})
	require.NoError(t, err)
	err = topicCreator.CreateTopics(ctx, topics...)
	require.NoError(t, err)
	for _, topic := range topics {
		topic := string(topic)
		t.Cleanup(func() {
			err := pubsubliteManager.DeleteTopic(context.Background(), topic)
			require.NoError(t, err)
		})
	}
}

// CreatePubsubTopicSubscriptions interacts with the Google Cloud API to create
// Pub/Sub Lite subscriptions.
func CreatePubsubTopicSubscriptions(ctx context.Context, t testing.TB, consumer string, topics ...apmqueue.Topic) {
	for _, topic := range topics {
		subscriptionName := pubsublite.JoinTopicConsumer(topic, consumer)
		err := pubsubliteManager.CreateSubscription(ctx, subscriptionName, string(topic), true)
		require.NoError(t, err)
		t.Cleanup(func() {
			err := pubsubliteManager.DeleteSubscription(context.Background(), subscriptionName)
			require.NoError(t, err)
		})
	}
}

func destroyPubsubResources(ctx context.Context, manager *pubsublite.Manager, reservationPrefix string) error {
	var g errgroup.Group
	reservations, err := manager.ListReservations(ctx)
	if err != nil {
		return err
	}
	for _, reservation := range reservations {
		if !strings.HasPrefix(reservation, reservationPrefix) {
			continue
		}
		reservation := reservation // copy for closure
		g.Go(func() error {
			topics, err := manager.ListReservationTopics(ctx, reservation)
			if err != nil {
				return err
			}
			var g errgroup.Group
			for _, topic := range topics {
				topic := topic // copy for closure
				g.Go(func() error {
					subscriptions, err := manager.ListTopicSubscriptions(ctx, topic)
					if err != nil {
						return err
					}
					var g errgroup.Group
					for _, subscription := range subscriptions {
						subscription := subscription
						g.Go(func() error {
							return manager.DeleteSubscription(ctx, subscription)
						})
					}
					if err := g.Wait(); err != nil {
						return err
					}
					return manager.DeleteTopic(ctx, topic)
				})
			}
			if err := g.Wait(); err != nil {
				return err
			}
			return manager.DeleteReservation(ctx, reservation)
		})
	}
	return g.Wait()
}

func sanitizePubSubSuffix(id string) string {
	var out strings.Builder
	out.Grow(len(id))
	for _, r := range id {
		switch r {
		case '-', '.', '_', '~', '%', '+':
			out.WriteRune(r)
		default:
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				out.WriteRune(r)
			} else {
				out.WriteRune('_')
			}
		}
	}
	return out.String()
}

var initGCloudOnce sync.Once

func initGCloud() {
	initGCloudOnce.Do(func() {
		logger := logger().Desugar()
		cmd := exec.Command("gcloud", "config", "list", "--format=json")
		cmd.Stderr = os.Stderr
		output, err := cmd.Output()
		if err != nil {
			logger.Warn("`gcloud config list` failed", zap.Error(err))
			return
		}
		var gcloudConfig struct {
			Core struct {
				Account string `json:"account"`
				Project string `json:"project"`
			}
		}
		if err := json.Unmarshal(output, &gcloudConfig); err != nil {
			logger.Warn("failed to decode gcloud output", zap.Error(err))
			return
		}
		googleProject = gcloudConfig.Core.Project
		googleRegion = os.Getenv("GOOGLE_REGION")
		googleAccount = gcloudConfig.Core.Account
	})
}
