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

package pubsublite

import (
	"context"
	"errors"
	"fmt"
	"path"

	"cloud.google.com/go/pubsublite"
	"github.com/googleapis/gax-go/v2/apierror"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

// ManagerConfig holds configuration for managing GCP Pub/Sub Lite resources.
type ManagerConfig struct {
	CommonConfig
}

// Validate checks that cfg is valid, and returns an error otherwise.
func (cfg ManagerConfig) Validate() error {
	var errs []error
	if err := cfg.CommonConfig.Validate(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Manager manages GCP Pub/Sub topics.
type Manager struct {
	cfg    ManagerConfig
	client *pubsublite.AdminClient
}

// NewManager returns a new Manager with the given config.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if err := cfg.CommonConfig.setFromEnv(); err != nil {
		return nil, fmt.Errorf("pubsublite: failed to set config from environment: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("pubsublite: invalid manager config: %w", err)
	}
	client, err := pubsublite.NewAdminClient(
		context.Background(), cfg.Region, cfg.ClientOptions...,
	)
	if err != nil {
		return nil, fmt.Errorf("pubsublite: failed creating admin client: %w", err)
	}
	return &Manager{cfg: cfg, client: client}, nil
}

// Close closes the manager's resources.
func (m *Manager) Close() error {
	return m.client.Close()
}

// ListReservations lists reservations in the configured project and region.
func (m *Manager) ListReservations(ctx context.Context) ([]string, error) {
	parent := fmt.Sprintf("projects/%s/locations/%s", m.cfg.Project, m.cfg.Region)
	iter := m.client.Reservations(ctx, parent)
	var names []string
	for {
		reservation, err := iter.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				return names, nil
			}
			return nil, fmt.Errorf("pubsublite: failed listing reservations: %w", err)
		}
		names = append(names, path.Base(reservation.Name))
	}
}

// ListReservationTopics lists topics in the given reservation.
func (m *Manager) ListReservationTopics(ctx context.Context, reservation string) ([]string, error) {
	parent := fmt.Sprintf("projects/%s/locations/%s/reservations/%s", m.cfg.Project, m.cfg.Region, reservation)
	iter := m.client.ReservationTopics(ctx, parent)
	var names []string
	for {
		topicPath, err := iter.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				return names, nil
			}
			return nil, fmt.Errorf("pubsublite: failed listing topics for reservation %q: %w", reservation, err)
		}
		names = append(names, path.Base(topicPath))
	}
}

// ListTopicSubscriptions lists subscriptions for the given topic.
func (m *Manager) ListTopicSubscriptions(ctx context.Context, topic string) ([]string, error) {
	parent := fmt.Sprintf("projects/%s/locations/%s/topics/%s", m.cfg.Project, m.cfg.Region, topic)
	iter := m.client.TopicSubscriptions(ctx, parent)
	var names []string
	for {
		subscriptionPath, err := iter.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				return names, nil
			}
			return nil, fmt.Errorf("pubsublite: failed listing subscriptions for topic %q: %w", topic, err)
		}
		names = append(names, path.Base(subscriptionPath))
	}
}

// CreateReservation creates a reservation with the given name and throughput capacity.
//
// Reservations that already exist will be left unmodified.
func (m *Manager) CreateReservation(ctx context.Context, name string, throughputCapacity int) error {
	logger := m.cfg.Logger.With(zap.String("reservation", name))
	_, err := m.client.CreateReservation(ctx, pubsublite.ReservationConfig{
		Name: fmt.Sprintf(
			"projects/%s/locations/%s/reservations/%s",
			m.cfg.Project, m.cfg.Region, name,
		),
		ThroughputCapacity: throughputCapacity,
	})
	if err != nil {
		if err, ok := apierror.FromError(err); ok && err.Reason() == "RESOURCE_ALREADY_EXISTS" {
			logger.Debug("pubsublite reservation already exists")
			return nil
		}
		return fmt.Errorf("failed to create pubsublite reservation %q: %w", name, err)
	}
	logger.Info("created pubsublite reservation")
	return nil
}

// CreateSubscription creates a reservation with the given name and throughput capacity.
//
// Subscriptions that already exist will be left unmodified.
func (m *Manager) CreateSubscription(
	ctx context.Context, name, topic string,
	deliverImmediately bool,
) error {
	logger := m.cfg.Logger.With(
		zap.String("subscription", name),
		zap.String("topic", topic),
	)
	deliveryRequirement := pubsublite.DeliverAfterStored
	if deliverImmediately {
		deliveryRequirement = pubsublite.DeliverImmediately
	}
	_, err := m.client.CreateSubscription(ctx, pubsublite.SubscriptionConfig{
		Name: fmt.Sprintf(
			"projects/%s/locations/%s/subscriptions/%s",
			m.cfg.Project, m.cfg.Region, name,
		),
		Topic: fmt.Sprintf(
			"projects/%s/locations/%s/topics/%s",
			m.cfg.Project, m.cfg.Region, topic,
		),
		DeliveryRequirement: deliveryRequirement,
	}, pubsublite.AtTargetLocation(pubsublite.Beginning))
	if err != nil {
		if err, ok := apierror.FromError(err); ok && err.Reason() == "RESOURCE_ALREADY_EXISTS" {
			logger.Debug("pubsublite subscription already exists")
			return nil
		}
		return fmt.Errorf(
			"failed to create pubsublite subscription %q for topic %q: %w", name, topic, err,
		)
	}
	logger.Info(
		"created pubsublite subscription",
		zap.Bool("deliver_immediately", deliverImmediately),
	)
	return nil
}

// DeleteReservation deletes the given reservation.
//
// No error is returned if the reservation does not exist.
func (m *Manager) DeleteReservation(ctx context.Context, reservation string) error {
	logger := m.cfg.Logger.With(zap.String("reservation", reservation))
	if err := m.client.DeleteReservation(ctx, fmt.Sprintf(
		"projects/%s/locations/%s/reservations/%s", m.cfg.Project, m.cfg.Region, reservation,
	)); err != nil {
		if err, ok := apierror.FromError(err); ok && err.Reason() == "RESOURCE_NOT_EXIST" {
			logger.Debug("pubsublite reservation does not exist")
			return nil
		}
		return fmt.Errorf("failed to delete pubsublite reservation %q: %w", reservation, err)
	}
	logger.Info("deleted pubsublite reservation")
	return nil
}

// DeleteTopic deletes the given topic.
//
// No error is returned if the topic does not exist.
func (m *Manager) DeleteTopic(ctx context.Context, topic string) error {
	logger := m.cfg.Logger.With(zap.String("topic", topic))
	if err := m.client.DeleteTopic(ctx, fmt.Sprintf(
		"projects/%s/locations/%s/topics/%s", m.cfg.Project, m.cfg.Region, topic,
	)); err != nil {
		if err, ok := apierror.FromError(err); ok && err.Reason() == "RESOURCE_NOT_EXIST" {
			logger.Debug("pubsublite topic does not exist")
			return nil
		}
		return fmt.Errorf("failed to delete pubsublite topic %q: %w", topic, err)
	}
	logger.Info("deleted pubsublite topic")
	return nil
}

// DeleteSubscription deletes the given subscription.
//
// No error is returned if the subscription does not exist.
func (m *Manager) DeleteSubscription(ctx context.Context, subscription string) error {
	logger := m.cfg.Logger.With(zap.String("subscription", subscription))
	if err := m.client.DeleteSubscription(ctx, fmt.Sprintf(
		"projects/%s/locations/%s/subscriptions/%s", m.cfg.Project, m.cfg.Region, subscription,
	)); err != nil {
		if err, ok := apierror.FromError(err); ok && err.Reason() == "RESOURCE_NOT_EXIST" {
			logger.Debug("pubsublite subscription does not exist")
			return nil
		}
		return fmt.Errorf("failed to delete pubsublite subscription %q: %w", subscription, err)
	}
	logger.Info("deleted pubsublite subscription")
	return nil
}
