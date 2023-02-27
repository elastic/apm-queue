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
	"sync"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/pscompat"
	"go.uber.org/zap"
	"google.golang.org/api/option"

	"github.com/elastic/apm-data/model"
	"github.com/elastic/apm-queue/encoding"
	"github.com/elastic/apm-queue/queuecontext"
)

// ProducerConfig for the PubSub Lite producer.
type ProducerConfig struct {
	// Topic is the PubSub Lite topic.
	Topic Topic
	// Codec for a specific encoding.
	Codec encoding.Codec
	// Logger for the producer.
	Logger     *zap.Logger
	ClientOpts []option.ClientOption
}

// Topic represents a PubSub Lite topic.
type Topic struct {
	// Project where the topic is located.
	Project string
	// Region where the topic is located.
	Region string
	// Name/ID of the topic.
	Name string
}

func (s Topic) String() string {
	return fmt.Sprintf("projects/%s/locations/%s/topics/%s",
		s.Project, s.Region, s.Name,
	)
}

// Validate ensures the topic is valid.
func (s Topic) Validate() error {
	var errs []error
	if s.Name == "" {
		errs = append(errs, errors.New("pubsublite: topic: name must be set"))
	}
	if s.Project == "" {
		errs = append(errs, errors.New("pubsublite: topic: project must be set"))
	}
	if s.Region == "" {
		errs = append(errs, errors.New("pubsublite: topic: region must be set"))
	}
	return errors.Join(errs...)
}

// Validate ensures the configuration is valid, otherwise, returns an error.
func (cfg ProducerConfig) Validate() error {
	var errs []error
	if err := cfg.Topic.Validate(); err != nil {
		errs = append(errs, err)
	}
	if cfg.Codec == nil {
		errs = append(errs, errors.New("pubsublite: codec must be set"))
	}
	if cfg.Logger == nil {
		errs = append(errs, errors.New("pubsublite: logger must be set"))
	}
	return errors.Join(errs...)
}

// Producer implementes the model.BatchProcessor interface and sends each of
// the events in a batch to a PubSub Lite topic.
type Producer struct {
	mu       sync.RWMutex
	cfg      ProducerConfig
	producer *pscompat.PublisherClient
	closed   chan struct{}
}

// NewProducer creates a new PubSub Lite producer for a single project.
func NewProducer(ctx context.Context, cfg ProducerConfig) (*Producer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// TODO(marclop) connection pools:
	// https://pkg.go.dev/cloud.google.com/go/pubsublite#hdr-gRPC_Connection_Pools
	settings := pscompat.PublishSettings{
		// TODO(marclop) tweak producing settings, (maybe) key extractor.
	}
	publisher, err := pscompat.NewPublisherClientWithSettings(
		ctx, cfg.Topic.String(), settings, cfg.ClientOpts...,
	)
	if err != nil {
		return nil, err
	}
	cfg.Logger = cfg.Logger.With(zap.String("topic", cfg.Topic.Name))
	return &Producer{
		cfg:      cfg,
		producer: publisher,
		closed:   make(chan struct{}),
	}, nil
}

// Close stops the producer
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.producer.Stop()
	close(p.closed)
	return nil
}

// ProcessBatch processes a model.Batch.
func (p *Producer) ProcessBatch(ctx context.Context, batch *model.Batch) error {
	projectID, ok := queuecontext.ProjectFromContext(ctx)
	if !ok {
		return errors.New("project ID missing")
	}
	var responses []*pubsub.PublishResult
	p.mu.RLock()
	defer p.mu.RUnlock()
	select {
	case <-p.closed:
		return errors.New("producer closed")
	default:
	}
	for _, event := range *batch {
		encoded, err := p.cfg.Codec.Encode(event)
		if err != nil {
			return err
		}
		responses = append(responses, p.producer.Publish(ctx, &pubsub.Message{
			Attributes: map[string]string{
				"project_id": projectID,
				"processor":  event.Processor.Event,
			},
			Data: encoded,
		}))
	}
	// NOTE(marclop) should the error be returned to the client? Does it care?
	for _, res := range responses {
		if serverID, err := res.Get(ctx); err != nil {
			p.cfg.Logger.Error("failed producing message",
				zap.Error(err),
				zap.String("project_id", projectID),
				zap.String("server_id", serverID),
			)
		}
	}
	return nil
}

func (p *Producer) Healthy() error {
	return nil // TODO(marclop)
}
