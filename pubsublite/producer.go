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
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/queuecontext"
)

// Encoder encodes a model.APMEvent to a []byte
type Encoder interface {
	// Encode accepts a model.APMEvent and returns the encoded representation.
	Encode(model.APMEvent) ([]byte, error)
}

// ProducerConfig for the PubSub Lite producer.
type ProducerConfig struct {
	// Region is the GCP region for the producer.
	Region string
	// Project is the GCP project for the producer.
	Project string
	// Topics are the PubSub Lite topics where the messages will be produced.
	// The routing is determined by the TopicRouter.
	Topics []apmqueue.Topic
	// Encoder holds an encoding.Encoder for encoding events.
	Encoder Encoder
	// Logger for the producer.
	Logger *zap.Logger
	// TopicRouter returns the topic where an event should be produced.
	TopicRouter apmqueue.TopicRouter
	ClientOpts  []option.ClientOption
}

// Validate ensures the configuration is valid, otherwise, returns an error.
func (cfg ProducerConfig) Validate() error {
	var errs []error
	if len(cfg.Topics) == 0 {
		errs = append(errs,
			errors.New("pubsublite: at least one topic must be set"),
		)
	}
	if cfg.Project == "" {
		errs = append(errs, errors.New("pubsublite: project must be set"))
	}
	if cfg.Region == "" {
		errs = append(errs, errors.New("pubsublite: region must be set"))
	}
	if cfg.Encoder == nil {
		errs = append(errs, errors.New("pubsublite: encoder must be set"))
	}
	if cfg.Logger == nil {
		errs = append(errs, errors.New("pubsublite: logger must be set"))
	}
	if cfg.TopicRouter == nil {
		errs = append(errs, errors.New("pubsublite: topic router must be set"))
	}
	return errors.Join(errs...)
}

// Producer implementes the model.BatchProcessor interface and sends each of
// the events in a batch to a PubSub Lite topic, which is determined by calling
// the configured TopicRouter.
type Producer struct {
	mu       sync.RWMutex
	cfg      ProducerConfig
	producer map[apmqueue.Topic]*pscompat.PublisherClient
	closed   chan struct{}
}

// NewProducer creates a new PubSub Lite producer for a single project.
func NewProducer(ctx context.Context, cfg ProducerConfig) (*Producer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("pubsublite: invalid producer config: %w", err)
	}
	// TODO(marclop) connection pools:
	// https://pkg.go.dev/cloud.google.com/go/pubsublite#hdr-gRPC_Connection_Pools
	settings := pscompat.PublishSettings{
		// TODO(marclop) tweak producing settings, (maybe) key extractor.
	}
	producers := make(map[apmqueue.Topic]*pscompat.PublisherClient)
	for _, topic := range cfg.Topics {
		publisher, err := pscompat.NewPublisherClientWithSettings(ctx,
			formatTopic(cfg.Project, cfg.Region, topic),
			settings, cfg.ClientOpts...,
		)
		if err != nil {
			return nil, fmt.Errorf("pubsublite: failed creating producer: %w", err)
		}
		producers[topic] = publisher
	}
	return &Producer{
		cfg:      cfg,
		producer: producers,
		closed:   make(chan struct{}),
	}, nil
}

// Close stops the producer
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, producer := range p.producer {
		producer.Stop()
	}
	close(p.closed)
	return nil
}

// ProcessBatch processes a model.Batch.
func (p *Producer) ProcessBatch(ctx context.Context, batch *model.Batch) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	select {
	case <-p.closed:
		return errors.New("pubsublite: producer closed")
	default:
	}
	var wg sync.WaitGroup
	for _, event := range *batch {
		encoded, err := p.cfg.Encoder.Encode(event)
		if err != nil {
			return fmt.Errorf("failed to encode event: %w", err)
		}
		msg := pubsub.Message{Data: encoded}
		if meta, ok := queuecontext.MetadataFromContext(ctx); ok {
			for k, v := range meta {
				if msg.Attributes == nil {
					msg.Attributes = make(map[string]string)
				}
				msg.Attributes[k] = v
			}
		}
		topic := p.cfg.TopicRouter(event)
		producer, ok := p.producer[topic]
		if !ok {
			return fmt.Errorf("pubsublite: unable to find producer for %s", topic)
		}
		result := producer.Publish(ctx, &msg)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// NOTE(marclop) should the error be returned to the client? Does it care?
			if serverID, err := result.Get(ctx); err != nil {
				p.cfg.Logger.Error("failed producing message",
					zap.Error(err),
					zap.String("topic", string(topic)),
					zap.ByteString("message.value", msg.Data),
					zap.String("server_id", serverID),
					zap.Any("headers", msg.Attributes),
				)
			}
		}()
	}
	wg.Wait()
	return nil
}

func (p *Producer) Healthy() error {
	return nil // TODO(marclop)
}

func formatTopic(project, region string, topic apmqueue.Topic) string {
	return fmt.Sprintf("projects/%s/locations/%s/topics/%s",
		project, region, topic,
	)
}
