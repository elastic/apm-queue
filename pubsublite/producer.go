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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/pubsublite/internal/telemetry"
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
	// Encoder holds an encoding.Encoder for encoding events.
	Encoder Encoder
	// Logger for the producer.
	Logger *zap.Logger
	// TopicRouter returns the topic where an event should be produced.
	TopicRouter apmqueue.TopicRouter
	// Sync can be used to indicate whether production should be synchronous.
	// Due to the mechanics of pubsub lite publishing, producing synchronously
	// will yield poor performance unless the model.Batch are large enough to
	// trigger immediate flush after processing a single batch.
	Sync       bool
	ClientOpts []option.ClientOption

	// TracerProvider allows specifying a custom otel tracer provider.
	// Defaults to the global one.
	TracerProvider trace.TracerProvider
}

// Validate ensures the configuration is valid, otherwise, returns an error.
func (cfg ProducerConfig) Validate() error {
	var errs []error
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

// resTopic enriches a pubsub.PublishResult with its topic.
type resTopic struct {
	response *pubsub.PublishResult
	topic    apmqueue.Topic
}

// Producer implementes the model.BatchProcessor interface and sends each of
// the events in a batch to a PubSub Lite topic, which is determined by calling
// the configured TopicRouter.
type Producer struct {
	mu        sync.RWMutex
	cfg       ProducerConfig
	producers sync.Map // map[apmqueue.Topic]*pscompat.PublisherClient
	errg      errgroup.Group
	responses chan []resTopic
	closed    chan struct{}
	tracer    trace.Tracer

	project string
	region  string
}

// NewProducer creates a new PubSub Lite producer for a single project.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("pubsublite: invalid producer config: %w", err)
	}

	tracerProvider := cfg.TracerProvider
	if tracerProvider == nil {
		tracerProvider = otel.GetTracerProvider()
	}

	p := &Producer{
		cfg:    cfg,
		closed: make(chan struct{}),
		// NOTE(marclop) should the channel size be dynamic? 1000 is an arbitrary
		// number, but it must be greater than 0, so async produces don't block.
		responses: make(chan []resTopic, 1000),
		tracer:    tracerProvider.Tracer("pubsublite"),

		project: cfg.Project,
		region:  cfg.Region,
	}

	if !cfg.Sync {
		// If producing is async, start a goroutine that blocks until the
		// all messages have been produced. This happens in the ProcessBatch
		// function when producing is set to sync.
		p.errg.Go(func() error {
			ctx := context.Background()
			for responses := range p.responses {
				blockUntilProduced(ctx, responses, cfg.Logger)
			}
			return nil
		})
	}

	return p, nil
}

// Close stops the producer.
//
// This call is blocking and will cause all the underlying clients to stop
// producing. If producing is asynchronous, it'll block until all messages
// have been produced. After Close() is called, Producer cannot be reused.
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.producers.Range(func(key, value any) bool {
		value.(*pscompat.PublisherClient).Stop()
		return true
	})
	close(p.closed)
	close(p.responses)
	return p.errg.Wait()
}

// ProcessBatch publishes the batch to the PubSub Lite topic inferred from the
// configured TopicRouter. If the Producer is synchronous, it waits until all
// messages have been produced to PubSub Lite, otherwise, returns as soon as
// the messages have been stored in the producer's buffer.
func (p *Producer) ProcessBatch(ctx context.Context, batch *model.Batch) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	select {
	case <-p.closed:
		return errors.New("pubsublite: producer closed")
	default:
	}
	responses := make([]resTopic, 0, len(*batch))
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
		publisher, err := p.getPublisher(topic)
		if err != nil {
			return fmt.Errorf("pubsublite: failed to get publisher: %w", err)
		}
		responses = append(responses, resTopic{
			// NOTE(marclop) producer.Publish() is completely asynchronous and
			// doesn't use the context. If/when the pubsublite library supports
			// instrumentation, the context will be useful to propagate traces.
			// This is accurates as of pubsublite@v1.7.0
			response: telemetry.Publisher(ctx, p.tracer, &msg, publisher.Publish, []attribute.KeyValue{
				semconv.MessagingDestinationNameKey.String(string(topic)),
				semconv.CloudRegion(p.region),
				semconv.CloudAccountID(p.project),
			}),
			topic: topic,
		})
	}
	if p.cfg.Sync {
		blockUntilProduced(ctx, responses, p.cfg.Logger)
		return nil
	}
	select {
	case p.responses <- responses:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (p *Producer) getPublisher(topic apmqueue.Topic) (*pscompat.PublisherClient, error) {
	if v, ok := p.producers.Load(topic); ok {
		return v.(*pscompat.PublisherClient), nil
	}
	publisher, err := newPublisher(context.Background(), p.cfg, topic)
	if err != nil {
		return nil, fmt.Errorf(
			"pubsublite: failed creating publisher client for topic %s: %w",
			topic, err,
		)
	}
	if existing, ok := p.producers.LoadOrStore(topic, publisher); ok {
		// Another goroutine created the publisher concurrently, so close the one we just created.
		publisher.Stop()
		publisher = existing.(*pscompat.PublisherClient)
	}
	return publisher, nil
}

func newPublisher(ctx context.Context, cfg ProducerConfig, topic apmqueue.Topic) (*pscompat.PublisherClient, error) {
	// TODO(marclop) connection pools:
	// https://pkg.go.dev/cloud.google.com/go/pubsublite#hdr-gRPC_Connection_Pools
	settings := pscompat.PublishSettings{
		// TODO(marclop) tweak producing settings, to cap memory use, trying
		// to size for good performance. It may be desireable to provide a
		// maximum memory usage for this component and size accordingly.
		// The number of topics should be taken into account since it creates
		// a publisher client per topic.
	}
	return pscompat.NewPublisherClientWithSettings(ctx,
		formatTopic(cfg.Project, cfg.Region, topic),
		settings, cfg.ClientOpts...,
	)
}

func blockUntilProduced(ctx context.Context, res []resTopic, logger *zap.Logger) {
	// TODO(marclop) Retryable errors are automatically handled. If a result
	// returns an error, this indicates that the publisher client encountered
	// a fatal error and can no longer be used. Fatal errors should be manually
	// inspected and the cause resolved. A new publisher client instance must
	// be created to republish failed messages.
	// Any time an error is logged, we should attempt to re-create the producer
	// instance that attempted to produce the message. Currently, the producer
	// will be useless and all the messages that are attempted to be producer
	// will fail with an error.
	for _, res := range res {
		if serverID, err := res.response.Get(ctx); err != nil {
			logger.Error("failed producing message",
				zap.Error(err),
				zap.String("server_id", serverID),
				zap.String("topic", string(res.topic)),
			)
		}
	}
}

func (p *Producer) Healthy(ctx context.Context) error {
	return nil // TODO(marclop)
}

func formatTopic(project, region string, topic apmqueue.Topic) string {
	return fmt.Sprintf("projects/%s/locations/%s/topics/%s",
		project, region, topic,
	)
}
