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
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	apmqueue "github.com/elastic/apm-queue/v2"
	"github.com/elastic/apm-queue/v2/pubsublite/internal/pubsubabs"
	"github.com/elastic/apm-queue/v2/pubsublite/internal/telemetry"
	"github.com/elastic/apm-queue/v2/queuecontext"
)

// ProducerConfig for the PubSub Lite producer.
type ProducerConfig struct {
	CommonConfig
	// Sync can be used to indicate whether production should be synchronous.
	// Due to the mechanics of PubSub Lite publishing, producing synchronously
	// will yield poor performance the unless a single call to produce contains
	// enough records that are large enough to cause immediate flush.
	Sync bool
}

// finalize ensures the configuration is valid, setting default values from
// environment variables as described in doc comments, returning an error if
// any configuration is invalid.
func (cfg *ProducerConfig) finalize() error {
	return cfg.CommonConfig.finalize()
}

// resTopic enriches a pubsub.PublishResult with its topic.
type resTopic struct {
	response pubsubabs.PublishResult
	topic    apmqueue.Topic
}

var _ apmqueue.Producer = &Producer{}

// Producer implementes the apmqueue.Producer interface and sends each of
// the events in a batch to a PubSub Lite topic, which is determined by calling
// the configured TopicRouter.
type Producer struct {
	mu        sync.RWMutex
	cfg       ProducerConfig
	producers sync.Map // map[apmqueue.Topic]pubsubabs.Publisher
	errg      errgroup.Group
	responses chan []resTopic
	closed    chan struct{}
	tracer    trace.Tracer
	metrics   telemetry.PublisherMetrics
}

// NewProducer creates a new PubSub Lite producer for a single project.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if err := cfg.finalize(); err != nil {
		return nil, fmt.Errorf("pubsublite: invalid producer config: %w", err)
	}
	metrics, err := telemetry.NewPublisherMetrics(cfg.meterProvider())
	if err != nil {
		return nil, fmt.Errorf("pubsublite: %w", err)
	}
	p := &Producer{
		cfg:    cfg,
		closed: make(chan struct{}),
		// NOTE(marclop) should the channel size be dynamic? 1000 is an arbitrary
		// number, but it must be greater than 0, so async produces don't block.
		responses: make(chan []resTopic, 1000),
		tracer:    cfg.tracerProvider().Tracer("pubsublite"),
		metrics:   metrics,
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
	select {
	case <-p.closed:
		return nil
	default:
	}
	p.producers.Range(func(key, value any) bool {
		value.(pubsubabs.Publisher).Stop()
		p.producers.Delete(key)
		return true
	})
	close(p.closed)
	close(p.responses)
	return p.errg.Wait()
}

// Produce produces N records. If the Producer is synchronous, waits until
// all records are produced, otherwise, returns as soon as the records are
// stored in the producer buffer, or when the records are produced to the
// queue if sync producing is configured.
// If the context has been enriched with metadata, each entry will be added
// as a record's header.
// Produce takes ownership of Record and any modifications after Produce is
// called may cause an unhandled exception.
func (p *Producer) Produce(ctx context.Context, rs ...apmqueue.Record) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	select {
	case <-p.closed:
		return errors.New("pubsublite: producer closed")
	default:
	}
	responses := make([]resTopic, 0, len(rs))

	for _, record := range rs {
		msg := pubsub.Message{
			OrderingKey: string(record.OrderingKey),
			Data:        record.Value,
			Attributes:  make(map[string]string),
		}

		if meta, ok := queuecontext.MetadataFromContext(ctx); ok {
			for k, v := range meta {
				msg.Attributes[k] = v
			}
		}

		publisher, err := p.getPublisher(record.Topic)
		if err != nil {
			return fmt.Errorf("pubsublite: failed to get publisher: %w", err)
		}
		if !p.cfg.Sync {
			ctx = queuecontext.DetachedContext(ctx)
		}
		responses = append(responses, resTopic{
			// NOTE(marclop) producer.Publish() is completely asynchronous and
			// doesn't use the context. If/when the pubsublite library supports
			// instrumentation, the context will be useful to propagate traces.
			// This is accurate as of pubsublite@v1.7.0
			response: publisher.Publish(ctx, &msg),
			topic:    record.Topic,
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

func (p *Producer) getPublisher(topic apmqueue.Topic) (pubsubabs.Publisher, error) {
	if v, ok := p.producers.Load(topic); ok {
		return v.(pubsubabs.Publisher), nil
	}
	pub, err := p.newPublisher(topic)
	if err != nil {
		return nil, fmt.Errorf(
			"pubsublite: failed creating publisher client for topic %s: %w",
			topic, err,
		)
	}
	publisher := pubsubabs.Wrap(pub)
	publisher = telemetry.NewProducer(publisher, p.tracer, p.metrics, []attribute.KeyValue{
		semconv.MessagingDestinationNameKey.String(string(topic)),
		semconv.CloudRegion(p.cfg.Region),
		semconv.CloudAccountID(p.cfg.Project),
	})
	if existing, ok := p.producers.LoadOrStore(topic, publisher); ok {
		// Another goroutine created the publisher concurrently, so close the
		// one we just created.
		publisher.Stop()
		return existing.(pubsubabs.Publisher), nil
	}
	return publisher, nil
}

func (p *Producer) newPublisher(topic apmqueue.Topic) (*pscompat.PublisherClient, error) {
	// TODO(marclop) connection pools:
	// https://pkg.go.dev/cloud.google.com/go/pubsublite#hdr-gRPC_Connection_Pools
	settings := pscompat.PublishSettings{
		// TODO(marclop) tweak producing settings, to cap memory use, trying
		// to size for good performance. It may be desireable to provide a
		// maximum memory usage for this component and size accordingly.
		// The number of topics should be taken into account since it creates
		// a publisher client per topic.
	}
	return pscompat.NewPublisherClientWithSettings(context.Background(),
		p.formatTopicPath(topic),
		settings, p.cfg.ClientOptions...,
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

// Healthy is a noop at the moment.
// TODO(marclop) range over the producers, call .Error(), if any returns
// an error, return it.
func (p *Producer) Healthy(ctx context.Context) error {
	return nil
}

func (p *Producer) formatTopicPath(topic apmqueue.Topic) string {
	return fmt.Sprintf(
		"projects/%s/locations/%s/topics/%s%s",
		p.cfg.Project, p.cfg.Region, p.cfg.namespacePrefix(), topic,
	)
}
