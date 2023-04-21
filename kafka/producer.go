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

package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/plugin/kotel"
	"github.com/twmb/franz-go/plugin/kzap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/queuecontext"
)

// Encoder encodes a model.APMEvent to a []byte
type Encoder interface {
	// Encode accepts a model.APMEvent and returns the encoded representation.
	Encode(model.APMEvent) ([]byte, error)
}

// CompressionCodec configures how records are compressed before being sent.
// Type alias to kgo.CompressionCodec.
type CompressionCodec = kgo.CompressionCodec

// NoCompression is a compression option that avoids compression. This can
// always be used as a fallback compression.
func NoCompression() CompressionCodec { return kgo.NoCompression() }

// GzipCompression enables gzip compression with the default compression level.
func GzipCompression() CompressionCodec { return kgo.GzipCompression() }

// SnappyCompression enables snappy compression.
func SnappyCompression() CompressionCodec { return kgo.SnappyCompression() }

// Lz4Compression enables lz4 compression with the fastest compression level.
func Lz4Compression() CompressionCodec { return kgo.Lz4Compression() }

// ZstdCompression enables zstd compression with the default compression level.
func ZstdCompression() CompressionCodec { return kgo.ZstdCompression() }

// ProducerConfig holds configuration for publishing events to Kafka.
type ProducerConfig struct {
	// Brokers holds a slice of (host:port) addresses of the Kafka brokers
	// to which events should be published.
	Brokers []string

	// ClientID to use when connecting to Kafka. This is used for logging
	// and client identification purposes.
	ClientID string
	// Version is the software version to use in the Kafka client. This is
	// useful since it shows up in Kafka metrics and logs.
	Version string

	// Logger is used for logging producer errors.
	Logger *zap.Logger

	// Encoder holds an encoding.Encoder for encoding events.
	Encoder Encoder

	// Sync can be used to indicate whether production should be synchronous.
	Sync bool

	// TopicRouter returns the topic where an event should be produced.
	TopicRouter apmqueue.TopicRouter

	// SASL configures the kgo.Client to use SASL authorization.
	SASL sasl.Mechanism
	// TLS configures the kgo.Client to use TLS for authentication.
	// This option conflicts with Dialer. Only one can be used.
	TLS *tls.Config
	// CompressionCodec specifies a list of compression codecs.
	// See kgo.ProducerBatchCompression for more details.
	CompressionCodec []CompressionCodec
	// Dialer uses fn to dial addresses, overriding the default dialer that uses a
	// 10s dial timeout and no TLS (unless TLS option is set).
	//
	// The context passed to the dial function is the context used in the request
	// that caused the dial. If the request is a client-internal request, the
	// context is the context on the client itself (which is canceled when the
	// client is closed).
	Dialer func(ctx context.Context, network, address string) (net.Conn, error)

	// DisableTelemetry disables the OpenTelemetry hook
	DisableTelemetry bool
}

// Validate checks that cfg is valid, and returns an error otherwise.
func (cfg ProducerConfig) Validate() error {
	var err []error
	if len(cfg.Brokers) == 0 {
		err = append(err, errors.New("kafka: brokers cannot be empty"))
	}
	if cfg.Logger == nil {
		err = append(err, errors.New("kafka: logger cannot be nil"))
	}
	if cfg.Encoder == nil {
		err = append(err, errors.New("kafka: encoder cannot be nil"))
	}
	if cfg.TopicRouter == nil {
		err = append(err, errors.New("kafka: topic router must be set"))
	}
	if cfg.TLS != nil && cfg.Dialer != nil {
		err = append(err, errors.New("kafka: only one of TLS or Dialer can be set"))
	}
	return errors.Join(err...)
}

// Producer is a model.BatchProcessor that publishes events to Kafka.
type Producer struct {
	cfg    ProducerConfig
	client *kgo.Client
	tracer trace.Tracer

	mu sync.RWMutex
}

// NewProducer returns a new Producer with the given config.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka: invalid producer config: %w", err)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.WithLogger(kzap.New(cfg.Logger.Named("kafka"))),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
		if cfg.Version != "" {
			opts = append(opts, kgo.SoftwareNameAndVersion(
				cfg.ClientID, cfg.Version,
			))
		}
	}
	if cfg.Dialer != nil {
		opts = append(opts, kgo.Dialer(cfg.Dialer))
	} else if cfg.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(cfg.TLS.Clone()))
	}
	if cfg.SASL != nil {
		opts = append(opts, kgo.SASL(cfg.SASL))
	}
	if len(cfg.CompressionCodec) > 0 {
		opts = append(opts, kgo.ProducerBatchCompression(cfg.CompressionCodec...))
	}

	if !cfg.DisableTelemetry {
		kotelService := kotel.NewKotel()
		opts = append(opts, kgo.WithHooks(kotelService.Hooks()...))
	}
	tracer := otel.Tracer("kafka")

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed creating producer: %w", err)
	}
	// Issue a metadata refresh request on construction, so the broker list is
	// populated.
	client.ForceMetadataRefresh()

	return &Producer{
		cfg:    cfg,
		client: client,
		tracer: tracer,
	}, nil
}

// Close stops the producer
//
// This call is blocking and will cause all the underlying clients to stop
// producing. If producing is asynchronous, it'll block until all messages
// have been produced. After Close() is called, Producer cannot be reused.
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.client.Close()
	return nil
}

// ProcessBatch publishes the batch to the kafka topic inferred from the
// configured TopicRouter. If the Producer is synchronous, it waits until all
// messages have been produced to Kafka, otherwise, returns as soon as
// the messages have been stored in the producer's buffer.
func (p *Producer) ProcessBatch(ctx context.Context, batch *model.Batch) error {
	ctx, span := p.tracer.Start(ctx, "producer.ProcessBatch", trace.WithAttributes(
		attribute.Int("batch.size", len(*batch)),
	))

	// Take a read lock to prevent Close from closing the client
	// while we're attempting to produce records.
	p.mu.RLock()
	defer p.mu.RUnlock()

	var headers []kgo.RecordHeader
	if m, ok := queuecontext.MetadataFromContext(ctx); ok {
		for k, v := range m {
			headers = append(headers, kgo.RecordHeader{
				Key:   k,
				Value: []byte(v),
			})
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(*batch))
	for _, event := range *batch {
		record := &kgo.Record{
			Headers: headers,
			Topic:   string(p.cfg.TopicRouter(event)),
		}
		encoded, err := p.cfg.Encoder.Encode(event)
		if err != nil {
			err = fmt.Errorf("failed to encode event: %w", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return err
		}
		record.Value = encoded
		if !p.cfg.Sync {
			// Detach the context from its deadline or cancellation.
			ctx = queuecontext.DetachedContext(ctx)
		}
		p.client.Produce(ctx, record, func(msg *kgo.Record, err error) {
			defer func() {
				wg.Done()
				span.End()
			}()
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				p.cfg.Logger.Error("failed producing message",
					zap.Error(err),
					zap.String("topic", msg.Topic),
					zap.Int64("offset", msg.Offset),
					zap.Int32("partition", msg.Partition),
					zap.Any("headers", headers),
				)
			}
		})
	}
	if p.cfg.Sync {
		wg.Wait()
	}
	return nil
}

// Healthy returns an error if the Kafka client fails to reach a discovered
// broker.
func (p *Producer) Healthy() error {
	if err := p.client.Ping(context.Background()); err != nil {
		return fmt.Errorf("health probe: %w", err)
	}
	return nil
}
