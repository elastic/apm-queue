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
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/twmb/franz-go/pkg/kgo"

	apmqueue "github.com/elastic/apm-queue/v2"
	"github.com/elastic/apm-queue/v2/queuecontext"
)

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
	CommonConfig

	// MaxBufferedRecords sets the max amount of records the client will buffer
	MaxBufferedRecords int

	// ProducerBatchMaxBytes upper bounds the size of a record batch
	ProducerBatchMaxBytes int32

	// ManualFlushing disables auto-flushing when producing.
	ManualFlushing bool

	// Sync can be used to indicate whether production should be synchronous.
	Sync bool

	// CompressionCodec specifies a list of compression codecs.
	// See kgo.ProducerBatchCompression for more details.
	//
	// If CompressionCodec is empty, then the default will be set
	// based on $KAFKA_PRODUCER_COMPRESSION_CODEC, which should be
	// a comma-separated list of codec preferences from the list:
	//
	//   [none, gzip, snappy, lz4, zstd]
	//
	// If $KAFKA_PRODUCER_COMPRESSION_CODEC is not specified, then
	// the default behaviour of franz-go is to use [snappy, none].
	CompressionCodec []CompressionCodec
}

// finalize ensures the configuration is valid, setting default values from
// environment variables as described in doc comments, returning an error if
// any configuration is invalid.
func (cfg *ProducerConfig) finalize() error {
	var errs []error
	if err := cfg.CommonConfig.finalize(); err != nil {
		errs = append(errs, err)
	}
	if cfg.MaxBufferedRecords < 0 {
		errs = append(errs, fmt.Errorf("kafka: max buffered records cannot be negative: %d", cfg.MaxBufferedRecords))
	}
	if cfg.ProducerBatchMaxBytes < 0 {
		errs = append(errs, fmt.Errorf("kafka: producer batch max bytes cannot be negative: %d", cfg.ProducerBatchMaxBytes))
	}
	if len(cfg.CompressionCodec) == 0 {
		if v := os.Getenv("KAFKA_PRODUCER_COMPRESSION_CODEC"); v != "" {
			names := strings.Split(v, ",")
			codecs := make([]CompressionCodec, 0, len(names))
			for _, name := range names {
				switch name {
				case "none":
					codecs = append(codecs, NoCompression())
				case "gzip":
					codecs = append(codecs, GzipCompression())
				case "snappy":
					codecs = append(codecs, SnappyCompression())
				case "lz4":
					codecs = append(codecs, Lz4Compression())
				case "zstd":
					codecs = append(codecs, ZstdCompression())
				default:
					errs = append(errs, fmt.Errorf("kafka: unknown codec %q", name))
				}
			}
			cfg.CompressionCodec = codecs
		}
	}
	return errors.Join(errs...)
}

var _ apmqueue.Producer = &Producer{}

// Producer publishes events to Kafka. Implements the Producer interface.
type Producer struct {
	cfg    ProducerConfig
	client *kgo.Client
	tracer trace.Tracer

	mu sync.RWMutex
}

// NewProducer returns a new Producer with the given config.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if err := cfg.finalize(); err != nil {
		return nil, fmt.Errorf("kafka: invalid producer config: %w", err)
	}
	var opts []kgo.Opt
	if len(cfg.CompressionCodec) > 0 {
		opts = append(opts, kgo.ProducerBatchCompression(cfg.CompressionCodec...))
	}
	if cfg.MaxBufferedRecords != 0 {
		opts = append(opts, kgo.MaxBufferedRecords(cfg.MaxBufferedRecords))
	}
	if cfg.ProducerBatchMaxBytes != 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(cfg.ProducerBatchMaxBytes))
	}
	if cfg.ManualFlushing {
		opts = append(opts, kgo.ManualFlushing())
	}
	client, err := cfg.newClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed creating producer: %w", err)
	}
	return &Producer{
		cfg:    cfg,
		client: client,
		tracer: cfg.tracerProvider().Tracer("kafka"),
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
	if err := p.client.Flush(context.Background()); err != nil {
		return fmt.Errorf("cannot flush on close: %w", err)
	}
	p.client.Close()
	return nil
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
	ctx, span := p.tracer.Start(ctx, "producer.Produce", trace.WithAttributes(
		attribute.Bool("sync", p.cfg.Sync),
		attribute.Int("record.count", len(rs)),
	))
	defer span.End()

	if len(rs) == 0 {
		return nil
	}

	// Take a read lock to prevent Close from closing the client
	// while we're attempting to produce records.
	p.mu.RLock()
	defer p.mu.RUnlock()

	var headers []kgo.RecordHeader
	if m, ok := queuecontext.MetadataFromContext(ctx); ok {
		headers = make([]kgo.RecordHeader, 0, len(m))
		for k, v := range m {
			headers = append(headers, kgo.RecordHeader{
				Key: k, Value: []byte(v),
			})
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(rs))
	if !p.cfg.Sync {
		ctx = queuecontext.DetachedContext(ctx)
	}
	namespacePrefix := p.cfg.namespacePrefix()
	for _, record := range rs {
		kgoRecord := &kgo.Record{
			Headers: headers,
			Topic:   fmt.Sprintf("%s%s", namespacePrefix, record.Topic),
			Key:     record.OrderingKey,
			Value:   record.Value,
		}
		p.client.Produce(ctx, kgoRecord, func(r *kgo.Record, err error) {
			defer wg.Done()
			// kotel already marks spans as errors. No need to handle it here.
			if err != nil {
				p.cfg.Logger.Error("failed producing message",
					zap.Error(err),
					zap.String("topic", strings.TrimPrefix(r.Topic, namespacePrefix)),
					zap.Int64("offset", r.Offset),
					zap.Int32("partition", r.Partition),
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
func (p *Producer) Healthy(ctx context.Context) error {
	if err := p.client.Ping(ctx); err != nil {
		return fmt.Errorf("health probe: %w", err)
	}
	return nil
}
