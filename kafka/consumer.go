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
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	"github.com/elastic/apm-queue/encoding"
	"github.com/elastic/apm-queue/queuecontext"
)

// ConsumerConfig defines the configuration for the Kafka consumer.
type ConsumerConfig struct {
	// Brokers is the list of kafka brokers used to seed the Kafka client.
	Brokers []string
	// Topics that the consumer will consume messages from
	Topics []string
	// GroupID to join as part of the consumer group.
	GroupID string
	// ClientID to use when connecting to Kafka. This is used for logging
	// and client identification purposes.
	ClientID string
	// Version is the software version to use in the Kafka client. This is
	// useful since it shows up in Kafka metrics and logs.
	Version string
	// Codec for a specific encoding.
	Codec encoding.Codec

	// Logger to use for any errors.
	Logger *zap.Logger
	// Processor that will be used to process each event individually.
	Processor model.BatchProcessor
}

// Validate ensures the configuration is valid, otherwise, returns an error.
func (cfg ConsumerConfig) Validate() error {
	var errs []error
	if len(cfg.Brokers) == 0 {
		errs = append(errs, errors.New("kafka: at least one broker must be set"))
	}
	if len(cfg.Topics) == 0 {
		errs = append(errs, errors.New("kafka: at least one topic must be set"))
	}
	if cfg.GroupID == "" {
		errs = append(errs, errors.New("kafka: consumer GroupID must be set"))
	}
	if cfg.Codec == nil {
		errs = append(errs, errors.New("kafka: codec must be set"))
	}
	if cfg.Logger == nil {
		errs = append(errs, errors.New("kafka: logger must be set"))
	}
	if cfg.Processor == nil {
		errs = append(errs, errors.New("kafka: processor must be set"))
	}
	return errors.Join(errs...)
}

// Consumer wraps a Kafka consumer and the consumption implementation details.
type Consumer struct {
	mu     sync.RWMutex
	client *kgo.Client
	cfg    ConsumerConfig
}

// NewConsumer creates a new instance of a Consumer.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.WithLogger(kzap.New(cfg.Logger)),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
		if cfg.Version != "" {
			opts = append(opts, kgo.SoftwareNameAndVersion(
				cfg.ClientID, cfg.Version,
			))
		}
	}
	// TODO(marclop) block on re-balances, auto-commit high watermarks.
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	// Issue a metadata refresh request on construction, so the broker list is
	// populated.
	client.ForceMetadataRefresh()
	consumer := Consumer{
		cfg:    cfg,
		client: client,
	}
	return &consumer, nil
}

// Close closes the consumer.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.client.Close()
	return nil
}

// Run executes the consumer in a blocking manner.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		if err := c.fetch(ctx); err != nil {
			return err
		}
	}
}

func (c *Consumer) fetch(ctx context.Context) error {
	// NOTE(marclop) this is pretty naive consuming, to maximize throughput,
	// it's best to use one goroutine per partition, but that requires more
	// state management and blocking when rebalances happen.
	c.mu.RLock()
	defer c.mu.RUnlock()
	fetches := c.client.PollFetches(ctx)
	if fetches.IsClientClosed() || errors.Is(fetches.Err0(), context.Canceled) {
		return context.Canceled // Client closed or context cancelled.
	}
	fetches.EachError(func(t string, p int32, err error) {
		c.cfg.Logger.Error("consumer fetches returned error",
			zap.Error(err), zap.String("topic", t), zap.Int32("partition", p),
		)
	})
	fetches.EachRecord(func(msg *kgo.Record) {
		ctx := context.Background()
		for _, h := range msg.Headers {
			if h.Key == "project_id" {
				ctx = queuecontext.WithProject(ctx, string(h.Value))
				break
			}
		}
		var event model.APMEvent
		if err := c.cfg.Codec.Decode(msg.Value, &event); err != nil {
			// TODO(marclop) DLQ?
			c.cfg.Logger.Error("unable to unmarshal json into model.APMEvent",
				zap.Error(err),
				zap.String("topic", msg.Topic),
				zap.ByteString("message.value", msg.Value),
				zap.Int64("offset", msg.Offset),
				zap.Int32("partition", int32(msg.Partition)),
			)
			return
		}
		batch := model.Batch{event}
		if err := c.cfg.Processor.ProcessBatch(ctx, &batch); err != nil {
			c.cfg.Logger.Error("unable to process event",
				zap.Error(err),
				zap.String("topic", msg.Topic),
				zap.Int64("offset", msg.Offset),
				zap.Int32("partition", int32(msg.Partition)),
			)
			return
		}
	})
	return nil
}

// Healthy returns an error if the Kafka active broker length dips below 1.
func (c *Consumer) Healthy() error {
	if brokers := c.client.DiscoveredBrokers(); len(brokers) < 1 {
		return fmt.Errorf("number of brokers below 1")
	}
	return nil
}
