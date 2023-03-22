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
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/queuecontext"
)

// Decoder decodes a []byte into a model.APMEvent
type Decoder interface {
	// Decode decodes an encoded model.APM Event into its struct form.
	Decode([]byte, *model.APMEvent) error
}

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
	// Decoder holds an encoding.Decoder for decoding events.
	Decoder Decoder
	// MaxPollRecords defines an upper bound to the number of records that can
	// be polled on a single fetch. If MaxPollRecords <= 0, defaults to 100.
	//
	// It is best to keep the number of polled records small or the consumer
	// risks being forced out of the group if it exceeds rebalance.timeout.ms.
	MaxPollRecords int
	// Delivery mechanism to use to acknowledge the messages.
	// AtMostOnceDeliveryType and AtLeastOnceDeliveryType are supported.
	// If not set, it defaults to apmqueue.AtMostOnceDeliveryType.
	Delivery apmqueue.DeliveryType
	// Logger to use for any errors.
	Logger *zap.Logger
	// Processor that will be used to process each event individually.
	// It is recommended to keep the synchronous processing fast and below the
	// rebalance.timeout.ms setting in Kafka.
	//
	// The processing time of each processing cycle can be calculated as:
	// record.process.time * MaxPollRecords.
	Processor model.BatchProcessor
	// SASL configures the kgo.Client to use SASL authorization.
	SASL sasl.Mechanism
	// TLS configures the kgo.Client to use TLS for authentication.
	TLS *tls.Config
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
	if cfg.Decoder == nil {
		errs = append(errs, errors.New("kafka: decoder must be set"))
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
// Consumes each partition in a dedicated goroutine.
type Consumer struct {
	mu       sync.RWMutex
	client   *kgo.Client
	cfg      ConsumerConfig
	consumer *consumer
}

// NewConsumer creates a new instance of a Consumer. The consumer will read from
// each partition concurrently by using a dedicated goroutine per partition.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka: invalid consumer config: %w", err)
	}
	consumer := &consumer{
		consumers: make(map[topicPartition]partitionConsumer),
		processor: cfg.Processor,
		logger:    cfg.Logger.Named("partition"),
		decoder:   cfg.Decoder,
		delivery:  cfg.Delivery,
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.WithLogger(kzap.New(cfg.Logger.Named("kafka"))),
		// If a rebalance happens while the client is polling, the consumed
		// records may belong to a partition which has been reassigned to a
		// different consumer int he group. To avoid this scenario, Polls will
		// block rebalances of partitions which would be lost, and the consumer
		// MUST manually call `AllowRebalance`.
		kgo.BlockRebalanceOnPoll(),
		kgo.DisableAutoCommit(),
		// Assign concurrent consumer callbacks to ensure consuming starts
		// for newly assigned partitions, and consuming ceases from lost or
		// revoked partitions.
		kgo.OnPartitionsAssigned(consumer.assigned),
		kgo.OnPartitionsLost(consumer.lost),
		kgo.OnPartitionsRevoked(consumer.lost),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
		if cfg.Version != "" {
			opts = append(opts, kgo.SoftwareNameAndVersion(
				cfg.ClientID, cfg.Version,
			))
		}
	}
	if cfg.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(cfg.TLS.Clone()))
	}
	if cfg.SASL != nil {
		opts = append(opts, kgo.SASL(cfg.SASL))
	}
	if cfg.MaxPollRecords <= 0 {
		cfg.MaxPollRecords = 100
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed creating kafka consumer: %w", err)
	}
	// Issue a metadata refresh request on construction, so the broker list is
	// populated.
	client.ForceMetadataRefresh()
	return &Consumer{
		cfg:      cfg,
		client:   client,
		consumer: consumer,
	}, nil
}

// Close closes the consumer.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Since we take the full lock when closing, there's no need to explicitly
	// allow rebalances since polls aren't concurrent with Close().
	c.client.Close()
	c.consumer.wg.Wait() // Wait for all the goroutines to exit.
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

// fetch polls the Kafka broker for new records up to cfg.MaxPollRecords.
// Any errors returned by fetch should be considered fatal.
func (c *Consumer) fetch(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	fetches := c.client.PollRecords(ctx, c.cfg.MaxPollRecords)
	defer c.client.AllowRebalance()

	if fetches.IsClientClosed() {
		return fmt.Errorf("client is closed: %w", context.Canceled)
	}
	if errors.Is(fetches.Err0(), context.Canceled) {
		return fmt.Errorf("context canceled: %w", fetches.Err0())
	}
	switch c.cfg.Delivery {
	case apmqueue.AtLeastOnceDeliveryType:
		// Committing the processed records happens on each partition consumer.
	case apmqueue.AtMostOnceDeliveryType:
		// Commit the fetched record offsets as soon as we've polled them.
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			// If the commit fails, then return immediately and any uncommitted
			// records will be re-delivered in time. Otherwise, records may be
			// processed twice.
			return nil
		}
		// Allow rebalancing now that we have committed offsets, preventing
		// another consumer from reprocessing the records.
		c.client.AllowRebalance()
	}
	fetches.EachError(func(t string, p int32, err error) {
		c.cfg.Logger.Error("consumer fetches returned error",
			zap.Error(err), zap.String("topic", t), zap.Int32("partition", p),
		)
	})
	// Send partition records to be processed by its dedicated goroutine.
	fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
		if len(ftp.Records) == 0 {
			return
		}
		tp := topicPartition{topic: ftp.Topic, partition: ftp.Partition}
		select {
		case c.consumer.consumers[tp].records <- ftp.Records:
		// When using AtMostOnceDelivery, if the context is cancelled between
		// PollRecords and this line, the events will be lost.
		// NOTE(marclop) Add a shutdown timer so that there's a grace period
		// to allow the events to be processed before they're lost.
		case <-ctx.Done():
			if c.cfg.Delivery == apmqueue.AtMostOnceDeliveryType {
				c.cfg.Logger.Warn(
					"data loss: context cancelled after records were committed",
					zap.String("topic", ftp.Topic),
					zap.Int32("partition", ftp.Partition),
					zap.Int64("offset", ftp.HighWatermark),
					zap.Int("records", len(ftp.Records)),
				)
			}
		}
	})
	return nil
}

// Healthy returns an error if the Kafka client fails to reach a discovered
// broker.
func (c *Consumer) Healthy() error {
	if err := c.client.Ping(context.Background()); err != nil {
		return fmt.Errorf("health probe: %w", err)
	}
	return nil
}

// consumer wraps partitionConsumers and exposes the necessary callbacks
// to use when partitions are reassigned.
type consumer struct {
	mu        sync.Mutex
	wg        sync.WaitGroup
	consumers map[topicPartition]partitionConsumer
	processor model.BatchProcessor
	logger    *zap.Logger
	decoder   Decoder
	delivery  apmqueue.DeliveryType
}

type topicPartition struct {
	topic     string
	partition int32
}

// assigned must be set as a kgo.OnPartitionsAssigned callback. Ensuring all
// assigned partitions to this consumer process received records.
func (c *consumer) assigned(_ context.Context, client *kgo.Client, assigned map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			c.wg.Add(1)
			pc := partitionConsumer{
				records:   make(chan []*kgo.Record),
				processor: c.processor,
				logger:    c.logger,
				decoder:   c.decoder,
				client:    client,
				delivery:  c.delivery,
			}
			go func(topic string, partition int32) {
				defer c.wg.Done()
				pc.consume(topic, partition)
			}(topic, partition)
			tp := topicPartition{partition: partition, topic: topic}
			c.consumers[tp] = pc
		}
	}
}

// lost must be set as a kgo.OnPartitionsLost and kgo.OnPartitionsReassigned
// callbacks. Ensures that partitions that are lost (see kgo.OnPartitionsLost
// for more details) or reassigned (see kgo.OnPartitionsReassigned for more
// details) have their partition consumer stopped.
// This callback must finish within the re-balance timeout.
func (c *consumer) lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for topic, partitions := range lost {
		for _, partition := range partitions {
			tp := topicPartition{topic: topic, partition: partition}
			pc := c.consumers[tp]
			delete(c.consumers, tp)
			close(pc.records)
		}
	}
}

type partitionConsumer struct {
	client    *kgo.Client
	records   chan []*kgo.Record
	processor model.BatchProcessor
	logger    *zap.Logger
	decoder   Decoder
	delivery  apmqueue.DeliveryType
}

// consume processed the records from a topic and partition. Calling consume
// more than once will cause a panic.
func (pc partitionConsumer) consume(topic string, partition int32) {
	logger := pc.logger.With(
		zap.String("topic", topic),
		zap.Int32("partition", partition),
	)
	for records := range pc.records {
		// Store the last processed record. Default to -1 for cases where
		// only the first record is received.
		last := -1
	recordLoop:
		for i, msg := range records {
			meta := make(map[string]string)
			for _, h := range msg.Headers {
				meta[h.Key] = string(h.Value)
			}
			var event model.APMEvent
			if err := pc.decoder.Decode(msg.Value, &event); err != nil {
				logger.Error("unable to decode message.Value into model.APMEvent",
					zap.Error(err),
					zap.ByteString("message.value", msg.Value),
					zap.Int64("offset", msg.Offset),
					zap.Any("headers", meta),
				)
				// TODO(marclop) DLQ? The decoding has failed, re-delivery
				// may cause the same error. Discard the event for now.
				continue
			}
			ctx := queuecontext.WithMetadata(context.Background(), meta)
			batch := model.Batch{event}
			if err := pc.processor.ProcessBatch(ctx, &batch); err != nil {
				logger.Error("unable to process event",
					zap.Error(err),
					zap.Int64("offset", msg.Offset),
					zap.Any("headers", meta),
				)
				switch pc.delivery {
				case apmqueue.AtLeastOnceDeliveryType:
					// Exit the loop and commit the last processed offset
					// (if any). This ensures events which haven't been
					// processed are re-delivered, but those that have, are
					// committed.
					break recordLoop
				case apmqueue.AtMostOnceDeliveryType:
					// Events which can't be processed, are lost.
					continue
				}
			}
			last = i
		}
		// Only commit the records when at least a record has been processed
		// with AtLeastOnceDeliveryType.
		if pc.delivery == apmqueue.AtLeastOnceDeliveryType && last >= 0 {
			lastRecord := records[last]
			err := pc.client.CommitRecords(context.Background(), lastRecord)
			if err != nil {
				logger.Error("unable to commit records",
					zap.Error(err),
					zap.Int64("offset", lastRecord.Offset),
				)
			} else if len(records) > 0 {
				logger.Info("committed",
					zap.Int64("offset", lastRecord.Offset),
				)
			}
		}
	}
}
