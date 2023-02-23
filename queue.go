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

// Package apmqueue provides an abstraction layer for producing and consuming
// model.Batch es from and to Kafka and GCP PubSub Lite.
package apmqueue

import (
	"context"
	"errors"
	"strings"

	"github.com/elastic/apm-data/model"
	"github.com/elastic/apm-queue/kafka"
	"github.com/elastic/apm-queue/pubsublite"
)

const (
	_ QueueType = iota
	// QueueTypeKafka defines the kafka queue type
	QueueTypeKafka QueueType = iota
	// QueueTypePubSubLite defines the PubSub Lite queue type.
	QueueTypePubSubLite QueueType = iota
)

// QueueType defines the type of queue to be used.
type QueueType uint8

func (t QueueType) String() string {
	switch t {
	case QueueTypeKafka:
		return "kafka"
	case QueueTypePubSubLite:
		return "pubsublite"
	default:
		return ""
	}
}

// Consumer wraps the implementation details of the consumer implementation.
type Consumer interface {
	// Run executes the consumer in a blocking manner.
	Run(ctx context.Context) error
	// Healthy returns an error if the consumer isn't healthy.
	Healthy() error
	// Close closes the consumer.
	Close() error
}

// Producer wraps the producer implementation details
type Producer interface {
	model.BatchProcessor
	// Healthy returns an error if the producer isn't healthy.
	Healthy() error
	// Close closes the producer.
	Close() error
}

// ErrUnsupportedQueueType is returned when the queue type is not in the list
// of supported queues.
var ErrUnsupportedQueueType = errors.New("invalid queue type")

// ParseQueueType returns the queue type
func ParseQueueType(t string) (QueueType, error) {
	switch strings.ToLower(t) {
	case "kafka":
		return QueueTypeKafka, nil
	case "pubsublite":
		return QueueTypePubSubLite, nil
	default:
		return 0, ErrUnsupportedQueueType
	}
}

// ConsumerConfig wraps the different adapter consumer configs
type ConsumerConfig struct {
	Type       QueueType
	Kafka      kafka.ConsumerConfig
	PubSubLite pubsublite.ConsumerConfig
}

// NewConsumer creates a new consumer of the specified QueueType.
func NewConsumer(cfg ConsumerConfig) (Consumer, error) {
	switch cfg.Type {
	case QueueTypeKafka:
		return kafka.NewConsumer(cfg.Kafka)
	case QueueTypePubSubLite:
		return pubsublite.NewConsumer(context.Background(), cfg.PubSubLite)
	}
	return nil, ErrUnsupportedQueueType
}

// ProducerConfig wraps the different adapter producer configs
type ProducerConfig struct {
	Type       QueueType
	Kafka      any // TODO(marclop)
	PubSubLite pubsublite.ProducerConfig
}

// NewProducer creates a new producer of the specified QueueType.
func NewProducer(cfg ProducerConfig) (Producer, error) {
	switch cfg.Type {
	case QueueTypeKafka:
		// return kafka.NewProducer(cfg.Kafka)
	case QueueTypePubSubLite:
		return pubsublite.NewProducer(context.Background(), cfg.PubSubLite)
	}
	return nil, ErrUnsupportedQueueType
}
