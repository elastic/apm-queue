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
	"strings"

	"github.com/elastic/apm-data/model"
)

const (
	// AtMostOnceDeliveryType acknowledges the message as soon as it's received
	// and decoded, without waiting for the message to be processed.
	AtMostOnceDeliveryType DeliveryType = iota
	// AtLeastOnceDeliveryType acknowledges the message after it has been
	// processed. It may or may not create duplicates, depending on how batches
	// are processed by the underlying model.BatchProcessor.
	AtLeastOnceDeliveryType
)

// DeliveryType for the consumer. For more details See the supported DeliveryTypes.
type DeliveryType uint8

// Consumer wraps the implementation details of the consumer implementation.
type Consumer interface {
	// Run executes the consumer in a blocking manner.
	Run(ctx context.Context) error
	// Healthy returns an error if the consumer isn't healthy.
	Healthy(ctx context.Context) error
	// Close closes the consumer.
	Close() error
}

// Producer wraps the producer implementation details
type Producer interface {
	model.BatchProcessor
	// Healthy returns an error if the producer isn't healthy.
	Healthy(ctx context.Context) error
	// Close closes the producer.
	Close() error
}

// Subscription represents a topic subscription. This is roughly
// equivalent to a Kafka topic consumer group.
type Subscription struct {
	// Name holds the name of the subscription.
	Name string

	// Topic holds the name of the subscribed topic.
	Topic Topic
}

// Topic represents a destination topic where to produce a message/record.
type Topic string

// TopicRouter is used to determine the destination topic for an model.APMEvent.
type TopicRouter func(event model.APMEvent) Topic

// NewEventTypeTopicRouter returns a topic router which routes events based on
// an optional prefix, and the event.Processor.Event field for an event.
func NewEventTypeTopicRouter(prefix string) TopicRouter {
	txTopic := joinPrefix(prefix, model.TransactionProcessor.Event)
	spanTopic := joinPrefix(prefix, model.SpanProcessor.Event)
	errorTopic := joinPrefix(prefix, model.ErrorProcessor.Event)
	metricTopic := joinPrefix(prefix, model.MetricsetProcessor.Event)
	logTopic := joinPrefix(prefix, model.LogProcessor.Event)
	return func(event model.APMEvent) Topic {
		switch event.Processor {
		case model.TransactionProcessor:
			return Topic(txTopic)
		case model.SpanProcessor:
			return Topic(spanTopic)
		case model.ErrorProcessor:
			return Topic(errorTopic)
		case model.MetricsetProcessor:
			return Topic(metricTopic)
		case model.LogProcessor:
			return Topic(logTopic)
		default:
			return "unknown"
		}
	}
}

func joinPrefix(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return strings.Join([]string{prefix, name}, ".")
}
