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
