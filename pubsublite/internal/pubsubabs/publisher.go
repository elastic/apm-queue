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

// Package pubsubabs provides an abstraction layer over the `pubsub`
// PublisherClient types to allow testing.
package pubsubabs

import (
	"context"

	"cloud.google.com/go/pubsub"
)

// ErrorStop defines the Stop and Error methods.
type ErrorStop interface {
	// Error returns the error that caused the publisher client to terminate. The
	// error returned here may contain more context than PublishResult errors. The
	// return value may be nil if Stop() was called.
	Error() error
	// Stop sends all remaining published messages and closes publish streams.
	// Returns once all outstanding messages have been sent or have failed to be
	// sent. Stop should be called when the client is no longer required.
	Stop()
}

// PublisherClient represents a PubSub PublisherClient.
type PublisherClient interface {
	ErrorStop
	Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult
}

// Publisher defines an abstracted interface that can be used for mocking or
// testing purposes. It wraps a Publisher client.
type Publisher interface {
	ErrorStop
	// Publish publishes `msg` to the topic asynchronously. Messages are batched and
	// sent according to the client's PublishSettings. Publish never blocks.
	//
	// Publish returns a non-nil PublishResult which will be ready when the
	// message has been sent (or has failed to be sent) to the server. Retry-able
	// errors are automatically handled. If a PublishResult returns an error, this
	// indicates that the publisher client encountered a fatal error and can no
	// longer be used. Fatal errors should be manually inspected and the cause
	// resolved. A new publisher client instance must be created to republish failed
	// messages.
	//
	// Once Stop() has been called or the publisher client has failed permanently
	// due to an error, future calls to Publish will immediately return a
	// PublishResult with error ErrPublisherStopped.
	//
	// Error() returns the error that caused the publisher client to terminate and
	// may contain more context than the error returned by PublishResult.
	Publish(ctx context.Context, msg *pubsub.Message) PublishResult
}

// Wrap returns an abstracted Publisher from a PublisherClient.
func Wrap(c PublisherClient) Publisher { return &pubSubClient{c} }

type pubSubClient struct {
	PublisherClient
}

func (p *pubSubClient) Publish(ctx context.Context, msg *pubsub.Message) PublishResult {
	return p.PublisherClient.Publish(ctx, msg)
}

// PublishResult abstracts the pubsub.PublishResult type.
type PublishResult interface {
	// Get returns the server-generated message ID and/or error result of a Publish call.
	// Get blocks until the Publish call completes or the context is done.
	Get(ctx context.Context) (serverID string, err error)
	// Ready returns a channel that is closed when the result is ready.
	// When the Ready channel is closed, Get is guaranteed not to block.
	Ready() <-chan struct{}
}

// IPublishResult is a copy from the internal pubsub packgage. All the code
// below has been copied from "cloud.google.com/go/internal/pubsub"
// Copyright 2020 Google LLC
type IPublishResult struct {
	err      error
	ready    chan struct{}
	serverID string
}

// Ready returns a channel that is closed when the result is ready.
// When the Ready channel is closed, Get is guaranteed not to block.
func (r *IPublishResult) Ready() <-chan struct{} { return r.ready }

// Get returns the server-generated message ID and/or error result of a Publish call.
// Get blocks until the Publish call completes or the context is done.
func (r *IPublishResult) Get(ctx context.Context) (serverID string, err error) {
	// If the result is already ready, return it even if the context is done.
	select {
	case <-r.Ready():
		return r.serverID, r.err
	default:
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-r.Ready():
		return r.serverID, r.err
	}
}

// NewPublishResult creates a PublishResult.
func NewPublishResult() *IPublishResult {
	return &IPublishResult{ready: make(chan struct{})}
}

// SetPublishResult sets the server ID and error for a publish result and closes
// the Ready channel.
func SetPublishResult(r *IPublishResult, sid string, err error) {
	r.serverID = sid
	r.err = err
	close(r.ready)
}
