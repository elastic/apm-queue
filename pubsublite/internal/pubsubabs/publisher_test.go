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
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

func TestWrap(t *testing.T) {
	var publisher mockPublisher
	wrapped := Wrap(&publisher)

	assert.False(t, publisher.publishCalled)
	assert.False(t, publisher.errorCalled)
	assert.False(t, publisher.stopCalled)

	assert.NoError(t, wrapped.Error())
	assert.True(t, publisher.errorCalled)

	wrapped.Stop()
	assert.True(t, publisher.stopCalled)

	assert.Equal(t,
		wrapped.Publish(context.Background(), &pubsub.Message{}),
		&pubsub.PublishResult{},
	)
	assert.True(t, publisher.publishCalled)
}

type mockPublisher struct {
	errorCalled   bool
	stopCalled    bool
	publishCalled bool
}

// Error returns the producer stop error.
func (p *mockPublisher) Error() error {
	p.errorCalled = true
	return nil
}

// Stop stops the producer.
func (p *mockPublisher) Stop() {
	p.stopCalled = true
}

// Publish wraps the pubsublite publish.
func (p *mockPublisher) Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult {
	p.publishCalled = true
	return &pubsub.PublishResult{}
}
