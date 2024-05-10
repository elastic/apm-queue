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

// Package queuecontext provides convenient wrappers for storing and
// accessing a stored metadata.
package queuecontext

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetachedContext(t *testing.T) {
	// Ensures that the detached context isn't cancelled and any values are
	// still accessible.
	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx := WithMetadata(cancelCtx, map[string]string{"a": "b"})
	detached := DetachedContext(ctx)

	// Cancel the context
	cancel()
	select {
	case <-detached.Done():
		t.Fatal("context shouldn't be cancelled")
	case <-time.After(time.Millisecond):
	}

	meta, ok := MetadataFromContext(detached)
	require.True(t, ok)
	assert.Equal(t, map[string]string{"a": "b"}, meta)
}

func TestEnrichedContext(t *testing.T) {
	ctx := context.Background()
	ctx = WithMetadata(ctx, map[string]string{"a": "b"})
	ctx = Enrich(ctx, "partition_id", "1")

	meta, ok := MetadataFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, map[string]string{"a": "b", "partition": "1"}, meta)
}
