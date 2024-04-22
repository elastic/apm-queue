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

package main

import (
	"context"
	"sync"

	apmqueue "github.com/elastic/apm-queue/v2"
)

type metaConsumer struct {
	consumers []apmqueue.Consumer
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// Run executes the consumer in a blocking manner. Returns
// ErrConsumerAlreadyRunning when it has already been called.
func (mc *metaConsumer) Run(ctx context.Context) error {
	mc.wg.Add(len(mc.consumers))
	ctx, mc.cancel = context.WithCancel(ctx)
	for _, c := range mc.consumers {
		go func(c apmqueue.Consumer) {
			defer mc.wg.Done()
			c.Run(ctx)
		}(c)
	}
	mc.wg.Wait()
	return nil
}

// Healthy returns an error if the consumer isn't healthy.
func (mc *metaConsumer) Healthy(context.Context) error {
	return nil
}

// Close closes the consumer.
func (mc *metaConsumer) Close() error {
	mc.cancel()
	mc.wg.Wait()
	return nil
}
