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

package pubsublite

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue"
)

func TestNewConsumer(t *testing.T) {
	t.Run("empty config", func(t *testing.T) {
		_, err := NewConsumer(context.Background(), ConsumerConfig{})
		assert.EqualError(t, err, "pubsublite: invalid consumer config: "+strings.Join([]string{
			"pubsublite: project must be set",
			"pubsublite: region must be set",
			"pubsublite: logger must be set",
			"pubsublite: at least one topic must be set",
			"pubsublite: consumer name must be set",
			"pubsublite: processor must be set",
		}, "\n"))
	})

	validConfig := func() ConsumerConfig {
		var validProcessor struct{ apmqueue.Processor }
		return ConsumerConfig{
			CommonConfig: CommonConfig{
				Project: "project_name",
				Region:  "region_name",
				Logger:  zap.NewNop(),
			},
			Processor:    validProcessor,
			Topics:       []apmqueue.Topic{"topic_name"},
			ConsumerName: "consumer_name",
		}
	}

	t.Run("invalid delivery type", func(t *testing.T) {
		config := validConfig()
		config.Delivery = 100
		_, err := NewConsumer(context.Background(), config)
		assert.Error(t, err)
		assert.EqualError(t, err, "pubsublite: invalid consumer config: pubsublite: delivery is not valid")
	})
}
