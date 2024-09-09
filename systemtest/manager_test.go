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

package systemtest

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apmqueue "github.com/elastic/apm-queue/v2"
	"github.com/elastic/apm-queue/v2/kafka"
)

func TestManagerCreateTopics(t *testing.T) {
	// This test covers:
	//  - Creating topics with a number of partitions
	//  - Updating the partition count
	//  - Updating the configuration
	t.Run("Kafka", func(t *testing.T) {
		manager := NewKafkaManager(t)
		cfg := kafka.TopicCreatorConfig{
			PartitionCount: 1,
			TopicConfigs: map[string]string{
				"retention.ms": strconv.FormatInt(time.Hour.Milliseconds(), 10),
			},
		}
		creator, err := manager.NewTopicCreator(cfg)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		letters := "abcdefghijk"
		topics := make([]apmqueue.Topic, 0, 5)
		for i := 0; i < 3; i++ {
			topics = append(topics, apmqueue.Topic(fmt.Sprintf("%s-%s",
				t.Name(), string(letters[i]),
			)))
		}
		topics = SuffixTopics(topics...)
		// Create topics for the first time.
		assert.NoError(t, creator.CreateTopics(ctx, topics...))

		// New creator with increased partitions
		cfg.PartitionCount = 4
		creator, err = manager.NewTopicCreator(cfg)
		require.NoError(t, err)
		// Update Partition count.
		assert.NoError(t, creator.CreateTopics(ctx, topics...))

		// New creator with increased retention
		cfg.TopicConfigs = map[string]string{
			"retention.ms": strconv.FormatInt(2*time.Hour.Milliseconds(), 10),
		}
		creator, err = manager.NewTopicCreator(cfg)
		require.NoError(t, err)
		// Update topic configuration.
		assert.NoError(t, creator.CreateTopics(ctx, topics...))
	})
}
