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
	"testing"

	"github.com/stretchr/testify/assert"

	apmqueue "github.com/elastic/apm-queue"
)

func TestNewProducer(t *testing.T) {
	_, err := NewProducer(ProducerConfig{})
	assert.Error(t, err)
}

func TestTopicString(t *testing.T) {
	tests := []struct {
		Project string
		Region  string
		Topic   apmqueue.Topic
		want    string
	}{
		{
			Topic:   "topic1",
			Region:  "us-east1",
			Project: "aproject",

			want: "projects/aproject/locations/us-east1/topics/topic1",
		},
		{
			Topic:   "topic2",
			Region:  "us-west2",
			Project: "anotherproject",

			want: "projects/anotherproject/locations/us-west2/topics/topic2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, formatTopic(tt.Project, tt.Region, tt.Topic))
		})
	}
}
