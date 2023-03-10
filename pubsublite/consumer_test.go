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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConsumer(t *testing.T) {
	t.Run("empty config", func(t *testing.T) {
		_, err := NewConsumer(context.Background(), ConsumerConfig{})
		assert.Error(t, err)
	})
	t.Run("invalid delivery type", func(t *testing.T) {
		_, err := NewConsumer(context.Background(), ConsumerConfig{
			Delivery: 100,
		})
		assert.Error(t, err)
		assert.ErrorContains(t, err, "pubsublite: delivery is not valid")
	})
}

func TestSubscriptionString(t *testing.T) {
	tests := []struct {
		Project string
		Region  string
		Name    string
		want    string
	}{
		{
			Name:    "topic1",
			Region:  "us-east1",
			Project: "aproject",

			want: "projects/aproject/locations/us-east1/subscriptions/topic1",
		},
		{
			Name:    "topic2",
			Region:  "us-west2",
			Project: "anotherproject",

			want: "projects/anotherproject/locations/us-west2/subscriptions/topic2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			subs := Subscription{
				Project: tt.Project,
				Region:  tt.Region,
				Name:    tt.Name,
			}
			assert.Equal(t, tt.want, subs.String())
		})
	}
}
