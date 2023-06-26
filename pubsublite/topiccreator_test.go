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
	"time"

	pubsublitepb "cloud.google.com/go/pubsublite/apiv1/pubsublitepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestManagerNewTopicCreator(t *testing.T) {
	_, commonConfig := newTestAdminService(t)
	_, monitoringClientOpts := newTestMetricService(t)
	manager, err := NewManager(ManagerConfig{CommonConfig: commonConfig, MonitoringClientOptions: monitoringClientOpts})
	require.NoError(t, err)
	defer manager.Close()

	_, err = manager.NewTopicCreator(TopicCreatorConfig{})
	require.Error(t, err)
	assert.EqualError(t, err, "pubsublite: invalid topic creator config: "+strings.Join([]string{
		"pubsublite: reservation must be set",
		"pubsublite: partition count must be greater than zero",
		"pubsublite: publish capacity must between 4 and 16, inclusive",
		"pubsublite: subscribe capacity must between 4 and 32, inclusive",
		"pubsublite: per-partition bytes must be at least 30GiB",
		"pubsublite: retention duration must be greater than zero",
	}, "\n"))

	creator, err := manager.NewTopicCreator(TopicCreatorConfig{
		Reservation:                "reservation_name",
		PartitionCount:             1,
		PublishCapacityMiBPerSec:   4,
		SubscribeCapacityMiBPerSec: 4,
		PerPartitionBytes:          30 * 1024 * 1024 * 1024,
		RetentionDuration:          time.Hour,
	})
	require.NoError(t, err)
	require.NotNil(t, creator)
}

func TestTopicCreatorCreateTopics(t *testing.T) {
	server, commonConfig := newTestAdminService(t)
	core, observedLogs := observer.New(zapcore.DebugLevel)
	commonConfig.Logger = zap.New(core)
	_, monitoringClientOpts := newTestMetricService(t)
	manager, err := NewManager(ManagerConfig{CommonConfig: commonConfig, MonitoringClientOptions: monitoringClientOpts})
	require.NoError(t, err)
	defer manager.Close()

	creator, err := manager.NewTopicCreator(TopicCreatorConfig{
		Reservation:                "reservation_name",
		PartitionCount:             123,
		PublishCapacityMiBPerSec:   4,
		SubscribeCapacityMiBPerSec: 5,
		PerPartitionBytes:          30 * 1024 * 1024 * 1024,
		RetentionDuration:          time.Hour,
	})
	require.NoError(t, err)

	server.topic = &pubsublitepb.Topic{
		Name:            "foo",
		PartitionConfig: &pubsublitepb.Topic_PartitionConfig{},
		RetentionConfig: &pubsublitepb.Topic_RetentionConfig{},
	}
	err = creator.CreateTopics(context.Background(), "topic_name")
	require.NoError(t, err)

	assert.Equal(t, "projects/project/locations/region-1", server.createTopicRequest.Parent)
	assert.Equal(t, "topic_name", server.createTopicRequest.TopicId)
	assert.Equal(t, &pubsublitepb.Topic_PartitionConfig{
		Count: 123,
		Dimension: &pubsublitepb.Topic_PartitionConfig_Capacity_{
			Capacity: &pubsublitepb.Topic_PartitionConfig_Capacity{
				PublishMibPerSec:   4,
				SubscribeMibPerSec: 5,
			},
		},
	}, server.createTopicRequest.Topic.PartitionConfig)
	assert.Equal(t, &pubsublitepb.Topic_RetentionConfig{
		PerPartitionBytes: 30 * 1024 * 1024 * 1024,
		Period:            durationpb.New(time.Hour),
	}, server.createTopicRequest.Topic.RetentionConfig)
	assert.Equal(t, &pubsublitepb.Topic_ReservationConfig{
		ThroughputReservation: "projects/project/locations/region-1/reservations/reservation_name",
	}, server.createTopicRequest.Topic.ReservationConfig)

	errorInfo, err := anypb.New(&errdetails.ErrorInfo{Reason: "RESOURCE_ALREADY_EXISTS"})
	require.NoError(t, err)
	server.err = status.ErrorProto(&spb.Status{
		Code:    int32(codes.AlreadyExists),
		Message: "topic already exists",
		Details: []*anypb.Any{errorInfo},
	})
	err = creator.CreateTopics(context.Background(), "topic_name")
	require.NoError(t, err)

	server.err = status.Errorf(codes.PermissionDenied, "nope")
	err = creator.CreateTopics(context.Background(), "topic_name")
	require.Error(t, err)
	assert.EqualError(t, err,
		`failed to create pubsublite topic "topic_name": rpc error: code = PermissionDenied desc = nope`,
	)

	assert.Equal(t, []observer.LoggedEntry{{
		Entry: zapcore.Entry{
			Level:   zapcore.InfoLevel,
			Message: "created pubsublite topic",
		},
		Context: []zapcore.Field{
			zap.String("topic", "topic_name"),
			zap.String("reservation", "reservation_name"),
			zap.Int("partition_count", 123),
			zap.Int64("per_partition_bytes", 30*1024*1024*1024),
			zap.Int("publish_capacity", 4),
			zap.Int("subscribe_capacity", 5),
			zap.Duration("retention_duration", time.Hour),
		},
	}, {
		Entry: zapcore.Entry{
			Level:   zapcore.DebugLevel,
			Message: "pubsublite topic already exists",
		},
		Context: []zapcore.Field{
			zap.String("topic", "topic_name"),
		},
	}}, observedLogs.AllUntimed())
}
