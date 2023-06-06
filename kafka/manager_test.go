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

package kafka

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewManager(t *testing.T) {
	_, err := NewManager(ManagerConfig{})
	assert.Error(t, err)
	assert.EqualError(t, err, "kafka: invalid manager config: "+strings.Join([]string{
		"kafka: at least one broker must be set",
		"kafka: logger must be set",
	}, "\n"))
}

func TestManagerDeleteTopics(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	defer tp.Shutdown(context.Background())

	cluster, commonConfig := newFakeCluster(t)
	core, observedLogs := observer.New(zapcore.DebugLevel)
	commonConfig.Logger = zap.New(core)
	commonConfig.TracerProvider = tp
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig})
	require.NoError(t, err)
	t.Cleanup(func() { m.Close() })

	var deleteTopicsRequest *kmsg.DeleteTopicsRequest
	cluster.ControlKey(kmsg.DeleteTopics.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		deleteTopicsRequest = req.(*kmsg.DeleteTopicsRequest)
		return &kmsg.DeleteTopicsResponse{
			Version: deleteTopicsRequest.Version,
			Topics: []kmsg.DeleteTopicsResponseTopic{{
				Topic:        kmsg.StringPtr("topic1"),
				ErrorCode:    kerr.UnknownTopicOrPartition.Code,
				ErrorMessage: &kerr.UnknownTopicOrPartition.Message,
			}, {
				Topic:        kmsg.StringPtr("topic2"),
				ErrorCode:    kerr.InvalidTopicException.Code,
				ErrorMessage: &kerr.InvalidTopicException.Message,
			}, {
				Topic:   kmsg.StringPtr("topic3"),
				TopicID: [16]byte{123},
			}},
		}, nil, true
	})
	err = m.DeleteTopics(context.Background(), "topic1", "topic2", "topic3")
	require.Error(t, err)
	assert.EqualError(t, err,
		`failed to delete topic "topic2": `+
			`INVALID_TOPIC_EXCEPTION: The request attempted to perform an operation on an invalid topic.`,
	)

	require.Len(t, deleteTopicsRequest.Topics, 3)
	assert.Equal(t, []kmsg.DeleteTopicsRequestTopic{{
		Topic: kmsg.StringPtr("topic1"),
	}, {
		Topic: kmsg.StringPtr("topic2"),
	}, {
		Topic: kmsg.StringPtr("topic3"),
	}}, deleteTopicsRequest.Topics)

	matchingLogs := observedLogs.FilterFieldKey("topic")
	assert.Equal(t, []observer.LoggedEntry{{
		Entry: zapcore.Entry{
			Level:   zapcore.DebugLevel,
			Message: "kafka topic does not exist",
		},
		Context: []zapcore.Field{
			zap.String("topic", "topic1"),
		},
	}, {
		Entry: zapcore.Entry{
			Level:   zapcore.InfoLevel,
			Message: "deleted kafka topic",
		},
		Context: []zapcore.Field{
			zap.String("topic", "topic3"),
		},
	}}, matchingLogs.AllUntimed())

	spans := exp.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, "DeleteTopics", spans[0].Name)
	assert.Equal(t, codes.Error, spans[0].Status.Code)
	require.Len(t, spans[0].Events, 1)
	assert.Equal(t, "exception", spans[0].Events[0].Name)
	assert.Equal(t, []attribute.KeyValue{
		semconv.ExceptionTypeKey.String("*kerr.Error"),
		semconv.ExceptionMessageKey.String(
			"INVALID_TOPIC_EXCEPTION: The request attempted to perform an operation on an invalid topic.",
		),
	}, spans[0].Events[0].Attributes)
}

func TestManagerMetrics(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	defer tp.Shutdown(context.Background())
	defer mp.Shutdown(context.Background())

	cluster, commonConfig := newFakeCluster(t)
	core, observedLogs := observer.New(zapcore.DebugLevel)
	commonConfig.Logger = zap.New(core)
	commonConfig.TracerProvider = tp
	commonConfig.MeterProvider = mp
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig})
	require.NoError(t, err)
	t.Cleanup(func() { m.Close() })

	var listGroupsRequest *kmsg.ListGroupsRequest
	cluster.ControlKey(kmsg.ListGroups.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		listGroupsRequest = req.(*kmsg.ListGroupsRequest)
		return &kmsg.ListGroupsResponse{
			Version: listGroupsRequest.Version,
			Groups: []kmsg.ListGroupsResponseGroup{
				{Group: "consumer1", ProtocolType: "consumer"},
				{Group: "connect", ProtocolType: "connect"},
				{Group: "consumer2", ProtocolType: "consumer"},
				{Group: "consumer3", ProtocolType: "consumer"}, // not authorized, while fetching offsets
			},
		}, nil, true
	})
	var describeGroupsRequest *kmsg.DescribeGroupsRequest
	cluster.ControlKey(kmsg.DescribeGroups.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		describeGroupsRequest = req.(*kmsg.DescribeGroupsRequest)
		return &kmsg.DescribeGroupsResponse{
			Version: describeGroupsRequest.Version,
			Groups: []kmsg.DescribeGroupsResponseGroup{
				{
					Group:        "consumer1",
					ProtocolType: "consumer",
					Members: []kmsg.DescribeGroupsResponseGroupMember{{
						MemberID:   "member_id_1",
						InstanceID: kmsg.StringPtr("instance_id_1"),
						ClientID:   "client_id",
						ClientHost: "127.0.0.1",
						MemberAssignment: (&kmsg.ConsumerMemberAssignment{
							Version: 2,
							Topics: []kmsg.ConsumerMemberAssignmentTopic{{
								Topic:      "topic1",
								Partitions: []int32{1},
							}},
						}).AppendTo(nil),
					}},
				},
				{Group: "connect", ProtocolType: "connect"}, // ignored
				{
					Group:        "consumer2",
					ProtocolType: "consumer",
					Members: []kmsg.DescribeGroupsResponseGroupMember{{
						MemberID:   "member_id_2",
						InstanceID: kmsg.StringPtr("instance_id_2"),
						ClientID:   "client_id",
						ClientHost: "127.0.0.1",
						MemberAssignment: (&kmsg.ConsumerMemberAssignment{
							Version: 2,
							Topics: []kmsg.ConsumerMemberAssignmentTopic{{
								Topic:      "topic1",
								Partitions: []int32{2},
							}, {
								Topic:      "topic2",
								Partitions: []int32{3, 4},
							}},
						}).AppendTo(nil),
					}},
				},
				{
					Group:        "consumer3",
					ProtocolType: "consumer",
					Members: []kmsg.DescribeGroupsResponseGroupMember{{
						MemberID:   "member_id_3",
						InstanceID: kmsg.StringPtr("instance_id_3"),
						ClientID:   "client_id",
						ClientHost: "127.0.0.1",
						MemberAssignment: (&kmsg.ConsumerMemberAssignment{
							Version: 2,
							Topics: []kmsg.ConsumerMemberAssignmentTopic{{
								Topic:      "topic3",
								Partitions: []int32{4},
							}},
						}).AppendTo(nil),
					}},
				},
			},
		}, nil, true
	})
	var offsetFetchRequest *kmsg.OffsetFetchRequest
	cluster.ControlKey(kmsg.OffsetFetch.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		offsetFetchRequest = req.(*kmsg.OffsetFetchRequest)
		return &kmsg.OffsetFetchResponse{
			Version: offsetFetchRequest.Version,
			Groups: []kmsg.OffsetFetchResponseGroup{{
				Group: "consumer1",
				Topics: []kmsg.OffsetFetchResponseGroupTopic{{
					Topic: "topic1",
					Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{{
						Partition: 1,
						Offset:    1,
					}},
				}},
			}, {
				Group: "consumer2",
				Topics: []kmsg.OffsetFetchResponseGroupTopic{{
					Topic: "topic1",
					Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{{
						Partition: 2,
						Offset:    1,
					}},
				}, {
					Topic: "topic2",
					Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{{
						Partition: 3,
						Offset:    1,
					}},
				}},
			}, {
				Group:     "consumer3",
				ErrorCode: kerr.GroupAuthorizationFailed.Code,
			}},
		}, nil, true
	})
	var listOffsetsRequest *kmsg.ListOffsetsRequest
	cluster.ControlKey(kmsg.ListOffsets.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		listOffsetsRequest = req.(*kmsg.ListOffsetsRequest)
		return &kmsg.ListOffsetsResponse{
			Version: listOffsetsRequest.Version,
			Topics: []kmsg.ListOffsetsResponseTopic{{
				Topic: "topic1",
				Partitions: []kmsg.ListOffsetsResponseTopicPartition{{
					Partition: 1,
					Offset:    1,
				}, {
					Partition: 2,
					Offset:    2,
				}},
			}, {
				Topic: "topic2",
				Partitions: []kmsg.ListOffsetsResponseTopicPartition{{
					Partition: 3,
					Offset:    3,
				}},
			}, {
				Topic: "topic3",
				Partitions: []kmsg.ListOffsetsResponseTopicPartition{{
					Partition: 4,
					Offset:    4,
				}},
			}},
		}, nil, true
	})

	rm := metricdata.ResourceMetrics{}
	err = reader.Collect(context.Background(), &rm)
	require.NoError(t, err)
	require.Len(t, rm.ScopeMetrics, 2)
	sort.Slice(rm.ScopeMetrics, func(i, j int) bool {
		return rm.ScopeMetrics[i].Scope.Name < rm.ScopeMetrics[j].Scope.Name
	})
	assert.Equal(t, "github.com/elastic/apm-queue/kafka", rm.ScopeMetrics[0].Scope.Name)

	metrics := rm.ScopeMetrics[0].Metrics
	require.Len(t, metrics, 1)
	assert.Equal(t, "kafka.consumer_group_lag", metrics[0].Name)
	metricdatatest.AssertAggregationsEqual(t, metricdata.Gauge[int64]{
		DataPoints: []metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(
				attribute.String("group", "consumer1"),
				attribute.String("topic", "topic1"),
				attribute.Int("partition", 1),
			),
			Value: 1,
		}, {
			Attributes: attribute.NewSet(
				attribute.String("group", "consumer2"),
				attribute.String("topic", "topic1"),
				attribute.Int("partition", 2),
			),
			Value: 2,
		}, {
			Attributes: attribute.NewSet(
				attribute.String("group", "consumer2"),
				attribute.String("topic", "topic2"),
				attribute.Int("partition", 3),
			),
			Value: 3,
		}, {
			Attributes: attribute.NewSet(
				attribute.String("group", "consumer3"),
				attribute.String("topic", "topic3"),
				attribute.Int("partition", 4),
			),
			Value: 4,
		}},
	}, metrics[0].Data, metricdatatest.IgnoreTimestamp())

	assert.Equal(t, &kmsg.ListGroupsRequest{Version: 4}, listGroupsRequest)
	assert.Equal(t, &kmsg.DescribeGroupsRequest{
		Version: 5,
		Groups:  []string{"connect", "consumer1", "consumer2", "consumer3"},
	}, describeGroupsRequest)
	assert.Equal(t, &kmsg.OffsetFetchRequest{
		Version: 8,
		Groups: []kmsg.OffsetFetchRequestGroup{
			{Group: "consumer1"},
			{Group: "consumer2"},
			{Group: "consumer3"},
		},
	}, offsetFetchRequest)
	assert.ElementsMatch(t, []kmsg.ListOffsetsRequestTopic{
		{Topic: "topic1"}, {Topic: "topic2"}, {Topic: "topic3"},
	}, listOffsetsRequest.Topics)

	matchingLogs := observedLogs.FilterFieldKey("group")
	assert.Equal(t, []observer.LoggedEntry{{
		Entry: zapcore.Entry{
			Level:   zapcore.DebugLevel,
			Message: "ignoring non-consumer group",
		},
		Context: []zapcore.Field{
			zap.String("group", "connect"),
			zap.String("protocol_type", "connect"),
		},
	}, {
		Entry: zapcore.Entry{
			Level:   zapcore.WarnLevel,
			Message: "error getting consumer group lag",
		},
		Context: []zapcore.Field{
			zap.String("group", "consumer2"),
			zap.String("topic", "topic2"),
			zap.Int32("partition", 4),
			zap.Error(errors.New("missing from list offsets")),
		},
	}}, matchingLogs.AllUntimed())

	spans := exp.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, "GatherMetrics", spans[0].Name)
}

func newFakeCluster(t testing.TB) (*kfake.Cluster, CommonConfig) {
	cluster, err := kfake.NewCluster(
		// Just one broker to simplify dealing with sharded requests.
		kfake.NumBrokers(1),
	)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	return cluster, CommonConfig{
		Brokers: cluster.ListenAddrs(),
		Logger:  zap.NewNop(),
	}
}
