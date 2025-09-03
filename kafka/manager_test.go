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

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
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

	apmqueue "github.com/elastic/apm-queue/v2"
	"github.com/elastic/apm-queue/v2/metrictest"
)

func TestNewManager(t *testing.T) {
	_, err := NewManager(ManagerConfig{})
	assert.Error(t, err)
	assert.EqualError(t, err, "kafka: invalid manager config: "+strings.Join([]string{
		"kafka: logger must be set",
		"kafka: at least one broker must be set",
	}, "\n"))
}

func TestManagerDeleteTopics(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer tp.Shutdown(context.Background())

	mt := metrictest.New()
	cluster, commonConfig := newFakeCluster(t)
	core, observedLogs := observer.New(zapcore.DebugLevel)
	commonConfig.Logger = zap.New(core)
	commonConfig.TracerProvider = tp
	commonConfig.MeterProvider = mt.MeterProvider
	commonConfig.TopicLogFieldFunc = func(topic string) zap.Field {
		return zap.String("from-1", "singular-topic-log-func")
	}
	commonConfig.TopicLogFieldsFunc = func(topic string) []zap.Field {
		return []zap.Field{zap.String("from-2", "multiple-topic-log-func")}
	}
	commonConfig.TopicAttributeFunc = func(topic string) attribute.KeyValue {
		return attribute.String("from-1", "singular-topic-attribute-func")
	}
	commonConfig.TopicAttributesFunc = func(topic string) []attribute.KeyValue {
		return []attribute.KeyValue{attribute.String("from-2", "multiple-topic-attribute-func")}
	}
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig})
	require.NoError(t, err)
	t.Cleanup(func() { m.Close() })

	var deleteTopicsRequest *kmsg.DeleteTopicsRequest
	cluster.ControlKey(kmsg.DeleteTopics.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		deleteTopicsRequest = req.(*kmsg.DeleteTopicsRequest)
		return &kmsg.DeleteTopicsResponse{
			Version: deleteTopicsRequest.Version,
			Topics: []kmsg.DeleteTopicsResponseTopic{{
				Topic:        kmsg.StringPtr("name_space-topic1"),
				ErrorCode:    kerr.UnknownTopicOrPartition.Code,
				ErrorMessage: &kerr.UnknownTopicOrPartition.Message,
			}, {
				Topic:        kmsg.StringPtr("name_space-topic2"),
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
		Topic: kmsg.StringPtr("name_space-topic1"),
	}, {
		Topic: kmsg.StringPtr("name_space-topic2"),
	}, {
		Topic: kmsg.StringPtr("name_space-topic3"),
	}}, deleteTopicsRequest.Topics)

	matchingLogs := observedLogs.FilterFieldKey("topic")
	assert.Equal(t, []observer.LoggedEntry{{
		Entry: zapcore.Entry{
			Level:      zapcore.DebugLevel,
			LoggerName: "kafka",
			Message:    "kafka topic does not exist",
		},
		Context: []zapcore.Field{
			zap.String("namespace", "name_space"),
			zap.String("topic", "topic1"),
			zap.String("from-2", "multiple-topic-log-func"),
			zap.String("from-1", "singular-topic-log-func"),
		},
	}, {
		Entry: zapcore.Entry{
			Level:      zapcore.InfoLevel,
			LoggerName: "kafka",
			Message:    "deleted kafka topic",
		},
		Context: []zapcore.Field{
			zap.String("namespace", "name_space"),
			zap.String("topic", "topic3"),
			zap.String("from-2", "multiple-topic-log-func"),
			zap.String("from-1", "singular-topic-log-func"),
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
	rm, err := mt.Collect(context.Background())
	require.NoError(t, err)
	// Filter all other kafka metrics.
	var metrics []metricdata.Metrics
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name == "github.com/elastic/apm-queue/kafka" {
			metrics = sm.Metrics
			break
		}
	}
	gathered := metrictest.GatherInt64Metric(metrics)
	metricKey := metrictest.Key{Name: "topics.deleted.count"}
	gotMetrics := metrictest.Int64Metrics{
		metricKey: gathered[metricKey],
	}
	// Ensure only 1 topic was deleted, which also matches the number of spans.
	assert.Empty(t, cmp.Diff(metrictest.Int64Metrics{
		{Name: "topics.deleted.count"}: {
			{K: "topic", V: "topic2"}:                         1,
			{K: "messaging.system", V: "kafka"}:               2,
			{K: "outcome", V: "failure"}:                      1,
			{K: "outcome", V: "success"}:                      1,
			{K: "topic", V: "topic3"}:                         1,
			{K: "from-1", V: "singular-topic-attribute-func"}: 2,
			{K: "from-2", V: "multiple-topic-attribute-func"}: 2,
		},
	}, gotMetrics))
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
	commonConfig.TopicAttributeFunc = func(topic string) attribute.KeyValue {
		return attribute.Bool("foo", true)
	}
	commonConfig.TopicAttributesFunc = func(topic string) []attribute.KeyValue {
		return []attribute.KeyValue{attribute.Bool("bar", true)}
	}
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig})
	require.NoError(t, err)
	t.Cleanup(func() { m.Close() })

	registration, err := m.MonitorConsumerLag([]apmqueue.TopicConsumer{
		{
			Topic:    "topic1",
			Consumer: "consumer1",
		},
		{
			Topic:    "topic1",
			Consumer: "consumer2",
		},
		{
			Topic:    "topic2",
			Consumer: "consumer2",
		},
		{
			Topic:    "topic3",
			Consumer: "consumer3",
		},
		{
			Topic:    "",
			Consumer: "connect",
		},
		{
			Regex:    "my.*",
			Consumer: "consumer3",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { registration.Unregister() })

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
								Topic:      "name_space-topic1",
								Partitions: []int32{1},
							}},
						}).AppendTo(nil),
					}},
				},
				{Group: "connect", ProtocolType: "connect"},
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
								Topic:      "name_space-topic1",
								Partitions: []int32{2},
							}, {
								Topic:      "name_space-topic2",
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
								Topic:      "name_space-topic3",
								Partitions: []int32{4},
							}, {
								Topic:      "name_space-mytopic",
								Partitions: []int32{1},
							}},
						}).AppendTo(nil),
					}},
				},
				{
					// Consumer 4 and its topics are ignored since it's not
					// captured by the monitoring list.
					Group:        "consumer4",
					ProtocolType: "consumer",
					Members: []kmsg.DescribeGroupsResponseGroupMember{{
						MemberID:   "member_id_3",
						InstanceID: kmsg.StringPtr("instance_id_3"),
						ClientID:   "client_id",
						ClientHost: "127.0.0.1",
						MemberAssignment: (&kmsg.ConsumerMemberAssignment{
							Version: 2,
							Topics: []kmsg.ConsumerMemberAssignmentTopic{{
								Topic:      "name_space-mytopic",
								Partitions: []int32{1},
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
					Topic: "name_space-topic1",
					Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{{
						Partition: 1,
						Offset:    1,
					}},
				}},
			}, {
				Group: "consumer2",
				Topics: []kmsg.OffsetFetchResponseGroupTopic{{
					Topic: "name_space-topic1",
					Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{{
						Partition: 2,
						Offset:    1,
					}},
				}, {
					Topic: "name_space-topic2",
					Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{{
						Partition: 3,
						Offset:    1,
					}},
				}},
			}, {
				Group: "consumer3",
				Topics: []kmsg.OffsetFetchResponseGroupTopic{{
					Topic: "name_space-mytopic",
					Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{{
						Partition: 1,
						Offset:    1,
					}},
				}},
			}, {
				// Consumer 4 and its topics are ignored since it's not
				// captured by the monitoring list.
				Group: "consumer4",
				Topics: []kmsg.OffsetFetchResponseGroupTopic{{
					Topic: "name_space-mytopic",
					Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{{
						Partition: 1,
						Offset:    1,
					}},
				}},
			}},
		}, nil, true
	})

	var metadataRequest *kmsg.MetadataRequest
	cluster.ControlKey(kmsg.Metadata.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		if len(req.(*kmsg.MetadataRequest).Topics) == 0 {
			return nil, nil, false
		}

		metadataRequest = req.(*kmsg.MetadataRequest)
		cluster.KeepControl()
		return &kmsg.MetadataResponse{
			Version: metadataRequest.Version,
			Brokers: []kmsg.MetadataResponseBroker{},
			Topics: []kmsg.MetadataResponseTopic{{
				Topic:      kmsg.StringPtr("name_space-topic1"),
				Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 1}, {Partition: 2}},
			}, {
				Topic:      kmsg.StringPtr("name_space-topic2"),
				Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 3}},
			}, {
				Topic:      kmsg.StringPtr("name_space-topic3"),
				Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 4}},
			}, {
				Topic:      kmsg.StringPtr("name_space-mytopic"),
				Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 1}},
			}},
		}, nil, true
	})

	var listOffsetsRequest *kmsg.ListOffsetsRequest
	cluster.ControlKey(kmsg.ListOffsets.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		listOffsetsRequest = req.(*kmsg.ListOffsetsRequest)
		return &kmsg.ListOffsetsResponse{
			Version: listOffsetsRequest.Version,
			Topics: []kmsg.ListOffsetsResponseTopic{{
				Topic: "name_space-topic1",
				Partitions: []kmsg.ListOffsetsResponseTopicPartition{{
					Partition: 1,
					Offset:    1,
				}, {
					Partition: 2,
					Offset:    2,
				}},
			}, {
				Topic: "name_space-topic2",
				Partitions: []kmsg.ListOffsetsResponseTopicPartition{{
					Partition: 3,
					Offset:    3,
				}},
			}, {
				Topic: "name_space-topic3",
				Partitions: []kmsg.ListOffsetsResponseTopicPartition{{
					Partition: 4,
					Offset:    4,
				}},
			}, {
				Topic: "name_space-mytopic",
				Partitions: []kmsg.ListOffsetsResponseTopicPartition{{
					Partition: 1,
					Offset:    2,
				}},
			}},
		}, nil, true
	})

	rm := metricdata.ResourceMetrics{}
	err = reader.Collect(context.Background(), &rm)
	require.NoError(t, err)
	require.Len(t, rm.ScopeMetrics, 1)
	assert.Equal(t, "github.com/elastic/apm-queue/kafka", rm.ScopeMetrics[0].Scope.Name)

	metrics := rm.ScopeMetrics[0].Metrics
	require.Len(t, metrics, 8)
	var lagMetric, assignmentMetric metricdata.Metrics
	// these are not stable so we just assert for existence
	var connectsMetric, disconnectsMetric, writeBytesMetric, readBytesMetric, writeLatencyMetric, readLatencyMetric bool
	for _, metric := range metrics {
		switch metric.Name {
		case "consumer_group_lag":
			lagMetric = metric
		case "consumer_group_assignment":
			assignmentMetric = metric
		case "messaging.kafka.connects.count":
			connectsMetric = true
		case "messaging.kafka.disconnects.count":
			disconnectsMetric = true
		case "messaging.kafka.write_bytes":
			writeBytesMetric = true
		case "messaging.kafka.read_bytes.count":
			readBytesMetric = true
		case "messaging.kafka.write.latency":
			writeLatencyMetric = true
		case "messaging.kafka.read.latency":
			readLatencyMetric = true
		}
	}
	assert.True(t, writeBytesMetric)
	assert.True(t, readBytesMetric)
	assert.True(t, connectsMetric)
	assert.True(t, disconnectsMetric)
	assert.True(t, writeLatencyMetric)
	assert.True(t, readLatencyMetric)
	metricdatatest.AssertAggregationsEqual(t, metricdata.Gauge[int64]{
		DataPoints: []metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(
				attribute.String("group", "consumer1"),
				attribute.String("topic", "topic1"),
				attribute.Int("partition", 1),
				attribute.Bool("foo", true),
				attribute.Bool("bar", true),
			),
			Value: 0, // end offset = 1, committed = 1
		}, {
			Attributes: attribute.NewSet(
				attribute.String("group", "consumer2"),
				attribute.String("topic", "topic1"),
				attribute.Int("partition", 2),
				attribute.Bool("foo", true),
				attribute.Bool("bar", true),
			),
			Value: 1, // end offset = 2, committed = 1
		}, {
			Attributes: attribute.NewSet(
				attribute.String("group", "consumer2"),
				attribute.String("topic", "topic2"),
				attribute.Int("partition", 3),
				attribute.Bool("foo", true),
				attribute.Bool("bar", true),
			),
			Value: 2, // end offset = 3, committed = 1
		}, {
			Attributes: attribute.NewSet(
				attribute.String("group", "consumer3"),
				attribute.String("topic", "topic3"),
				attribute.Int("partition", 4),
				attribute.Bool("foo", true),
				attribute.Bool("bar", true),
			),
			Value: 0, // end offset  = 4, nothing committed
		}, {
			Attributes: attribute.NewSet(
				attribute.String("group", "consumer3"),
				attribute.String("topic", "mytopic"),
				attribute.Int("partition", 1),
				attribute.Bool("foo", true),
				attribute.Bool("bar", true),
			),
			Value: 1, // end offset  = 1, nothing committed
		}},
	}, lagMetric.Data, metricdatatest.IgnoreTimestamp())

	metricdatatest.AssertAggregationsEqual(t, metricdata.Gauge[int64]{
		DataPoints: []metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(
				attribute.String("client_id", "client_id"),
				attribute.String("group", "consumer1"),
				attribute.String("topic", "topic1"),
				attribute.Bool("foo", true),
				attribute.Bool("bar", true),
			),
			Value: 1,
		}, {
			Attributes: attribute.NewSet(
				attribute.String("client_id", "client_id"),
				attribute.String("group", "consumer2"),
				attribute.String("topic", "topic2"),
				attribute.Bool("foo", true),
				attribute.Bool("bar", true),
			),
			Value: 1,
		}, {
			Attributes: attribute.NewSet(
				attribute.String("client_id", "client_id"),
				attribute.String("group", "consumer2"),
				attribute.String("topic", "topic1"),
				attribute.Bool("foo", true),
				attribute.Bool("bar", true),
			),
			Value: 1,
		}, {
			Attributes: attribute.NewSet(
				attribute.String("client_id", "client_id"),
				attribute.String("group", "consumer3"),
				attribute.String("topic", "topic3"),
				attribute.Bool("foo", true),
				attribute.Bool("bar", true),
			),
			Value: 1,
		}, {
			Attributes: attribute.NewSet(
				attribute.String("client_id", "client_id"),
				attribute.String("group", "consumer3"),
				attribute.String("topic", "mytopic"),
				attribute.Bool("foo", true),
				attribute.Bool("bar", true),
			),
			Value: 1,
		}},
	}, assignmentMetric.Data, metricdatatest.IgnoreTimestamp())

	assert.Equal(t, int16(6), describeGroupsRequest.Version)
	assert.ElementsMatch(t, []string{"connect", "consumer1", "consumer2", "consumer3"}, describeGroupsRequest.Groups)
	assert.ElementsMatch(t, []kmsg.OffsetFetchRequestGroup{
		{Group: "connect", MemberEpoch: -1},
		{Group: "consumer1", MemberEpoch: -1},
		{Group: "consumer2", MemberEpoch: -1},
		{Group: "consumer3", MemberEpoch: -1},
		{Group: "consumer4", MemberEpoch: -1},
	}, offsetFetchRequest.Groups)
	assert.ElementsMatch(t, []kmsg.MetadataRequestTopic{
		{Topic: kmsg.StringPtr("name_space-topic1")},
		{Topic: kmsg.StringPtr("name_space-topic2")},
		{Topic: kmsg.StringPtr("name_space-topic3")},
		{Topic: kmsg.StringPtr("name_space-mytopic")},
	}, metadataRequest.Topics)

	sort.Slice(listOffsetsRequest.Topics, func(i, j int) bool {
		return listOffsetsRequest.Topics[i].Topic < listOffsetsRequest.Topics[j].Topic
	})
	for _, topic := range listOffsetsRequest.Topics {
		sort.Slice(topic.Partitions, func(i, j int) bool {
			return topic.Partitions[i].Partition < topic.Partitions[j].Partition
		})
	}
	assert.Equal(t, []kmsg.ListOffsetsRequestTopic{{
		Topic: "name_space-mytopic",
		Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
			Partition:          1,
			CurrentLeaderEpoch: -1,
			Timestamp:          -1,
			MaxNumOffsets:      1,
		}},
	}, {
		Topic: "name_space-topic1",
		Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
			Partition:          1,
			CurrentLeaderEpoch: -1,
			Timestamp:          -1,
			MaxNumOffsets:      1,
		}, {
			Partition:          2,
			CurrentLeaderEpoch: -1,
			Timestamp:          -1,
			MaxNumOffsets:      1,
		}},
	}, {
		Topic: "name_space-topic2",
		Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
			Partition:          3,
			CurrentLeaderEpoch: -1,
			Timestamp:          -1,
			MaxNumOffsets:      1,
		}},
	}, {
		Topic: "name_space-topic3",
		Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
			Partition:          4,
			CurrentLeaderEpoch: -1,
			Timestamp:          -1,
			MaxNumOffsets:      1,
		}},
	}}, listOffsetsRequest.Topics)

	matchingLogs := observedLogs.FilterFieldKey("group")
	assert.Equal(t, []observer.LoggedEntry{{
		Entry: zapcore.Entry{
			Level:      zapcore.WarnLevel,
			LoggerName: "kafka",
			Message:    "error getting consumer group lag",
		},
		Context: []zapcore.Field{
			zap.String("namespace", "name_space"),
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

func TestManagerCreateACLs(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		cluster, commonConfig := newFakeCluster(t)
		// Test successful ACL creation
		cluster.ControlKey(kmsg.CreateACLs.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
			return &kmsg.CreateACLsResponse{
				Version: req.GetVersion(),
				Results: []kmsg.CreateACLsResponseResult{
					{}, {}, // Empty result means success
				},
			}, nil, true
		})
		m, err := NewManager(ManagerConfig{CommonConfig: commonConfig})
		require.NoError(t, err)
		t.Cleanup(func() { m.Close() })

		acls := kadm.NewACLs().
			Allow("User:*").
			Topics("topic").
			Operations(kadm.OpRead, kadm.OpWrite). // More specific operations instead of OpAll
			ResourcePatternType(kadm.ACLPatternPrefixed)

		// `kfake` does not support the ApiVersions API, so we need to
		// manually add the CreateACLs API to the ApiVersions response.
		cluster.ControlKey(kmsg.ApiVersions.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
			return &kmsg.ApiVersionsResponse{
				Version: req.GetVersion(),
				ApiKeys: []kmsg.ApiVersionsResponseApiKey{
					{ApiKey: kmsg.CreateACLs.Int16(), MaxVersion: 3},
				},
			}, nil, true
		})

		err = m.CreateACLs(context.Background(), acls)
		assert.NoError(t, err)
	})
	t.Run("Partial Failure", func(t *testing.T) {
		cluster, commonConfig := newFakeCluster(t)
		respErr := kerr.InvalidPrincipalType
		// Test unsuccessful ACL creation
		cluster.ControlKey(kmsg.CreateACLs.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
			return &kmsg.CreateACLsResponse{
				Version: req.GetVersion(),
				Results: []kmsg.CreateACLsResponseResult{
					{ErrorCode: respErr.Code, ErrorMessage: &respErr.Message},
				},
			}, nil, true
		})
		m, err := NewManager(ManagerConfig{CommonConfig: commonConfig})
		require.NoError(t, err)
		t.Cleanup(func() { m.Close() })

		acls := kadm.NewACLs().
			Allow("User:*").
			Topics("topic").
			Operations(kadm.OpWrite). // More specific operations instead of OpAll
			ResourcePatternType(kadm.ACLPatternPrefixed)

		// `kfake` does not support the ApiVersions API, so we need to
		// manually add the CreateACLs API to the ApiVersions response.
		cluster.ControlKey(kmsg.ApiVersions.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
			return &kmsg.ApiVersionsResponse{
				Version: req.GetVersion(),
				ApiKeys: []kmsg.ApiVersionsResponseApiKey{
					{ApiKey: kmsg.CreateACLs.Int16(), MaxVersion: 3},
				},
			}, nil, true
		})

		err = m.CreateACLs(context.Background(), acls)
		assert.EqualError(t, err, respErr.Error())
	})
}

func TestListTopics(t *testing.T) {
	cluster, commonConfig := newFakeCluster(t)
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig})
	require.NoError(t, err)
	t.Cleanup(func() { m.Close() })
	var metadataRequest *kmsg.MetadataRequest
	cluster.ControlKey(kmsg.Metadata.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		metadataRequest = req.(*kmsg.MetadataRequest)
		cluster.KeepControl()
		return &kmsg.MetadataResponse{
			Version: metadataRequest.Version,
			Brokers: []kmsg.MetadataResponseBroker{},
			Topics: []kmsg.MetadataResponseTopic{{
				Topic:      kmsg.StringPtr("name_space-topic1"),
				Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 1}, {Partition: 2}},
			}, {
				Topic:      kmsg.StringPtr("name_space-topic2"),
				Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 3}},
				ErrorCode:  kerr.UnknownTopicOrPartition.Code,
			}, {
				Topic:      kmsg.StringPtr("name_space-topic3"),
				Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 4}},
			}, {
				Topic:      kmsg.StringPtr("name_space-mytopic"),
				Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 1}},
			}, {
				Topic:      kmsg.StringPtr("rnd-topic"),
				Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 4}},
			}},
		}, nil, true
	})
	topics, err := m.ListTopics(context.Background(), "name_space")
	assert.EqualError(t, err, "name_space-topic2 UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.")
	assert.Equal(t, []string{"name_space-mytopic", "name_space-topic1", "name_space-topic3"}, topics)
}

func newFakeCluster(t testing.TB) (*kfake.Cluster, CommonConfig) {
	cluster, err := kfake.NewCluster(
		// Just one broker to simplify dealing with sharded requests.
		kfake.NumBrokers(1),
	)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	return cluster, CommonConfig{
		Brokers:   cluster.ListenAddrs(),
		Logger:    zap.NewNop(),
		Namespace: "name_space",
	}
}
