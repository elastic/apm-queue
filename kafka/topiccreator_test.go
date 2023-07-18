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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewTopicCreator(t *testing.T) {
	m, err := NewManager(ManagerConfig{
		CommonConfig: CommonConfig{
			Brokers: []string{"broker"},
			Logger:  zap.NewNop(),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { m.Close() })

	_, err = m.NewTopicCreator(TopicCreatorConfig{})
	assert.Error(t, err)
	assert.EqualError(t, err, "kafka: invalid topic creator config: "+strings.Join([]string{
		"kafka: partition count must be non-zero",
	}, "\n"))
}

func TestTopicCreatorCreateTopics(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	defer tp.Shutdown(context.Background())

	cluster, commonConfig := newFakeCluster(t)
	core, observedLogs := observer.New(zapcore.DebugLevel)
	commonConfig.Logger = zap.New(core)
	commonConfig.TracerProvider = tp

	m, err := NewManager(ManagerConfig{
		CommonConfig: commonConfig,
	})
	require.NoError(t, err)
	t.Cleanup(func() { m.Close() })
	c, err := m.NewTopicCreator(TopicCreatorConfig{
		PartitionCount: 123,
		TopicConfigs: map[string]string{
			"retention.ms": "123",
		},
	})
	require.NoError(t, err)

	var createTopicsRequest *kmsg.CreateTopicsRequest
	cluster.ControlKey(kmsg.CreateTopics.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		createTopicsRequest = req.(*kmsg.CreateTopicsRequest)
		return &kmsg.CreateTopicsResponse{
			Version: createTopicsRequest.Version,
			Topics: []kmsg.CreateTopicsResponseTopic{{
				Topic:        "name_space-topic1",
				ErrorCode:    kerr.TopicAlreadyExists.Code,
				ErrorMessage: &kerr.TopicAlreadyExists.Message,
			}, {
				Topic:        "name_space-topic2",
				ErrorCode:    kerr.InvalidTopicException.Code,
				ErrorMessage: &kerr.InvalidTopicException.Message,
			}, {
				Topic:   "name_space-topic3",
				TopicID: [16]byte{123},
			}},
		}, nil, true
	})
	err = c.CreateTopics(context.Background(), "topic1", "topic2", "topic3")
	require.Error(t, err)
	assert.EqualError(t, err,
		`failed to create topic "topic2": `+
			`INVALID_TOPIC_EXCEPTION: The request attempted to perform an operation on an invalid topic.`,
	)

	require.Len(t, createTopicsRequest.Topics, 3)
	assert.Equal(t, []kmsg.CreateTopicsRequestTopic{{
		Topic:             "name_space-topic1",
		NumPartitions:     123,
		ReplicationFactor: -1,
		Configs: []kmsg.CreateTopicsRequestTopicConfig{{
			Name:  "retention.ms",
			Value: kmsg.StringPtr("123"),
		}},
	}, {
		Topic:             "name_space-topic2",
		NumPartitions:     123,
		ReplicationFactor: -1,
		Configs: []kmsg.CreateTopicsRequestTopicConfig{{
			Name:  "retention.ms",
			Value: kmsg.StringPtr("123"),
		}},
	}, {
		Topic:             "name_space-topic3",
		NumPartitions:     123,
		ReplicationFactor: -1,
		Configs: []kmsg.CreateTopicsRequestTopicConfig{{
			Name:  "retention.ms",
			Value: kmsg.StringPtr("123"),
		}},
	}}, createTopicsRequest.Topics)

	matchingLogs := observedLogs.FilterFieldKey("topic")
	assert.Equal(t, []observer.LoggedEntry{{
		Entry: zapcore.Entry{
			Level:      zapcore.DebugLevel,
			LoggerName: "kafka",
			Message:    "kafka topic already exists",
		},
		Context: []zapcore.Field{
			zap.String("namespace", "name_space"),
			zap.String("topic", "topic1"),
		},
	}, {
		Entry: zapcore.Entry{
			Level:      zapcore.InfoLevel,
			LoggerName: "kafka",
			Message:    "created kafka topic",
		},
		Context: []zapcore.Field{
			zap.String("namespace", "name_space"),
			zap.String("topic", "topic3"),
			zap.Int("partition_count", 123),
			zap.Any("topic_configs", map[string]string{
				"retention.ms": "123",
			}),
		},
	}}, matchingLogs.AllUntimed())

	spans := exp.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, "CreateTopics", spans[0].Name)
	assert.Equal(t, codes.Error, spans[0].Status.Code)
	require.Len(t, spans[0].Events, 2)
	assert.Equal(t, "kafka topic already exists", spans[0].Events[0].Name)
	assert.Equal(t, []attribute.KeyValue{
		semconv.MessagingDestinationKey.String("topic1"),
	}, spans[0].Events[0].Attributes)
	assert.Equal(t, "exception", spans[0].Events[1].Name)
	assert.Equal(t, []attribute.KeyValue{
		semconv.ExceptionTypeKey.String("*kerr.Error"),
		semconv.ExceptionMessageKey.String(
			"INVALID_TOPIC_EXCEPTION: The request attempted to perform an operation on an invalid topic.",
		),
	}, spans[0].Events[1].Attributes)
}
