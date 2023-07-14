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
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/pubsublite/apiv1/pubsublitepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	apmqueue "github.com/elastic/apm-queue"
)

func TestNewConsumer(t *testing.T) {
	t.Run("empty config", func(t *testing.T) {
		_, err := NewConsumer(context.Background(), ConsumerConfig{})
		assert.EqualError(t, err, "pubsublite: invalid consumer config: "+strings.Join([]string{
			"logger must be set",
			"region must be set",
			"project must be set",
			"at least one topic must be set",
			"consumer name must be set",
			"processor must be set",
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
		assert.EqualError(t, err, "pubsublite: invalid consumer config: delivery is not valid")
	})
}

func TestConsumerConsume(t *testing.T) {
	type subscriptionPartition struct {
		Subscription string
		Partition    int64
	}

	ch := make(chan apmqueue.Record)
	server, config := newConsumerService(t)
	topic1SubscriptionPath := "projects/project/locations/region-1/subscriptions/name_space-topic1+consumer_name"
	topic2SubscriptionPath := "projects/project/locations/region-1/subscriptions/name_space-topic2+consumer_name"

	server.startSession = func(session *subscriberSession, _ *pubsublitepb.SeekRequest) {
		switch session.subscription {
		case topic1SubscriptionPath:
		case topic2SubscriptionPath:
		default:
			panic("unexpected subscription: " + session.subscription)
		}
		switch session.partition {
		case 1, 2:
		default:
			panic(fmt.Errorf("unexpected partition: %d", session.partition))
		}
		orderingKey, _ := json.Marshal(subscriptionPartition{
			Subscription: session.subscription,
			Partition:    session.partition,
		})
		session.messages <- []*pubsublitepb.SequencedMessage{{
			Cursor: &pubsublitepb.Cursor{Offset: 0},
			Message: &pubsublitepb.PubSubMessage{
				Key:  orderingKey,
				Data: []byte("message1"),
			},
		}, {
			Cursor: &pubsublitepb.Cursor{Offset: 1},
			Message: &pubsublitepb.PubSubMessage{
				Key:  orderingKey,
				Data: []byte("message2"),
			},
		}}
	}
	server.process = func(ctx context.Context, records ...apmqueue.Record) error {
		if n := len(records); n != 1 {
			panic(fmt.Errorf("expected 1 record, got %d", n))
		}
		ch <- records[0]
		return nil
	}

	consumer, err := NewConsumer(context.Background(), config)
	require.NoError(t, err)
	defer consumer.Close()
	go consumer.Run(context.Background())

	// We should receive 2 records for each subscription-partition.
	records := make(map[subscriptionPartition][]apmqueue.Record)
	for i := 0; i < 8; i++ {
		record := <-ch
		var sp subscriptionPartition
		err := json.Unmarshal(record.OrderingKey, &sp)
		require.NoError(t, err)
		record.OrderingKey = nil // simplify comparison below
		records[sp] = append(records[sp], record)
	}

	assert.Equal(t, map[subscriptionPartition][]apmqueue.Record{
		subscriptionPartition{
			Subscription: topic1SubscriptionPath,
			Partition:    1,
		}: []apmqueue.Record{
			{Topic: "topic1", Value: []byte("message1")},
			{Topic: "topic1", Value: []byte("message2")},
		},

		subscriptionPartition{
			Subscription: topic1SubscriptionPath,
			Partition:    2,
		}: []apmqueue.Record{
			{Topic: "topic1", Value: []byte("message1")},
			{Topic: "topic1", Value: []byte("message2")},
		},

		subscriptionPartition{
			Subscription: topic2SubscriptionPath,
			Partition:    1,
		}: []apmqueue.Record{
			{Topic: "topic2", Value: []byte("message1")},
			{Topic: "topic2", Value: []byte("message2")},
		},

		subscriptionPartition{
			Subscription: topic2SubscriptionPath,
			Partition:    2,
		}: []apmqueue.Record{
			{Topic: "topic2", Value: []byte("message1")},
			{Topic: "topic2", Value: []byte("message2")},
		},
	}, records)
}

func newConsumerService(t testing.TB) (*subscriberServiceServer, ConsumerConfig) {
	s := grpc.NewServer()
	t.Cleanup(s.Stop)
	server := &subscriberServiceServer{
		startSession: func(*subscriberSession, *pubsublitepb.SeekRequest) {},
		process: func(ctx context.Context, records ...apmqueue.Record) error {
			return nil
		},
	}
	pubsublitepb.RegisterCursorServiceServer(s, server)
	pubsublitepb.RegisterPartitionAssignmentServiceServer(s, server)
	pubsublitepb.RegisterSubscriberServiceServer(s, server)

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { lis.Close() })
	go s.Serve(lis)

	return server, ConsumerConfig{
		CommonConfig: CommonConfig{
			Project:   "project",
			Region:    "region-1",
			Namespace: "name_space",
			Logger:    zap.NewNop(),
			ClientOptions: []option.ClientOption{
				option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
				option.WithEndpoint(lis.Addr().String()),
				option.WithoutAuthentication(),
			},
		},
		ConsumerName: "consumer_name",
		Topics:       []apmqueue.Topic{"topic1", "topic2"},
		Delivery:     apmqueue.AtMostOnceDeliveryType,
		Processor: apmqueue.ProcessorFunc(func(ctx context.Context, records ...apmqueue.Record) error {
			if server.process != nil {
				return server.process(ctx, records...)
			}
			return nil
		}),
	}
}

type subscriberServiceServer struct {
	pubsublitepb.UnimplementedCursorServiceServer
	pubsublitepb.UnimplementedPartitionAssignmentServiceServer
	pubsublitepb.UnimplementedSubscriberServiceServer

	startSession func(*subscriberSession, *pubsublitepb.SeekRequest)
	process      apmqueue.ProcessorFunc
}

type subscriberSession struct {
	subscription string
	partition    int64
	messages     chan []*pubsublitepb.SequencedMessage

	mu              sync.RWMutex
	cursor          int64
	allowedBytes    int64
	allowedMessages int64
}

func (s *subscriberServiceServer) Subscribe(ss pubsublitepb.SubscriberService_SubscribeServer) error {
	session := subscriberSession{
		messages: make(chan []*pubsublitepb.SequencedMessage, 1),
	}
	requests := make(chan *pubsublitepb.SubscribeRequest)

	ctx, cancel := context.WithCancelCause(ss.Context())
	defer cancel(nil)
	go func() {
		for {
			req, err := ss.Recv()
			if err != nil {
				cancel(err)
				return
			}
			select {
			case <-ctx.Done():
				return
			case requests <- req:
			}
		}
	}()

	for {
		var req *pubsublitepb.SubscribeRequest
		select {
		case <-ctx.Done():
			return ctx.Err()
		case messages := <-session.messages:
			if err := ss.Send(&pubsublitepb.SubscribeResponse{
				Response: &pubsublitepb.SubscribeResponse_Messages{
					Messages: &pubsublitepb.MessageResponse{
						Messages: messages,
					},
				},
			}); err != nil {
				return err
			}
			continue
		case req = <-requests:
		}
		if req := req.GetInitial(); req != nil {
			session.subscription = req.Subscription
			session.partition = req.Partition
			s.startSession(&session, req.InitialLocation)
			if err := ss.Send(&pubsublitepb.SubscribeResponse{
				Response: &pubsublitepb.SubscribeResponse_Initial{
					Initial: &pubsublitepb.InitialSubscribeResponse{
						Cursor: &pubsublitepb.Cursor{
							Offset: session.cursor,
						},
					},
				},
			}); err != nil {
				return err
			}
			continue
		}
		if req := req.GetFlowControl(); req != nil {
			session.mu.Lock()
			session.allowedBytes = req.AllowedBytes
			session.allowedMessages = req.AllowedMessages
			session.mu.Unlock()
			continue
		}
	}
}

func (*subscriberServiceServer) StreamingCommitCursor(cs pubsublitepb.CursorService_StreamingCommitCursorServer) error {
	for {
		req, err := cs.Recv()
		if err != nil {
			return err
		}
		if req := req.GetInitial(); req != nil {
			if err := cs.Send(&pubsublitepb.StreamingCommitCursorResponse{
				Request: &pubsublitepb.StreamingCommitCursorResponse_Initial{},
			}); err != nil {
				return err
			}
		}
		if req := req.GetCommit(); req != nil {
			if err := cs.Send(&pubsublitepb.StreamingCommitCursorResponse{
				Request: &pubsublitepb.StreamingCommitCursorResponse_Commit{
					Commit: &pubsublitepb.SequencedCommitCursorResponse{
						AcknowledgedCommits: 1,
					},
				},
			}); err != nil {
				return err
			}
		}
	}
}

func (s *subscriberServiceServer) AssignPartitions(aps pubsublitepb.PartitionAssignmentService_AssignPartitionsServer) error {
	for {
		req, err := aps.Recv()
		if err != nil {
			return err
		}
		if req := req.GetInitial(); req != nil {
			if err := aps.Send(&pubsublitepb.PartitionAssignment{
				Partitions: []int64{1, 2},
			}); err != nil {
				return err
			}
			continue
		}
		if req := req.GetAck(); req != nil {
			continue
		}
	}
}
