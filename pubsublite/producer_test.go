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
	"net"
	"sync"
	"testing"

	"cloud.google.com/go/pubsublite/apiv1/pubsublitepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	apmqueue "github.com/elastic/apm-queue/v2"
)

func TestNewProducer(t *testing.T) {
	_, err := NewProducer(ProducerConfig{})
	assert.Error(t, err)
}

func TestProducerProduce(t *testing.T) {
	server, config := newPublisherService(t)
	config.Sync = true

	var requestTopic string
	var requestMessages []*pubsublitepb.PubSubMessage
	server.process = func(ir *pubsublitepb.InitialPublishRequest, req *pubsublitepb.MessagePublishRequest) (
		*pubsublitepb.MessagePublishResponse, error,
	) {
		requestTopic = ir.GetTopic()
		requestMessages = req.GetMessages()
		return &pubsublitepb.MessagePublishResponse{}, nil
	}

	t.Helper()
	p, err := NewProducer(config)
	require.NoError(t, err)
	defer p.Close()

	err = p.Produce(context.Background(), apmqueue.Record{
		Topic: "topic_name",
		Value: []byte("abc123"),
	})
	require.NoError(t, err)

	parent := "projects/project/locations/region-1/topics/"
	topicName := "name_space-topic_name"
	assert.Equal(t, parent+topicName, requestTopic)
	require.Len(t, requestMessages, 1)
	assert.Equal(t, []byte("abc123"), requestMessages[0].Data)
}

func TestProducerConcurrentClose(t *testing.T) {
	producer := Producer{
		closed:    make(chan struct{}),
		responses: make(chan []resTopic),
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, producer.Close())
		}()
	}
	wg.Wait()
}

func newPublisherService(t testing.TB) (*publisherServiceServer, ProducerConfig) {
	s := grpc.NewServer()
	t.Cleanup(s.Stop)
	server := &publisherServiceServer{
		process: func(ir *pubsublitepb.InitialPublishRequest, req *pubsublitepb.MessagePublishRequest) (
			*pubsublitepb.MessagePublishResponse, error,
		) {
			return &pubsublitepb.MessagePublishResponse{}, nil
		},
	}
	pubsublitepb.RegisterAdminServiceServer(s, server)
	pubsublitepb.RegisterPublisherServiceServer(s, server)

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { lis.Close() })
	go s.Serve(lis)

	return server, ProducerConfig{
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
	}
}

type publisherServiceServer struct {
	pubsublitepb.UnimplementedAdminServiceServer
	pubsublitepb.UnimplementedPublisherServiceServer
	process func(*pubsublitepb.InitialPublishRequest, *pubsublitepb.MessagePublishRequest) (
		*pubsublitepb.MessagePublishResponse, error,
	)
}

func (s *publisherServiceServer) GetTopicPartitions(
	ctx context.Context, req *pubsublitepb.GetTopicPartitionsRequest,
) (*pubsublitepb.TopicPartitions, error) {
	return &pubsublitepb.TopicPartitions{PartitionCount: 1}, nil
}

func (s *publisherServiceServer) Publish(ps pubsublitepb.PublisherService_PublishServer) error {
	var ir *pubsublitepb.InitialPublishRequest
	for {
		req, err := ps.Recv()
		if err != nil {
			return err
		}
		if req := req.GetInitialRequest(); req != nil {
			if err := ps.Send(&pubsublitepb.PublishResponse{
				ResponseType: &pubsublitepb.PublishResponse_InitialResponse{
					InitialResponse: &pubsublitepb.InitialPublishResponse{},
				},
			}); err != nil {
				return err
			}
			ir = req
			continue
		}
		res, err := s.process(ir, req.GetMessagePublishRequest())
		if err != nil {
			return err
		}
		if err := ps.Send(&pubsublitepb.PublishResponse{
			ResponseType: &pubsublitepb.PublishResponse_MessageResponse{
				MessageResponse: res,
			},
		}); err != nil {
			return err
		}
	}
}
