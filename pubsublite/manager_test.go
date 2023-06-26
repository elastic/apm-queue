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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	pubsublitepb "cloud.google.com/go/pubsublite/apiv1/pubsublitepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	apmqueue "github.com/elastic/apm-queue"
)

func TestNewManager(t *testing.T) {
	_, err := NewManager(ManagerConfig{})
	require.Error(t, err)
	assert.EqualError(t, err, "pubsublite: invalid manager config: "+strings.Join([]string{
		"pubsublite: project must be set",
		"pubsublite: region must be set",
		"pubsublite: logger must be set",
	}, "\n"))

	_, commonConfig := newTestAdminService(t)
	_, monitoringClientOpts := newTestMetricService(t)
	manager, err := NewManager(ManagerConfig{CommonConfig: commonConfig, MonitoringClientOptions: monitoringClientOpts})
	require.NoError(t, err)
	require.NotNil(t, manager)
	require.NoError(t, manager.Close())
}

func TestNewManagerDefaultProject(t *testing.T) {
	tempdir := t.TempDir()
	credentialsPath := filepath.Join(tempdir, "credentials.json")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credentialsPath)

	_, err := NewManager(ManagerConfig{})
	require.Error(t, err)
	assert.EqualError(t, err,
		"pubsublite: failed to set config from environment: failed to read $GOOGLE_APPLICATION_CREDENTIALS: "+
			"open "+credentialsPath+": no such file or directory",
	)

	err = os.WriteFile(credentialsPath, []byte("jason"), 0644)
	require.NoError(t, err)
	_, err = NewManager(ManagerConfig{})
	require.Error(t, err)
	assert.EqualError(t, err,
		"pubsublite: failed to set config from environment: failed to parse $GOOGLE_APPLICATION_CREDENTIALS: "+
			"invalid character 'j' looking for beginning of value",
	)

	err = os.WriteFile(credentialsPath, []byte(`{"project_id":"default_project_id"}`), 0644)
	require.NoError(t, err)
	_, err = NewManager(ManagerConfig{})
	require.Error(t, err)
	assert.EqualError(t, err, "pubsublite: invalid manager config: "+strings.Join([]string{
		"pubsublite: region must be set",
		"pubsublite: logger must be set",
	}, "\n"))
}

func TestManagerCreateReservation(t *testing.T) {
	server, commonConfig := newTestAdminService(t)
	core, observedLogs := observer.New(zapcore.DebugLevel)
	commonConfig.Logger = zap.New(core)
	_, monitoringClientOpts := newTestMetricService(t)
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig, MonitoringClientOptions: monitoringClientOpts})
	require.NoError(t, err)
	defer m.Close()

	server.reservation = &pubsublitepb.Reservation{Name: "foo"}
	err = m.CreateReservation(context.Background(), "reservation_name", 123)
	require.NoError(t, err)

	assert.Equal(t, "projects/project/locations/region-1", server.createReservationRequest.Parent)
	assert.Equal(t, "reservation_name", server.createReservationRequest.ReservationId)
	assert.Equal(t, int64(123), server.createReservationRequest.Reservation.GetThroughputCapacity())

	errorInfo, err := anypb.New(&errdetails.ErrorInfo{Reason: "RESOURCE_ALREADY_EXISTS"})
	require.NoError(t, err)
	server.err = status.ErrorProto(&spb.Status{
		Code:    int32(codes.AlreadyExists),
		Message: "reservation already exists",
		Details: []*anypb.Any{errorInfo},
	})
	err = m.CreateReservation(context.Background(), "reservation_name", 123)
	require.NoError(t, err)

	server.err = status.Errorf(codes.PermissionDenied, "nope")
	err = m.CreateReservation(context.Background(), "reservation_name", 123)
	require.Error(t, err)
	assert.EqualError(t, err,
		`failed to create pubsublite reservation "reservation_name": rpc error: code = PermissionDenied desc = nope`,
	)

	assert.Equal(t, []observer.LoggedEntry{{
		Entry: zapcore.Entry{
			Level:   zapcore.InfoLevel,
			Message: "created pubsublite reservation",
		},
		Context: []zapcore.Field{
			zap.String("reservation", "reservation_name"),
		},
	}, {
		Entry: zapcore.Entry{
			Level:   zapcore.DebugLevel,
			Message: "pubsublite reservation already exists",
		},
		Context: []zapcore.Field{
			zap.String("reservation", "reservation_name"),
		},
	}}, observedLogs.AllUntimed())
}

func TestManagerCreateSubscription(t *testing.T) {
	server, commonConfig := newTestAdminService(t)
	core, observedLogs := observer.New(zapcore.DebugLevel)
	commonConfig.Logger = zap.New(core)
	_, monitoringClientOpts := newTestMetricService(t)
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig, MonitoringClientOptions: monitoringClientOpts})
	require.NoError(t, err)
	defer m.Close()

	server.subscription = &pubsublitepb.Subscription{Name: "foo"}
	err = m.CreateSubscription(context.Background(), "subscription_name", "topic_name", true)
	require.NoError(t, err)

	assert.Equal(t, "projects/project/locations/region-1", server.createSubscriptionRequest.Parent)
	assert.Equal(t, "subscription_name", server.createSubscriptionRequest.SubscriptionId)
	assert.Equal(t, false, server.createSubscriptionRequest.SkipBacklog)

	errorInfo, err := anypb.New(&errdetails.ErrorInfo{Reason: "RESOURCE_ALREADY_EXISTS"})
	require.NoError(t, err)
	server.err = status.ErrorProto(&spb.Status{
		Code:    int32(codes.AlreadyExists),
		Message: "subscription already exists",
		Details: []*anypb.Any{errorInfo},
	})
	err = m.CreateSubscription(context.Background(), "subscription_name", "topic_name", true)
	require.NoError(t, err)

	server.err = status.Errorf(codes.PermissionDenied, "nope")
	err = m.CreateSubscription(context.Background(), "subscription_name", "topic_name", false)
	require.Error(t, err)
	assert.EqualError(t, err,
		`failed to create pubsublite subscription "subscription_name" for topic "topic_name": `+
			`rpc error: code = PermissionDenied desc = nope`,
	)

	assert.Equal(t, []observer.LoggedEntry{{
		Entry: zapcore.Entry{
			Level:   zapcore.InfoLevel,
			Message: "created pubsublite subscription",
		},
		Context: []zapcore.Field{
			zap.String("subscription", "subscription_name"),
			zap.String("topic", "topic_name"),
			zap.Bool("deliver_immediately", true),
		},
	}, {
		Entry: zapcore.Entry{
			Level:   zapcore.DebugLevel,
			Message: "pubsublite subscription already exists",
		},
		Context: []zapcore.Field{
			zap.String("subscription", "subscription_name"),
			zap.String("topic", "topic_name"),
		},
	}}, observedLogs.AllUntimed())
}

func TestManagerDeleteReservation(t *testing.T) {
	server, commonConfig := newTestAdminService(t)
	core, observedLogs := observer.New(zapcore.DebugLevel)
	commonConfig.Logger = zap.New(core)
	_, monitoringClientOpts := newTestMetricService(t)
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig, MonitoringClientOptions: monitoringClientOpts})
	require.NoError(t, err)
	defer m.Close()

	err = m.DeleteReservation(context.Background(), "reservation_name")
	require.NoError(t, err)
	assert.Equal(t, "projects/project/locations/region-1/reservations/reservation_name", server.deleteReservationRequest.Name)

	errorInfo, err := anypb.New(&errdetails.ErrorInfo{Reason: "RESOURCE_NOT_EXIST"})
	require.NoError(t, err)
	server.err = status.ErrorProto(&spb.Status{
		Code:    int32(codes.NotFound),
		Message: "reservation not found",
		Details: []*anypb.Any{errorInfo},
	})
	err = m.DeleteReservation(context.Background(), "reservation_name")
	require.NoError(t, err)

	server.err = status.Errorf(codes.PermissionDenied, "nope")
	err = m.DeleteReservation(context.Background(), "reservation_name")
	require.Error(t, err)
	assert.EqualError(t, err,
		`failed to delete pubsublite reservation "reservation_name": `+
			`rpc error: code = PermissionDenied desc = nope`,
	)

	assert.Equal(t, []observer.LoggedEntry{{
		Entry: zapcore.Entry{
			Level:   zapcore.InfoLevel,
			Message: "deleted pubsublite reservation",
		},
		Context: []zapcore.Field{
			zap.String("reservation", "reservation_name"),
		},
	}, {
		Entry: zapcore.Entry{
			Level:   zapcore.DebugLevel,
			Message: "pubsublite reservation does not exist",
		},
		Context: []zapcore.Field{
			zap.String("reservation", "reservation_name"),
		},
	}}, observedLogs.AllUntimed())
}

func TestManagerDeleteTopic(t *testing.T) {
	server, commonConfig := newTestAdminService(t)
	core, observedLogs := observer.New(zapcore.DebugLevel)
	commonConfig.Logger = zap.New(core)
	_, monitoringClientOpts := newTestMetricService(t)
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig, MonitoringClientOptions: monitoringClientOpts})
	require.NoError(t, err)
	defer m.Close()

	err = m.DeleteTopic(context.Background(), "topic_name")
	require.NoError(t, err)
	assert.Equal(t, "projects/project/locations/region-1/topics/topic_name", server.deleteTopicRequest.Name)

	errorInfo, err := anypb.New(&errdetails.ErrorInfo{Reason: "RESOURCE_NOT_EXIST"})
	require.NoError(t, err)
	server.err = status.ErrorProto(&spb.Status{
		Code:    int32(codes.NotFound),
		Message: "topic not found",
		Details: []*anypb.Any{errorInfo},
	})
	err = m.DeleteTopic(context.Background(), "topic_name")
	require.NoError(t, err)

	server.err = status.Errorf(codes.PermissionDenied, "nope")
	err = m.DeleteTopic(context.Background(), "topic_name")
	require.Error(t, err)
	assert.EqualError(t, err,
		`failed to delete pubsublite topic "topic_name": `+
			`rpc error: code = PermissionDenied desc = nope`,
	)

	assert.Equal(t, []observer.LoggedEntry{{
		Entry: zapcore.Entry{
			Level:   zapcore.InfoLevel,
			Message: "deleted pubsublite topic",
		},
		Context: []zapcore.Field{
			zap.String("topic", "topic_name"),
		},
	}, {
		Entry: zapcore.Entry{
			Level:   zapcore.DebugLevel,
			Message: "pubsublite topic does not exist",
		},
		Context: []zapcore.Field{
			zap.String("topic", "topic_name"),
		},
	}}, observedLogs.AllUntimed())
}

func TestManagerDeleteSubscription(t *testing.T) {
	server, commonConfig := newTestAdminService(t)
	core, observedLogs := observer.New(zapcore.DebugLevel)
	commonConfig.Logger = zap.New(core)
	_, monitoringClientOpts := newTestMetricService(t)
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig, MonitoringClientOptions: monitoringClientOpts})
	require.NoError(t, err)
	defer m.Close()

	err = m.DeleteSubscription(context.Background(), "subscription_name")
	require.NoError(t, err)
	assert.Equal(t, "projects/project/locations/region-1/subscriptions/subscription_name", server.deleteSubscriptionRequest.Name)

	errorInfo, err := anypb.New(&errdetails.ErrorInfo{Reason: "RESOURCE_NOT_EXIST"})
	require.NoError(t, err)
	server.err = status.ErrorProto(&spb.Status{
		Code:    int32(codes.NotFound),
		Message: "subscription not found",
		Details: []*anypb.Any{errorInfo},
	})
	err = m.DeleteSubscription(context.Background(), "subscription_name")
	require.NoError(t, err)

	server.err = status.Errorf(codes.PermissionDenied, "nope")
	err = m.DeleteSubscription(context.Background(), "subscription_name")
	require.Error(t, err)
	assert.EqualError(t, err,
		`failed to delete pubsublite subscription "subscription_name": `+
			`rpc error: code = PermissionDenied desc = nope`,
	)

	assert.Equal(t, []observer.LoggedEntry{{
		Entry: zapcore.Entry{
			Level:   zapcore.InfoLevel,
			Message: "deleted pubsublite subscription",
		},
		Context: []zapcore.Field{
			zap.String("subscription", "subscription_name"),
		},
	}, {
		Entry: zapcore.Entry{
			Level:   zapcore.DebugLevel,
			Message: "pubsublite subscription does not exist",
		},
		Context: []zapcore.Field{
			zap.String("subscription", "subscription_name"),
		},
	}}, observedLogs.AllUntimed())
}

func TestManagerListReservations(t *testing.T) {
	server, commonConfig := newTestAdminService(t)
	_, monitoringClientOpts := newTestMetricService(t)
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig, MonitoringClientOptions: monitoringClientOpts})
	require.NoError(t, err)
	defer m.Close()

	reservations, err := m.ListReservations(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"reservation_one", "reservation_two", "reservation_three"}, reservations)

	server.err = status.Errorf(codes.PermissionDenied, "nope")
	_, err = m.ListReservations(context.Background())
	require.Error(t, err)
	assert.EqualError(t, err, `pubsublite: failed listing reservations: rpc error: code = PermissionDenied desc = nope`)
}

func TestManagerListReservationTopics(t *testing.T) {
	server, commonConfig := newTestAdminService(t)
	_, monitoringClientOpts := newTestMetricService(t)
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig, MonitoringClientOptions: monitoringClientOpts})
	require.NoError(t, err)
	defer m.Close()

	topics, err := m.ListReservationTopics(context.Background(), "reservation_name")
	require.NoError(t, err)
	assert.Equal(t, []string{"topic_one", "topic_two", "topic_three"}, topics)
	require.Len(t, server.listReservationTopicsRequests, 2)
	assert.Equal(t,
		"projects/project/locations/region-1/reservations/reservation_name",
		server.listReservationTopicsRequests[0].Name,
	)

	server.err = status.Errorf(codes.PermissionDenied, "nope")
	_, err = m.ListReservationTopics(context.Background(), "reservation_name")
	require.Error(t, err)
	assert.EqualError(t, err, `pubsublite: failed listing topics for reservation "reservation_name": `+
		`rpc error: code = PermissionDenied desc = nope`)
}

func TestManagerListTopicSubscriptions(t *testing.T) {
	server, commonConfig := newTestAdminService(t)
	_, monitoringClientOpts := newTestMetricService(t)
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig, MonitoringClientOptions: monitoringClientOpts})
	require.NoError(t, err)
	defer m.Close()

	subscriptions, err := m.ListTopicSubscriptions(context.Background(), "topic_name")
	require.NoError(t, err)
	assert.Equal(t, []string{"subscription_one", "subscription_two", "subscription_three"}, subscriptions)
	require.Len(t, server.listTopicSubscriptionsRequests, 2)
	assert.Equal(t, "projects/project/locations/region-1/topics/topic_name", server.listTopicSubscriptionsRequests[0].Name)

	server.err = status.Errorf(codes.PermissionDenied, "nope")
	_, err = m.ListTopicSubscriptions(context.Background(), "topic_name")
	require.Error(t, err)
	assert.EqualError(t, err, `pubsublite: failed listing subscriptions for topic "topic_name": `+
		`rpc error: code = PermissionDenied desc = nope`)
}

func newTestAdminService(t testing.TB) (*adminServiceServer, CommonConfig) {
	s := grpc.NewServer()
	t.Cleanup(s.Stop)
	server := &adminServiceServer{}
	pubsublitepb.RegisterAdminServiceServer(s, server)

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { lis.Close() })

	go s.Serve(lis)

	return server, CommonConfig{
		Project: "project",
		Region:  "region-1",
		Logger:  zap.NewNop(),
		ClientOptions: []option.ClientOption{
			option.WithGRPCDialOption(grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor())),
			option.WithGRPCDialOption(grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor())),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithEndpoint(lis.Addr().String()),
			option.WithoutAuthentication(),
		},
	}
}

type adminServiceServer struct {
	pubsublitepb.UnimplementedAdminServiceServer

	createTopicRequest        *pubsublitepb.CreateTopicRequest
	createReservationRequest  *pubsublitepb.CreateReservationRequest
	createSubscriptionRequest *pubsublitepb.CreateSubscriptionRequest

	listReservationsRequests       []*pubsublitepb.ListReservationsRequest
	listReservationTopicsRequests  []*pubsublitepb.ListReservationTopicsRequest
	listTopicSubscriptionsRequests []*pubsublitepb.ListTopicSubscriptionsRequest

	deleteTopicRequest        *pubsublitepb.DeleteTopicRequest
	deleteReservationRequest  *pubsublitepb.DeleteReservationRequest
	deleteSubscriptionRequest *pubsublitepb.DeleteSubscriptionRequest

	topic        *pubsublitepb.Topic
	reservation  *pubsublitepb.Reservation
	subscription *pubsublitepb.Subscription

	err error
}

func (s *adminServiceServer) CreateTopic(
	ctx context.Context,
	req *pubsublitepb.CreateTopicRequest,
) (*pubsublitepb.Topic, error) {
	s.createTopicRequest = req
	return s.topic, s.err
}

func (s *adminServiceServer) CreateReservation(
	ctx context.Context,
	req *pubsublitepb.CreateReservationRequest,
) (*pubsublitepb.Reservation, error) {
	s.createReservationRequest = req
	return s.reservation, s.err
}

func (s *adminServiceServer) CreateSubscription(
	ctx context.Context,
	req *pubsublitepb.CreateSubscriptionRequest,
) (*pubsublitepb.Subscription, error) {
	s.createSubscriptionRequest = req
	return s.subscription, s.err
}

func (s *adminServiceServer) ListReservations(
	ctx context.Context,
	req *pubsublitepb.ListReservationsRequest,
) (*pubsublitepb.ListReservationsResponse, error) {
	s.listReservationsRequests = append(s.listReservationsRequests, req)

	var resp pubsublitepb.ListReservationsResponse
	switch len(s.listReservationsRequests) {
	case 1:
		resp.NextPageToken = "one"
		resp.Reservations = []*pubsublitepb.Reservation{{Name: "reservation_one"}}
	case 2:
		resp.NextPageToken = ""
		resp.Reservations = []*pubsublitepb.Reservation{{Name: "reservation_two"}, {Name: "reservation_three"}}
	}
	return &resp, s.err
}

func (s *adminServiceServer) ListReservationTopics(
	ctx context.Context,
	req *pubsublitepb.ListReservationTopicsRequest,
) (*pubsublitepb.ListReservationTopicsResponse, error) {
	s.listReservationTopicsRequests = append(s.listReservationTopicsRequests, req)

	var resp pubsublitepb.ListReservationTopicsResponse
	switch len(s.listReservationTopicsRequests) {
	case 1:
		resp.NextPageToken = "one"
		resp.Topics = []string{"topic_one"}
	case 2:
		resp.NextPageToken = ""
		resp.Topics = []string{"topic_two", "topic_three"}
	}
	return &resp, s.err
}

func (s *adminServiceServer) ListTopicSubscriptions(
	ctx context.Context,
	req *pubsublitepb.ListTopicSubscriptionsRequest,
) (*pubsublitepb.ListTopicSubscriptionsResponse, error) {
	s.listTopicSubscriptionsRequests = append(s.listTopicSubscriptionsRequests, req)

	var resp pubsublitepb.ListTopicSubscriptionsResponse
	switch len(s.listTopicSubscriptionsRequests) {
	case 1:
		resp.NextPageToken = "one"
		resp.Subscriptions = []string{"subscription_one"}
	case 2:
		resp.NextPageToken = ""
		resp.Subscriptions = []string{"subscription_two", "subscription_three"}
	}
	return &resp, s.err
}

func (s *adminServiceServer) DeleteTopic(
	ctx context.Context,
	req *pubsublitepb.DeleteTopicRequest,
) (*emptypb.Empty, error) {
	s.deleteTopicRequest = req
	return &emptypb.Empty{}, s.err
}

func (s *adminServiceServer) DeleteReservation(
	ctx context.Context,
	req *pubsublitepb.DeleteReservationRequest,
) (*emptypb.Empty, error) {
	s.deleteReservationRequest = req
	return &emptypb.Empty{}, s.err
}

func (s *adminServiceServer) DeleteSubscription(
	ctx context.Context,
	req *pubsublitepb.DeleteSubscriptionRequest,
) (*emptypb.Empty, error) {
	s.deleteSubscriptionRequest = req
	return &emptypb.Empty{}, s.err
}

func newTestMetricService(t testing.TB) (*metricServiceServer, []option.ClientOption) {
	s := grpc.NewServer()
	t.Cleanup(s.Stop)
	server := &metricServiceServer{}
	monitoringpb.RegisterMetricServiceServer(s, server)

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { lis.Close() })

	go s.Serve(lis)
	return server, []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor())),
		option.WithGRPCDialOption(grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor())),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithEndpoint(lis.Addr().String()),
		option.WithoutAuthentication(),
	}
}

type metricServiceServer struct {
	monitoringpb.UnimplementedMetricServiceServer

	TimeSeriesFilter string
}

func (s *metricServiceServer) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
	s.TimeSeriesFilter = req.Filter
	return &monitoringpb.ListTimeSeriesResponse{
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric: &metricpb.Metric{
					Type: "pubsublite.googleapis.com/subscription/backlog_message_count",
				},
				Resource: &monitoredres.MonitoredResource{
					Labels: map[string]string{
						"subscription_id": "topic1+consumer1",
						"partition":       "1",
					},
				},
				Metadata:   nil,
				MetricKind: metricpb.MetricDescriptor_GAUGE,
				ValueType:  metricpb.MetricDescriptor_INT64,
				Points:     []*monitoringpb.Point{{Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: 1}}}},
				Unit:       "",
			},
			{
				Metric: &metricpb.Metric{
					Type: "pubsublite.googleapis.com/subscription/backlog_message_count",
				},
				Resource: &monitoredres.MonitoredResource{
					Labels: map[string]string{
						"subscription_id": "topic2+consumer1",
						"partition":       "2",
					},
				},
				Metadata:   nil,
				MetricKind: metricpb.MetricDescriptor_GAUGE,
				ValueType:  metricpb.MetricDescriptor_INT64,
				Points:     []*monitoringpb.Point{{Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: 2}}}},
				Unit:       "",
			},
		},
	}, nil
}

func TestManagerMetrics(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	defer tp.Shutdown(context.Background())
	defer mp.Shutdown(context.Background())

	_, commonConfig := newTestAdminService(t)
	testMetricService, monitoringClientOpts := newTestMetricService(t)

	core, _ := observer.New(zapcore.DebugLevel)
	commonConfig.Logger = zap.New(core)
	commonConfig.TracerProvider = tp
	commonConfig.MeterProvider = mp
	m, err := NewManager(ManagerConfig{CommonConfig: commonConfig, MonitoringClientOptions: monitoringClientOpts})
	require.NoError(t, err)
	t.Cleanup(func() { m.Close() })

	registration, err := m.MonitorConsumerLag([]apmqueue.TopicConsumer{
		{
			Topic:    "topic1",
			Consumer: "consumer1",
		},
		{
			Topic:    "topic2",
			Consumer: "consumer1",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { registration.Unregister() })

	rm := metricdata.ResourceMetrics{}
	err = reader.Collect(context.Background(), &rm)
	require.NoError(t, err)
	require.Len(t, rm.ScopeMetrics, 1)
	sort.Slice(rm.ScopeMetrics, func(i, j int) bool {
		return rm.ScopeMetrics[i].Scope.Name < rm.ScopeMetrics[j].Scope.Name
	})
	assert.Equal(t, "github.com/elastic/apm-queue/pubsublite", rm.ScopeMetrics[0].Scope.Name)

	metrics := rm.ScopeMetrics[0].Metrics
	require.Len(t, metrics, 1)
	assert.Equal(t, "consumer_group_lag", metrics[0].Name)
	metricdatatest.AssertAggregationsEqual(t, metricdata.Gauge[int64]{
		DataPoints: []metricdata.DataPoint[int64]{
			{
				Attributes: attribute.NewSet(
					attribute.String("topic", "topic1"),
					attribute.String("group", "consumer1"),
					attribute.Int("partition", 1),
				),
				Value: 1,
			},
			{
				Attributes: attribute.NewSet(
					attribute.String("topic", "topic2"),
					attribute.String("group", "consumer1"),
					attribute.Int("partition", 2),
				),
				Value: 2,
			},
		},
	}, metrics[0].Data, metricdatatest.IgnoreTimestamp())

	assert.Equal(t, testMetricService.TimeSeriesFilter, "metric.type = \"pubsublite.googleapis.com/subscription/backlog_message_count\""+
		" AND resource.labels.location = \"region-1\""+
		" AND (resource.labels.subscription_id = \"topic1+consumer1\" OR resource.labels.subscription_id = \"topic2+consumer1\")")
}
