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

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/elastic/apm-data/model"
	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/codec/json"
	"github.com/elastic/apm-queue/kafka"
	"github.com/elastic/apm-queue/pubsublite"
	"github.com/elastic/apm-queue/queuecontext"
)

var (
	defaultReplay         = 100
	defaultProducerCount  = 5
	defaultProducerKey    = "producer_id"
	defaultProducerFormat = "producer-%s"
	defaultOutput         = "kafka"
	defaultPrefix         = "prefix"
)

type loadgen struct {
	logger        *zap.Logger
	replay        int
	producerCount int
	producerKey   string
	producerFmt   string
	prefix        string
	outputAddress string
	outputType    string
}

func newLoadgen(opt ...func(*loadgen) error) (*loadgen, error) {
	logger, err := zap.NewProduction(zap.AddCaller())
	if err != nil {
		return nil, err
	}
	lg := &loadgen{
		logger:        logger.Named("loadgen"),
		prefix:        defaultPrefix,
		replay:        defaultReplay,
		outputType:    defaultOutput,
		producerCount: defaultProducerCount,
		producerKey:   defaultProducerKey,
		producerFmt:   defaultProducerFormat,
	}
	for _, f := range opt {
		if err := f(lg); err != nil {
			return nil, err
		}
	}
	return lg, nil
}

func (lg *loadgen) run(ctx context.Context) error {
	defer lg.logger.Sync()
	lg.logger.Info("STARTING")

	var p model.BatchProcessor
	var err error
	switch lg.outputType {
	case "kafka":
		p, err = lg.kafkaProcessor()
	case "pubsublite":
		p, err = lg.pubsubProcessor()
	}
	if err != nil {
		return fmt.Errorf("failed to create processor: %s: %w", lg.outputType, err)
	}
	if err := lg.process(ctx, p); err != nil {
		return err
	}

	lg.logger.Info("DONE")
	<-ctx.Done()
	return nil
}

func (lg *loadgen) process(ctx context.Context, p model.BatchProcessor) error {
	for j := 0; j < lg.producerCount; j++ {
		go func(j int) {
			producerValue := fmt.Sprintf(lg.producerFmt, j)
			ctx := queuecontext.WithMetadata(ctx, map[string]string{
				lg.producerKey: producerValue,
			})
			for i := 0; i < lg.replay; i++ {
				lg.logger.Info("Sending batch",
					zap.Int("count", i),
					zap.String(lg.producerKey, producerValue),
				)
				batch := newBatch()
				if err := p.ProcessBatch(ctx, &batch); err != nil {
					lg.logger.Error("failed to process batch", zap.Error(err))
				}
			}
		}(j)
	}
	return nil
}

func (lg *loadgen) kafkaProcessor() (model.BatchProcessor, error) {
	return kafka.NewProducer(kafka.ProducerConfig{
		CommonConfig: kafka.CommonConfig{
			Brokers: []string{lg.outputAddress},
			Logger:  lg.logger.Named("producer"),
			TLS:     &tls.Config{InsecureSkipVerify: true},
		},
		Encoder:     json.JSON{},
		TopicRouter: apmqueue.NewEventTypeTopicRouter(lg.prefix),
	})
}

func (lg *loadgen) pubsubProcessor() (model.BatchProcessor, error) {
	project := os.Getenv("GCLOUD_PROJECT_ID")
	region := os.Getenv("GCLOUD_REGION")
	router := apmqueue.NewEventTypeTopicRouter(lg.prefix)
	return pubsublite.NewProducer(pubsublite.ProducerConfig{
		CommonConfig: pubsublite.CommonConfig{
			Logger:  lg.logger.Named("producer"),
			Project: project,
			Region:  region,
		},
		Encoder:     json.JSON{},
		TopicRouter: router,
	})
}

func newBatch() model.Batch {
	traceID := uuid.NewString()
	event := model.Event{
		Outcome:  "success",
		Duration: time.Second,
	}
	trace := model.Trace{ID: traceID}
	txID := "123"
	runtimeVersion := runtime.Version()
	service := model.Service{
		Language: model.Language{
			Name:    "go",
			Version: runtimeVersion,
		},
		Runtime: model.Runtime{
			Name:    "gc",
			Version: runtimeVersion,
		},
		Node:        model.ServiceNode{Name: "loadgen"},
		Name:        "loadgen",
		Version:     "0.1.0",
		Environment: "dev",
	}
	agent := model.Agent{
		Name:    "go",
		Version: "2.2.0",
	}
	host := model.Host{
		OS: model.OS{
			Name:     runtime.GOOS,
			Platform: runtime.GOARCH,
		},
		Architecture: runtime.GOARCH,
	}
	return model.Batch{
		{
			Timestamp: time.Now(),
			Processor: model.TransactionProcessor,
			Event:     event,
			Trace:     trace,
			Service:   service,
			Agent:     agent,
			Host:      host,
			Transaction: &model.Transaction{
				Type:    "type",
				Name:    "tx",
				ID:      txID,
				Sampled: true,
				Root:    true,

				RepresentativeCount: 1,
			},
		},
		{
			Timestamp: time.Now(),
			Processor: model.SpanProcessor,
			Trace:     trace,
			Parent:    model.Parent{ID: txID},
			Service:   service,
			Agent:     agent,
			Host:      host,
			Event:     event,
			Span: &model.Span{
				ID:   "1234",
				Name: "span",
				Type: "type",
				DestinationService: &model.DestinationService{
					Type:     "db",
					Resource: "redis",
				},
				RepresentativeCount: 1,
			},
		},
		{
			Timestamp: time.Now(),
			Trace:     trace,
			Service:   service,
			Agent:     agent,
			Host:      host,
			Error: &model.Error{
				ID:          "foo",
				GroupingKey: "group",
				Type:        "type",
				Culprit:     "culprit",
				Log: &model.ErrorLog{
					Level:   "error",
					Message: "error message log",
				},
			},
			Message:   "error message log",
			Processor: model.ErrorProcessor,
		},
		{
			Timestamp: time.Now(),
			Metricset: &model.Metricset{
				Name: "app",
			},
			Agent:     agent,
			Service:   service,
			Processor: model.MetricsetProcessor,
		},
		{
			Timestamp: time.Now(),
			Service:   service,
			Agent:     agent,
			Host:      host,
			Trace:     trace,
			Log:       model.Log{Level: "info", Logger: "logger"},
			Message:   "A log message",
			Processor: model.LogProcessor,
		},
	}
}
