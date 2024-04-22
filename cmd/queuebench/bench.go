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
	"fmt"
	"log"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue/v2"
	"github.com/elastic/apm-queue/v2/kafka"
)

type bench struct {
	Brokers         []string
	ConsumerGroupID string
	Logger          *zap.Logger
	Partitions      int
	TopicNamespace  string
	Topics          []apmqueue.Topic
	MetaConsumer    bool
	MaxPollRecords  int

	mp metric.MeterProvider
	tp trace.TracerProvider

	c apmqueue.Consumer
	m *kafka.Manager
	p *kafka.Producer
}

func (b *bench) Setup(ctx context.Context) (err error) {
	kafkaCommonCfg := kafka.CommonConfig{
		Brokers:   b.Brokers,
		Namespace: b.TopicNamespace,

		Logger:         b.Logger,
		TracerProvider: b.tp,
		MeterProvider:  b.mp,
	}

	mngrCfg := kafka.ManagerConfig{CommonConfig: kafkaCommonCfg}
	mngrCfg.CommonConfig.ClientID = "queuebench-manager"
	mngrCfg.CommonConfig.Logger = b.Logger.With(zap.String("role", "manager"))
	b.m, err = kafka.NewManager(mngrCfg)
	if err != nil {
		return fmt.Errorf("cannot create kafka manager: %w", err)
	}

	if err = b.m.Healthy(ctx); err != nil {
		return fmt.Errorf("cluster health check failed: %w", err)
	}
	log.Println("cluster confirmed healthy")

	log.Printf("creating kafka topics: %v", b.Topics)
	topicsCfg := kafka.TopicCreatorConfig{PartitionCount: b.Partitions}
	if err = createTopics(ctx, b.m, topicsCfg, b.Topics); err != nil {
		return fmt.Errorf("cannot create topics: %w", err)
	}

	b.c, err = createConsumer(kafkaCommonCfg, b.Topics, b.ConsumerGroupID,
		b.MetaConsumer, b.MaxPollRecords,
	)
	if err != nil {
		return fmt.Errorf("cannot create consumer: %w", err)
	}

	b.p, err = createProducer(kafkaCommonCfg)
	if err != nil {
		return fmt.Errorf("cannot create producer: %w", err)
	}
	return nil
}

func (b *bench) Teardown(ctx context.Context) error {
	log.Printf("deleting benchmark kafka topics: %v", b.Topics)
	if err := deleteTopics(ctx, b.m, b.Topics); err != nil {
		return fmt.Errorf("teardown not completed: %w", err)
	}

	return nil
}

func createTopics(ctx context.Context, mngr *kafka.Manager, cfg kafka.TopicCreatorConfig, topics []apmqueue.Topic) error {
	creator, err := mngr.NewTopicCreator(cfg)
	if err != nil {
		return fmt.Errorf("cannot instantiate topic creator: %w", err)
	}

	for _, topic := range topics {
		err = creator.CreateTopics(ctx, topic)
		if err != nil {
			return fmt.Errorf("cannot create topics: %w", err)
		}
	}

	return nil
}

func deleteTopics(ctx context.Context, mngr *kafka.Manager, topics []apmqueue.Topic) error {
	err := mngr.DeleteTopics(ctx, topics...)
	if err != nil {
		return fmt.Errorf("cannot delete topics: %w", err)
	}

	return nil
}

type dummyProcessor struct{}

func (d dummyProcessor) Process(context.Context, apmqueue.Record) error {
	return nil
}

func createConsumer(commonCfg kafka.CommonConfig,
	topics []apmqueue.Topic,
	groupID string,
	independentClients bool,
	maxPoll int,
) (apmqueue.Consumer, error) {
	cfg := kafka.ConsumerConfig{
		CommonConfig:   commonCfg,
		GroupID:        groupID,
		Processor:      dummyProcessor{},
		Topics:         topics,
		MaxPollRecords: maxPoll,
	}
	cfg.CommonConfig.ClientID = "queuebench-consumer"
	cfg.CommonConfig.Logger = commonCfg.Logger.With(zap.String("role", "consumer"))
	if independentClients {
		var mc metaConsumer
		mc.consumers = make([]apmqueue.Consumer, 0, len(topics))
		for _, t := range topics {
			c := cfg
			cfg.Topics = []apmqueue.Topic{t}
			consumer, err := kafka.NewConsumer(c)
			if err != nil {
				return nil, err
			}
			mc.consumers = append(mc.consumers, consumer)
		}
		return &mc, nil
	}
	return kafka.NewConsumer(cfg)
}

func createProducer(commonCfg kafka.CommonConfig) (*kafka.Producer, error) {
	cfg := kafka.ProducerConfig{
		CommonConfig: commonCfg,
	}
	cfg.CommonConfig.ClientID = "queuebench-producer"
	cfg.CommonConfig.Logger = commonCfg.Logger.With(zap.String("role", "producer"))
	return kafka.NewProducer(cfg)
}
