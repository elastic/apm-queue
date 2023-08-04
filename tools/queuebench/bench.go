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
	"crypto/rand"
	"fmt"
	"log"

	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/kafka"
)

func newBench(cfg *config) *bench {
	return &bench{
		cfg: *cfg,
	}
}

type bench struct {
	cfg config

	manager  *kafka.Manager
	Producer *kafka.Producer
	Consumer *kafka.Consumer
}

func (b *bench) Setup(ctx context.Context, kafkaCommonCfg kafka.CommonConfig) {
	log.Println("prep kafka.Manager")
	mngr, err := newMngr(kafkaCommonCfg, b.cfg.brokers)
	if err != nil {
		log.Fatal(fmt.Sprintf("cannot create kafka manager: %s", err))
	}

	b.manager = mngr
	if err := mngr.Healthy(ctx); err != nil {

		log.Fatal(fmt.Sprintf("kafka cluster is not healthy (%s): %s", b.cfg.brokers, err))
	}

	log.Println("cluster confirmed healthy")

	log.Println("creating benchmark kafka topics")
	if err := createTopics(ctx, mngr, b.cfg.partitions, b.cfg.topics); err != nil {
		log.Fatal("cannot create topics: %w", err)
	}

	p, err := createProducer(kafkaCommonCfg)
	if err != nil {
		log.Fatalf("cannot create kafka producer: %s", err)
	}

	b.Producer = p

	c, err := createConsumer(kafkaCommonCfg, b.cfg.topics)
	if err != nil {
		log.Fatal(fmt.Sprintf("cannot create consumer: %s", err))
	}

	b.Consumer = c
}

func (b *bench) Teardown(ctx context.Context) {
	deleteTopics(ctx, b.manager, b.cfg.topics)
	b.manager.Close()
}

func newMngr(commoncfg kafka.CommonConfig, brokers []string) (*kafka.Manager, error) {
	commoncfg.ClientID = fmt.Sprintf("%s-manager", app)

	cfg := kafka.ManagerConfig{
		CommonConfig: commoncfg,
	}

	return kafka.NewManager(cfg)
}

func createTopics(ctx context.Context, mngr *kafka.Manager, partitions int, topics []apmqueue.Topic) error {
	cfg := kafka.TopicCreatorConfig{
		PartitionCount: partitions,
	}

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

func deleteTopics(ctx context.Context, mngr *kafka.Manager, topics []apmqueue.Topic) {
	log.Println("deleting benchmark kafka topics")
	err := mngr.DeleteTopics(ctx, topics...)
	if err != nil {
		panic(err)
	}
}

func createProducer(commoncfg kafka.CommonConfig) (*kafka.Producer, error) {
	commoncfg.ClientID = fmt.Sprintf("%s-producer", app)

	return kafka.NewProducer(kafka.ProducerConfig{
		CommonConfig: commoncfg,
	})
}

func generateEvent(size int) ([]byte, error) {
	buf := make([]byte, size)

	_, err := rand.Read(buf)
	if err != nil {
		return []byte{}, fmt.Errorf("error while generating random string: %w", err)
	}

	return buf, nil
}

func createConsumer(commoncfg kafka.CommonConfig, topics []apmqueue.Topic) (*kafka.Consumer, error) {
	commoncfg.ClientID = fmt.Sprintf("%s-consumer", app)

	return kafka.NewConsumer(kafka.ConsumerConfig{
		CommonConfig: commoncfg,

		Topics:    topics,
		GroupID:   "zero",
		Processor: dummyProcessor{},

		// ShutdownGracePeriod: 5 * time.Second,
	})
}
