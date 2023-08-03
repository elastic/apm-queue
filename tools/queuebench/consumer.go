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
	"time"

	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/kafka"
)

func consume(ctx context.Context, kafkaCommonCfg kafka.CommonConfig, cfg *config, timeout time.Duration) {
	consumer, err := createConsumer(kafkaCommonCfg, cfg.topics)
	if err != nil {
		log.Fatal(fmt.Sprintf("cannot create consumer: %s", err))
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	log.Println("consuming...")
	if err := consumer.Run(ctx); err != nil {
		panic(err)
	}
}

type dummyProcessor struct{}

// Process implements apmqueue.Processor
func (p dummyProcessor) Process(ctx context.Context, records ...apmqueue.Record) error {
	return nil
}

func createConsumer(commoncfg kafka.CommonConfig, topics []apmqueue.Topic) (*kafka.Consumer, error) {
	commoncfg.ClientID = fmt.Sprintf("%s-consumer", app)

	return kafka.NewConsumer(kafka.ConsumerConfig{
		CommonConfig: commoncfg,

		Topics:    topics,
		GroupID:   "zero",
		Processor: dummyProcessor{},
	})
}
