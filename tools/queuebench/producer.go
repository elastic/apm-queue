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

	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/kafka"
)

func produce(ctx context.Context, kafkaCommonCfg kafka.CommonConfig, cfg *config) {
	producer, err := createProducer(kafkaCommonCfg)
	if err != nil {
		log.Fatalf("cannot create kafka producer: %s", err)
	}
	defer producer.Close()

	event, err := generateEvent(cfg.eventSize)
	if err != nil {
		log.Fatal(fmt.Sprintf("cannot generate event: %s", err))
	}

	record := apmqueue.Record{
		Topic: cfg.topics[0],
		// OrderingKey: []byte{},
		Value: event,
	}

	log.Println("producing...")
	for {
		producer.Produce(ctx, record)
	}
}

func createProducer(commoncfg kafka.CommonConfig) (*kafka.Producer, error) {
	commoncfg.ClientID = fmt.Sprintf("%s-producer", app)

	return kafka.NewProducer(kafka.ProducerConfig{
		CommonConfig: commoncfg,
	})
}
