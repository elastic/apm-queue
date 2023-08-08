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
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/kafka"
)

const namespace = "queuebench"

func main() {
	// NOTE: intercept any panic and print a nice terminate gracefully
	// This allows using log.Panic methods in main; those function
	// trigger deferred functions whereas log.Fatal don't.
	defer func() {
		if r := recover(); r != nil {
			log.Fatal(r)
		}
	}()

	cfg := config{}

	cfg.Parse()

	fmt.Printf("%+v\n", cfg)

	log.Println("prep logger")
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("cannot create zap logger: %s", err)
	}

	log.Println("prep MeterProvider")
	rdr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(rdr),
	)

	ctx := context.Background()

	bench := bench{
		Brokers:        []string{cfg.broker},
		Logger:         logger,
		Partitions:     cfg.partitions,
		TopicNamespace: namespace,
		Topics: []apmqueue.Topic{
			apmqueue.Topic(fmt.Sprintf("run-%d", time.Now().Unix())),
		},

		mp: mp,
		tp: trace.NewNoopTracerProvider(),
	}

	log.Println("running benchmark setup")
	if err = bench.Setup(ctx); err != nil {
		log.Panicf("benchmark setup failed: %s", err)
	}
	defer func() {
		log.Println("running benchmark teardown")
		if err := bench.Teardown(ctx); err != nil {
			log.Panicf("benchmark teardown failed: %s", err)
		}
	}()

	start := time.Now()
	log.Println("==> running benchmark")

	log.Println("start consumer")
	go func() {
		if err := bench.c.Run(ctx); err != nil {
			log.Panicf("consumer run ended with an error: %s", err)
		}
	}()

	log.Println("start producing")
	produce(ctx, bench.p, bench.Topics[0], cfg.eventSize)

	log.Println("stop producing")

	log.Println("==> benchmark ")

	duration := time.Since(start)
	log.Printf("it took %s", duration)

	log.Println("collecting metrics")
	var rm metricdata.ResourceMetrics
	rdr.Collect(context.Background(), &rm)
	if err = display(rm); err != nil {
		log.Panicf("failed displaying metrics: %s", err)
	}

	log.Println("bench run completed successfully")
}

func produce(ctx context.Context, p *kafka.Producer, topic apmqueue.Topic, size int) error {
	buf := make([]byte, size)

	_, err := rand.Read(buf)
	if err != nil {
		return fmt.Errorf("cannot read random bytes: %w", err)
	}

	record := apmqueue.Record{
		Topic: topic,
		// OrderingKey: []byte{},
		Value: buf,
	}

	for i := 0; i < 10; i++ {
		if err = p.Produce(ctx, record); err != nil {
			return err
		}
	}

	return err
}
