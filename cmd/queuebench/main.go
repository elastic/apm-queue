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

var totalproduced = 0
var totalconsumed = 0

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
	var logger *zap.Logger
	var err error
	if cfg.verbose {
		logger, err = zap.NewDevelopment()
		if err != nil {
			log.Fatalf("cannot create zap logger: %s", err)
		}
	} else {
		logger = zap.NewNop()
	}

	log.Println("prep MeterProvider")
	rdr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(rdr),
	)

	ctx := context.Background()
	run := time.Now().Unix()
	log.Printf("running bench run: %d", run)

	bench := bench{
		Brokers:         []string{cfg.broker},
		ConsumerGroupID: fmt.Sprintf("queuebench-%d", run),
		Logger:          logger,
		Partitions:      cfg.partitions,
		TopicNamespace:  namespace,
		Topics: []apmqueue.Topic{
			apmqueue.Topic(fmt.Sprintf("run-%d", run)),
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

	log.Printf("start producing, will produce for %s", cfg.duration)
	if err := produce(ctx, bench.p, bench.Topics[0], cfg.eventSize, cfg.duration); err != nil {
		log.Panicf("error while producing records: %s", err)
	}

	log.Println("stop producing")

	for {
		// TODO: add timeout
		if totalconsumed < totalproduced {
			time.Sleep(100 * time.Millisecond)
		} else {
			log.Println("stop consuming")
			if err := bench.c.Close(); err != nil {
				log.Panicf("error closing consumer: %s", err)
			}
			break
		}
	}
	log.Println("==> benchmark ")

	duration := time.Since(start)
	log.Printf("it took %s", duration)
	log.Printf("total\n\tproduced: %d\n\tconsumed: %d", totalproduced, totalconsumed)

	log.Println("collecting metrics")
	var rm metricdata.ResourceMetrics
	rdr.Collect(context.Background(), &rm)
	if err = display(rm); err != nil {
		log.Panicf("failed displaying metrics: %s", err)
	}

	log.Println("bench run completed successfully")
}

func produce(ctx context.Context, p *kafka.Producer, topic apmqueue.Topic, size int, duration time.Duration) error {
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

	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if err = p.Produce(ctx, record); err != nil {
			return err
		}
		totalproduced += 1
	}

	return err
}
