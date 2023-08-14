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
	"os"
	"os/signal"
	"syscall"
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
	// NOTE: intercept any panic and terminate gracefully
	// This allows using log.Panic in main which triggers
	// deferred functions (whereas log.Fatal don't).
	defer func() {
		if r := recover(); r != nil {
			log.Fatal(r)
		}
	}()

	cfg := config{}
	cfg.Parse()
	log.Printf("parsed config: %+v\n", cfg)

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
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

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
	teardown := func() {
		log.Println("running benchmark teardown")
		// NOTE: using a different context to prevent this function
		// being affected by main context being closed
		if err := bench.Teardown(context.Background()); err != nil {
			log.Panicf("benchmark teardown failed: %s", err)
		}

	}
	defer teardown()

	start := time.Now()
	log.Println("==> running benchmark")

	log.Println("start consuming")
	var consumptionduration time.Duration
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Panicf("consumer loop panicked: %s", r)
			}
		}()

		consumptionstart := time.Now()
		if err := bench.c.Run(ctx); err != nil {
			log.Printf("consumer run ended with an error: %s", err)
		}
		consumptionduration = time.Since(consumptionstart)
	}()

	log.Printf("start producing, will produce for %s", cfg.duration)
	productionstart := time.Now()
	if err := produce(ctx, bench.p, bench.Topics[0], cfg.eventSize, cfg.duration); err != nil {
		log.Printf("error while producing records: %s", err)
	}
	productionduration := time.Since(productionstart)

	log.Println("production ended")

	log.Println("waiting for consumer to fetch all records")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(cfg.timeout)
	defer timer.Stop()

	var rm metricdata.ResourceMetrics
	totalproduced := int64(0)
	totalconsumed := int64(0)
wait:
	for {
		select {
		case <-ctx.Done():
			log.Panic("context closed, terminating execution")
		case <-timer.C:
			log.Println("reached timeout, moving on")
			break wait
		case <-ticker.C:
			if err := rdr.Collect(ctx, &rm); err != nil {
				// NOTE: consider any error here as transient and don't trigger termination
				log.Printf("cannot collect otel metrics: %s", err)
				continue
			}

			totalproduced = getSumInt64Metric("github.com/elastic/apm-queue/kafka", "producer.messages.produced", rm)
			totalconsumed = getSumInt64Metric("github.com/elastic/apm-queue/kafka", "consumer.messages.fetched", rm)
			if totalconsumed >= totalproduced {
				log.Println("consumption ended")
				break wait
			}
		}
	}
	if err := bench.c.Close(); err != nil {
		log.Panicf("error closing consumer: %s", err)
	}

	log.Println("==> benchmark ")
	if err := bench.p.Close(); err != nil {
		log.Panicf("error closing producer: %s", err)
	}

	duration := time.Since(start)
	log.Printf("it took %s (-duration=%s)", duration, cfg.duration)
	log.Printf("time spent producing: %s", productionduration)
	log.Printf("time spent consuming: %s", consumptionduration)
	log.Printf("total produced/consumed: %d/%d", totalproduced, totalconsumed)

	log.Println("collecting metrics")
	rdr.Collect(context.Background(), &rm)
	if err = display(rm); err != nil {
		log.Panicf("failed displaying metrics: %s", err)
	}

	if totalproduced != totalconsumed {
		log.Panicf("total produced and consumed don't match: %d vs %d", totalproduced, totalconsumed)
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
		Value: buf,
	}

	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err = p.Produce(ctx, record); err != nil {
			return err
		}
	}

	return err
}
