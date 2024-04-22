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
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	apmqueue "github.com/elastic/apm-queue/v2"
	"github.com/elastic/apm-queue/v2/kafka"
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

	var cfg config
	cfg.Parse()
	log.Printf("parsed config: %+v\n", cfg)

	log.Println("prep logger")
	var err error
	logger := logging(cfg.verbose)

	log.Println("prep MeterProvider")
	mp, rdr := metering()

	if err := host.Start(host.WithMeterProvider(mp)); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()

	run := time.Now().Unix()
	log.Printf("running bench run: %d", run)

	topics := make([]apmqueue.Topic, 0, cfg.topics)
	for i := 0; i < cfg.topics; i++ {
		topics = append(topics, apmqueue.Topic(fmt.Sprintf("run-%d", i)))
	}
	bench := bench{
		Brokers:         []string{cfg.broker},
		ConsumerGroupID: fmt.Sprintf("queuebench-%d", run),
		Logger:          logger,
		Partitions:      cfg.partitions,
		TopicNamespace:  namespace,
		Topics:          topics,
		MetaConsumer:    cfg.metaConsumer,
		MaxPollRecords:  cfg.maxPollRecords,

		mp: mp,
		tp: noop.NewTracerProvider(),
	}

	log.Println("running benchmark setup")
	if err = bench.Setup(ctx); err != nil {
		log.Panicf("benchmark setup failed: %s", err)
	}
	defer func() {
		log.Println("running benchmark teardown")
		// NOTE: using a different context to prevent this function
		// being affected by main context being closed
		// tear := func() error { return bench.Teardown(context.Background()) }
		// for err := tear(); err != nil; err = tear() {
		// 	log.Printf("teardown failed, retrying...")
		// 	time.Sleep(time.Second)
		// }
		// if err != nil {
		// 	log.Printf("teardown failed: %s", err)
		// }
	}()

	start := time.Now()
	log.Println("==> running benchmark")

	var consumptionduration time.Duration
	if !cfg.produceOnly {
		log.Println("start consuming")

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
	}
	var res metrics

	log.Printf("start producing, will produce for %s", cfg.duration)
	productionstart := time.Now()
	var rm metricdata.ResourceMetrics
	var totalDuration time.Duration
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer func() {
			t.Stop()
			defer close(done)
			mem := float64(available / 1024 / 1024 / 1024)
			avg := usage / float64(times)
			log.Printf("| %d topics | %.2f%% (%.2fGB) | %.2f%% (%.2fGB) | %.2fMB/s | %.2fMB/s | No | No | %.0fm (%0.fm consuming) | %d |\n",
				cfg.topics, max*100, max*mem, avg*100, mem*avg,
				res.writeT, res.readT,
				cfg.duration.Minutes(), totalDuration.Minutes(),
				cfg.maxPollRecords,
			)
			fmt.Printf("Average memory utilization: %0.2f%% (%0.2fGB)\n",
				avg*100, mem*avg,
			)
			fmt.Printf("Maximum memory utilization: %0.2f%% (%0.2fGB)\n",
				max*100, max*mem,
			)
			log.Printf("Available memory: %0.2f GB\n", mem)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				rdr.Collect(ctx, &rm)
				displayMemUsage(rm)
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(len(bench.Topics))
	// p, _ := createProducer(kafka.CommonConfig{
	// 	Brokers:   bench.Brokers,
	// 	Namespace: bench.TopicNamespace,

	// 	Logger:         bench.Logger,
	// 	TracerProvider: bench.tp,
	// 	MeterProvider:  bench.mp,
	// })
	for _, t := range bench.Topics {
		go func(t apmqueue.Topic) {
			defer wg.Done()
			// if i%2 == 0 {
			// 	if e := produce(ctx, p, t, cfg.eventSize, cfg.duration); err != nil {
			// 		log.Printf("error while producing records: %s", e)
			// 	}
			// 	return
			// }
			if e := produce(ctx, bench.p, t, cfg.eventSize, cfg.duration); err != nil {
				log.Printf("error while producing records: %s", e)
			}
		}(t)
	}

	wg.Wait()
	productionduration := time.Since(productionstart)
	log.Println("production ended")

	var totalproduced, totalconsumed int64
	if !cfg.produceOnly {
		log.Println("waiting for consumer to fetch all records")
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		log.Printf("timeout set to: %s", time.Now().Add(cfg.timeout))
		timer := time.NewTimer(cfg.timeout)
		defer timer.Stop()
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

				totalproduced = getSumInt64Metric("github.com/elastic/apm-queue/kafka", "producer.messages.count", rm)
				totalconsumed = getSumInt64Metric("github.com/elastic/apm-queue/kafka", "consumer.messages.fetched", rm)
				if totalconsumed >= totalproduced {
					log.Println("consumption ended")
					break wait
				}
			}
		}
		if err := bench.c.Close(); err != nil {
			log.Printf("error closing consumer: %s", err)
		}
	}

	log.Println("==> benchmark ")
	if err := bench.p.Close(); err != nil {
		log.Printf("error closing producer: %s", err)
	}

	totalDuration = time.Since(start)
	log.Printf("it took %s (-duration=%s)", totalDuration, cfg.duration)
	log.Printf("time spent producing: %s", productionduration)
	log.Printf("time spent consuming: %s", consumptionduration)
	log.Printf("total produced/consumed: %d/%d", totalproduced, totalconsumed)

	log.Println("collecting metrics")
	if len(rm.ScopeMetrics) == 0 {
		rdr.Collect(context.Background(), &rm)
	}
	if res, err = display(rm, totalDuration, productionduration); err != nil {
		log.Panicf("failed displaying metrics: %s", err)
	}
	cancel()

	if totalproduced != totalconsumed {
		log.Printf("total produced and consumed don't match: %d vs %d (%0.2f%%)",
			totalproduced, totalconsumed,
			float64(totalconsumed)/float64(totalproduced)*100,
		)
	}
	<-done
	log.Println("bench run completed successfully")
}

func produce(ctx context.Context, p *kafka.Producer, topic apmqueue.Topic, size int, duration time.Duration) error {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Errorf("cannot read random bytes: %w", err)
	}

	record := apmqueue.Record{
		Topic: topic,
		Value: buf,
	}
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if err := p.Produce(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func logging(verbose bool) *zap.Logger {
	if verbose {
		logger, err := zap.NewDevelopment()
		if err != nil {
			log.Fatalf("cannot create zap logger: %s", err)
		}
		return logger
	}
	return zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig()),
		os.Stdout, zap.WarnLevel,
	))
	// return zap.NewNop()
}
