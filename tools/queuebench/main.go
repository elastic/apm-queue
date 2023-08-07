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
	"strings"
	"time"

	flag "github.com/spf13/pflag"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/kafka"
)

const app = "queuebench"

type config struct {
	brokers    []string
	duration   time.Duration
	eventSize  int
	partitions int
	topics     []apmqueue.Topic
	verbose    bool
}

func flags2config() *config {
	brokers := flag.String("brokers", "", "broker/s bootstrap URL")
	duration := flag.Int("duration", 0, "duration time in seconds for the benchmark. Required.")
	eventSize := flag.Int("event-size", 1024, "size of each event, in bytes. Default to 1024B (1KB).")
	partitions := flag.Int("partitions", 1, "number of partitions to use for benchmarking a single topic. Default: 1.")
	verbose := flag.Bool("verbose", false, "print MOAR")

	flag.Parse()

	if *brokers == "" {
		log.Fatal("config validation: brokers must not be empty")
	}
	if *duration == 0 {
		log.Fatal("config validation: duration must be set and be greater than 0")
	}

	return &config{
		brokers:    []string{*brokers},
		duration:   time.Duration(*duration) * time.Second,
		eventSize:  *eventSize,
		partitions: *partitions,
		topics: []apmqueue.Topic{
			apmqueue.Topic(fmt.Sprintf("%s-%d", "run", time.Now().Unix())),
		},
		verbose: *verbose,
	}
}

func main() {
	cfg := flags2config()

	log.Println("prep logger")
	zapLevel := zap.ErrorLevel
	if cfg.verbose {
		zapLevel = zap.DebugLevel
	}
	logger, err := zap.NewDevelopment(zap.IncreaseLevel(zapLevel))
	if err != nil {
		log.Fatal(fmt.Sprintf("cannot create zap logger: %s", err))
	}

	log.Println("prep MeterProvider")
	rdr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(rdr),
	)

	kafkaCommonCfg := kafka.CommonConfig{
		Brokers:   cfg.brokers,
		Namespace: app,

		Logger:         logger,
		TracerProvider: trace.NewNoopTracerProvider(),
		MeterProvider:  mp,
	}

	ctx := context.Background()
	bench := newBench(cfg)
	bench.Setup(ctx, kafkaCommonCfg)
	defer bench.Teardown(ctx)

	log.Println("staring producer")
	stopProducer := make(chan struct{})
	go produce(ctx, bench.Producer, cfg, stopProducer)

	log.Println("starting consumer")
	go consume(ctx, bench.Consumer, cfg, cfg.duration*2)

	var quit = make(chan struct{})
	go func() {
		time.Sleep(cfg.duration)

		log.Println("signal producer to stop")
		stopProducer <- struct{}{}
		err := bench.Producer.Close()
		log.Println("closed producer")
		if err != nil {
			panic(err)
		}

		log.Println("gracefully stopping consumer")
		err = bench.Consumer.Close()
		if err != nil {
			panic(err)
		}

		log.Println("send bench complete mark")
		quit <- struct{}{}
	}()

	log.Println("waiting bench to complete...")
	<-quit
	log.Println("time's up!")

	log.Println("collecting metrics")
	var rm metricdata.ResourceMetrics
	rdr.Collect(context.Background(), &rm)

	log.Println("bench run complete")
	if cfg.verbose {
		rawPrintMetrics(rm)
	}
	display(rm)
}

func rawPrintMetrics(rm metricdata.ResourceMetrics) {
	getAttrs := func(a attribute.Set) string {
		s := strings.Builder{}
		for _, v := range a.ToSlice() {
			s.WriteString(fmt.Sprintf("(%s:%s)", v.Key, v.Value.AsString()))
		}
		return s.String()
	}

	franzMetrics := filterMetrics("github.com/twmb/franz-go/plugin/kotel", rm.ScopeMetrics)
	for _, m := range franzMetrics {
		fmt.Println(m.Name)

		for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
			fmt.Printf("%s %d (%s)\n", m.Name, dp.Value, getAttrs(dp.Attributes))
		}
	}

	kafkaMetrics := filterMetrics("github.com/elastic/apm-queue/kafka", rm.ScopeMetrics)
	for _, m := range kafkaMetrics {
		fmt.Println(m.Name)

		if md, ok := m.Data.(metricdata.Sum[int64]); ok {
			for _, dp := range md.DataPoints {
				fmt.Printf("%s %d (%s)\n", m.Name, dp.Value, getAttrs(dp.Attributes))
			}
		}
	}
}
