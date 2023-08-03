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

	"github.com/gosuri/uitable"
	flag "github.com/spf13/pflag"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	apmqueue "github.com/elastic/apm-queue"
	"github.com/elastic/apm-queue/kafka"
)

type config struct {
	brokers    []string
	duration   time.Duration
	eventSize  int
	partitions int
}

func flags2config() *config {
	brokers := flag.String("brokers", "", "broker/s bootstrap URL")
	duration := flag.Int("duration", 0, "duration time in seconds for the benchmark. Required.")
	eventSize := flag.Int("event-size", 1024, "size of each event, in bytes. Default to 1024B (1KB).")
	partitions := flag.Int("partitions", 1, "number of partitions to use for benchmarking a single topic. Default: 1.")

	flag.Parse()

	if *brokers == "" {
		log.Fatal("config validation: brokers must not be empty")
	}
	if *partitions != 1 {
		log.Fatal("config validation: partitions > 1 is not supported (yet)")
	}
	if *eventSize != 1024 {
		log.Fatal("config validation: event-size > 1024 is not supported (yet)")
	}
	if *duration == 0 {
		log.Fatal("config validation: duration must be set and be greater than 0")
	}

	return &config{
		brokers:    []string{*brokers},
		duration:   time.Duration(*duration),
		eventSize:  *eventSize,
		partitions: *partitions,
	}
}

func main() {
	cfg := flags2config()

	log.Println("prep logger")
	logger, err := zap.NewDevelopment()
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

	log.Println("prep kafka.Manager")
	mngr, err := newMngr(kafkaCommonCfg, cfg.brokers)
	if err != nil {
		log.Fatal(fmt.Sprintf("cannot create kafka manager: %s", err))
	}
	defer func() { mngr.Close() }()

	ctx := context.Background()

	if err := mngr.Healthy(ctx); err != nil {
		log.Fatal(fmt.Sprintf("kafka cluster is not healthy (%s): %s", cfg.brokers, err))
	}

	log.Println("cluster confirmed healthy")

	ts := time.Now().Unix()
	topics := []apmqueue.Topic{
		apmqueue.Topic(fmt.Sprintf("%s-%d", app, ts)),
	}

	log.Println("creating benchmark kafka topics")
	if err := createTopics(ctx, mngr, cfg.partitions, topics); err != nil {
		log.Fatal("cannot create topics: %w", err)
	}
	defer deleteTopics(ctx, mngr, topics)

	// generator := newEventGenerator(cfg.duration, cfg.eventSize)
	event, err := generateEvent(cfg.eventSize)
	if err != nil {
		log.Fatal(fmt.Sprintf("cannot generate event: %s", err))
	}

	producer, err := createProducer(kafkaCommonCfg)
	if err != nil {
		log.Fatalf("cannot create kafka producer: %s", err)
	}
	defer producer.Close()

	record := apmqueue.Record{
		Topic: topics[0],
		// OrderingKey: []byte{},
		Value: event,
	}
	producer.Produce(ctx, record)

	consumer, err := createConsumer(kafkaCommonCfg, topics)
	if err != nil {
		log.Fatal(fmt.Sprintf("cannot create consumer: %s", err))
	}
	defer consumer.Close()

	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	consumer.Run(c)

	var rm metricdata.ResourceMetrics
	rdr.Collect(context.Background(), &rm)

	// fmt.Printf("%+v\n", rm)

	franzMetrics := filterMetrics("github.com/twmb/franz-go/plugin/kotel", rm.ScopeMetrics)
	if len(franzMetrics) == 0 {
		panic("there should be something")
	}

	kafkaMetrics := filterMetrics("github.com/elastic/apm-queue/kafka", rm.ScopeMetrics)
	if len(kafkaMetrics) == 0 {
		panic("there should be something")
	}

	table := uitable.New()
	table.Wrap = true
	table.MaxColWidth = 80

	table.AddRow("SECTION", "VALUE", "ATTRS")

	tablelize := func(metrics []metricdata.Metrics, n string) {
		m, found := getMetric(metrics, n)
		if found {
			var v any

			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				v = displayDatapoints(d)
			case metricdata.Histogram[float64]:
				v = displayHistogram(d)
			}

			table.AddRow(n, v)
		} else {
			table.AddRow(n, 0)
		}
	}

	tablelize(franzMetrics, "messaging.kafka.produce_records.count")
	tablelize(franzMetrics, "messaging.kafka.fetch_records.count")
	tablelize(franzMetrics, "messaging.kafka.produce_bytes.count")
	tablelize(franzMetrics, "messaging.kafka.fetch_bytes.count")
	tablelize(kafkaMetrics, "producer.messages.produced")
	tablelize(kafkaMetrics, "consumer.messages.fetched")
	tablelize(kafkaMetrics, "consumer.messages.delay")

	fmt.Println()
	fmt.Println(table)
	fmt.Println()

	// go startProducing(producer)
	// go startConsuming(consumer)
}

// func startProducing(producer) {
// 	producer.Start()
// }

// func startConsuming(consumer) {
// 	consumer.Start()
// }

const app = "queuebench"

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

type dummyProcessor struct{}

// Process implements apmqueue.Processor
func (p dummyProcessor) Process(ctx context.Context, records ...apmqueue.Record) error {
	fmt.Println(records)
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
