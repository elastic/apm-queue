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
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
)

func main() {
	lg, err := newLoadgen(
		parseHost,
		parseReplayCount,
		parsePrefix,
		parseConcurrentProducers,
		parseProducerKey,
		parseProducerFormat,
		parseOutputType,
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := lg.run(ctx); err != nil {
		log.Fatal(err)
	}
}

func parseHost(lg *loadgen) error {
	if outputAddr, ok := os.LookupEnv("OUTPUT_ADDRESS"); ok {
		lg.outputAddress = outputAddr
		return nil
	}
	return errors.New("missing output address")
}

func parseReplayCount(lg *loadgen) error {
	if replay, ok := os.LookupEnv("REPLAY"); ok {
		replayCount, err := strconv.Atoi(replay)
		if err != nil {
			return fmt.Errorf("failed to parse replay count: %s: %w", replay, err)
		}
		lg.replay = replayCount
	}
	return nil
}

func parseConcurrentProducers(lg *loadgen) error {
	if producers, ok := os.LookupEnv("PRODUCER_COUNT"); ok {
		producerCount, err := strconv.Atoi(producers)
		if err != nil {
			return fmt.Errorf("failed to parse producer count: %s: %w", producers, err)
		}
		lg.producerCount = producerCount
	}
	return nil
}

func parseProducerKey(lg *loadgen) error {
	if key, ok := os.LookupEnv("PRODUCER_KEY"); ok {
		lg.producerKey = key
	}
	return nil
}

func parseProducerFormat(lg *loadgen) error {
	if f, ok := os.LookupEnv("PRODUCER_VALUE_FORMAT"); ok {
		lg.producerFmt = f
	}
	return nil
}

func parsePrefix(lg *loadgen) error {
	if prefix, ok := os.LookupEnv("TOPIC_PREFIX"); ok {
		lg.prefix = prefix
	}
	return nil
}

func parseOutputType(lg *loadgen) error {
	if outputType, ok := os.LookupEnv("OUTPUT_TYPE"); ok {
		switch outputType {
		case "kafka", "pubsublite":
			lg.outputType = outputType
		default:
			return fmt.Errorf("unknown output type: %s", outputType)
		}
	}
	return nil
}
