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

package pubsublite

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/api/option"

	apmqueue "github.com/elastic/apm-queue"
)

// CommonConfig defines common configuration for Kafka consumers, producers,
// and managers.
type CommonConfig struct {
	// Project is the GCP project.
	//
	// NewManager, NewProducer, and NewConsumer will set Project from
	// $GOOGLE_APPLICATION_CREDENTIALS it is not explicitly specified.
	Project string

	// Region is the GCP region.
	Region string

	// ClientOptions holds arbitrary Google API client options.
	ClientOptions []option.ClientOption

	// Logger to use for any errors.
	Logger *zap.Logger

	// DisableTelemetry disables the OpenTelemetry hook.
	DisableTelemetry bool

	// TracerProvider allows specifying a custom otel tracer provider.
	// Defaults to the global one.
	TracerProvider trace.TracerProvider

	// MeterProvider allows specifying a custom otel meter provider.
	// Defaults to the global one.
	MeterProvider metric.MeterProvider

	// MonitoringClientOptions holds arbitrary Google monitoring API client options.
	MonitoringClientOptions []option.ClientOption
}

// Validate ensures the configuration is valid, otherwise, returns an error.
func (cfg *CommonConfig) Validate() error {
	var errs []error
	if cfg.Project == "" {
		errs = append(errs, errors.New("pubsublite: project must be set"))
	}
	if cfg.Region == "" {
		errs = append(errs, errors.New("pubsublite: region must be set"))
	}
	if cfg.Logger == nil {
		errs = append(errs, errors.New("pubsublite: logger must be set"))
	}
	return errors.Join(errs...)
}

// setFromEnv sets unspecified config from environment variables.
//
// If $GOOGLE_APPLICATION_CREDENTIALS is set, the file to which it points will
// be read and parsed as JSON (assuming service account), and its `project_id`
// property will be used for the Project property if it is not already set.
func (cfg *CommonConfig) setFromEnv() error {
	if cfg.Project != "" {
		return nil
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	const credentialsEnvVar = "GOOGLE_APPLICATION_CREDENTIALS"
	credentialsPath := os.Getenv(credentialsEnvVar)
	if credentialsPath == "" {
		logger.Debug("$" + credentialsEnvVar + " not set, cannot set default project ID")
		return nil
	}

	credentialsFile, err := os.Open(credentialsPath)
	if err != nil {
		return fmt.Errorf("failed to read $%s: %w", credentialsEnvVar, err)
	}
	defer credentialsFile.Close()

	var config struct {
		ProjectID string `json:"project_id"`
	}
	if err := json.NewDecoder(credentialsFile).Decode(&config); err != nil {
		return fmt.Errorf("failed to parse $%s: %w", credentialsEnvVar, err)
	}
	cfg.Project = config.ProjectID
	logger.Info("set project ID from $"+credentialsEnvVar, zap.String("project", cfg.Project))
	return nil
}

func (cfg *CommonConfig) tracerProvider() trace.TracerProvider {
	if cfg.TracerProvider != nil {
		return cfg.TracerProvider
	}
	return otel.GetTracerProvider()
}

func (cfg *CommonConfig) meterProvider() metric.MeterProvider {
	if cfg.MeterProvider != nil {
		return cfg.MeterProvider
	}
	return otel.GetMeterProvider()
}

// TODO(axw) method for producing common option.ClientOptions, such as otelgrpc interceptors.

// SubscriptionName returns a Pub/Sub Lite subscription name
// for the given topic and consumer name.
func SubscriptionName(topic apmqueue.Topic, consumer string) string {
	return fmt.Sprintf("%s+%s", topic, consumer)
}
