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
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/api/option"

	apmqueue "github.com/elastic/apm-queue"
)

// CommonConfig defines common configuration for Pub/Sub Lite consumers,
// producers, and managers.
type CommonConfig struct {
	// Project is the GCP project.
	//
	// NewManager, NewProducer, and NewConsumer will set Project from
	// $GOOGLE_APPLICATION_CREDENTIALS it is not explicitly specified.
	Project string

	// Region is the GCP region.
	Region string

	// Namespace holds a namespace for Pub/Sub Lite resources.
	//
	// This is added as a prefix for reservation, topic, and
	// subscription names, and acts as a filter on resources
	// monitored or described by the manager.
	//
	// Namespace is always removed from resource names before
	// they are returned to callers. The only way Namespace
	// will surface is in telemetry (e.g. metrics), as an
	// independent dimension. This enables users to filter
	// metrics by namespace, while maintaining stable names
	// for resources (e.g. topics).
	Namespace string

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
}

// finalize ensures the configuration is valid, setting default values from
// environment variables as described in doc comments, returning an error if
// any configuration is invalid.
//
// If $GOOGLE_APPLICATION_CREDENTIALS is set, the file to which it points will
// be read and parsed as JSON (assuming service account), and its `project_id`
// property will be used for the Project property if it is not already set.
func (cfg *CommonConfig) finalize() error {
	var errs []error
	if cfg.Logger == nil {
		errs = append(errs, errors.New("logger must be set"))
		cfg.Logger = zap.NewNop()
	} else {
		cfg.Logger = cfg.Logger.Named("pubsublite")
	}
	if cfg.Region == "" {
		errs = append(errs, errors.New("region must be set"))
	}
	if cfg.Project == "" {
		if err := cfg.setProject(); err != nil {
			errs = append(errs, fmt.Errorf("error setting project: %w", err))
		} else if cfg.Project == "" {
			errs = append(errs, errors.New("project must be set"))
		}
	}
	if len(errs) == 0 {
		cfg.Logger = cfg.Logger.With(
			zap.String("region", cfg.Region),
			zap.String("project", cfg.Project),
		)
		if cfg.Namespace != "" {
			cfg.Logger = cfg.Logger.With(
				zap.String("namespace", cfg.Namespace),
			)
		}
	}
	return errors.Join(errs...)
}

func (cfg *CommonConfig) setProject() error {
	const credentialsEnvVar = "GOOGLE_APPLICATION_CREDENTIALS"
	credentialsPath := os.Getenv(credentialsEnvVar)
	if credentialsPath == "" {
		cfg.Logger.Debug("$" + credentialsEnvVar + " not set, cannot set default project ID")
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
	if cfg.Project == "" {
		cfg.Logger.Debug("project ID missing from $" + credentialsEnvVar + "")
	} else {
		cfg.Logger.Info(
			"set project ID from $"+credentialsEnvVar,
			zap.String("project", cfg.Project),
		)
	}
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

func (cfg *CommonConfig) namespacePrefix() string {
	if cfg.Namespace == "" {
		return ""
	}
	return cfg.Namespace + "-"
}

// TODO(axw) method for producing common option.ClientOptions, such as otelgrpc interceptors.

// JoinTopicConsumer returns a Pub/Sub Lite subscription name
// for the given topic and consumer name.
func JoinTopicConsumer(topic apmqueue.Topic, consumer string) string {
	return fmt.Sprintf("%s+%s", topic, consumer)
}

// SplitTopicConsumer does the opposite of JoinTopicConsumer
// by parsing topic and consumer out of a subscription name.
// Returns an error if subscription name is not in an expected format.
func SplitTopicConsumer(subscriptionName string) (topic apmqueue.Topic, consumer string, err error) {
	parts := strings.Split(subscriptionName, "+")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("malformed subscription name")
	}
	topic = apmqueue.Topic(parts[0])
	consumer = parts[1]
	return
}
