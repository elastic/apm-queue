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
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/api/option"
)

// CommonConfig defines common configuration for Kafka consumers, producers,
// and managers.
type CommonConfig struct {
	// Project is the GCP project.
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
}

// Validate ensures the configuration is valid, otherwise, returns an error.
func (cfg CommonConfig) Validate() error {
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

func (cfg *CommonConfig) tracerProvider() trace.TracerProvider {
	if cfg.TracerProvider != nil {
		return cfg.TracerProvider
	}
	return otel.GetTracerProvider()
}

// TODO(axw) method for producing common option.ClientOptions, such as otelgrpc interceptors.
