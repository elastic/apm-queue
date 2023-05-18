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

package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/plugin/kotel"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// SASLMechanism type alias to sasl.Mechanism
type SASLMechanism = sasl.Mechanism

// CommonConfig defines common configuration for Kafka consumers, producers,
// and managers.
type CommonConfig struct {
	// Brokers is the list of kafka brokers used to seed the Kafka client.
	Brokers []string

	// ClientID to use when connecting to Kafka. This is used for logging
	// and client identification purposes.
	ClientID string

	// Version is the software version to use in the Kafka client. This is
	// useful since it shows up in Kafka metrics and logs.
	Version string

	// SASL configures the kgo.Client to use SASL authorization.
	SASL SASLMechanism

	// TLS configures the kgo.Client to use TLS for authentication.
	// This option conflicts with Dialer. Only one can be used.
	TLS *tls.Config

	// Dialer uses fn to dial addresses, overriding the default dialer that uses a
	// 10s dial timeout and no TLS (unless TLS option is set).
	//
	// The context passed to the dial function is the context used in the request
	// that caused the dial. If the request is a client-internal request, the
	// context is the context on the client itself (which is canceled when the
	// client is closed).
	// This option conflicts with TLS. Only one can be used.
	Dialer func(ctx context.Context, network, address string) (net.Conn, error)

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
	if len(cfg.Brokers) == 0 {
		errs = append(errs, errors.New("kafka: at least one broker must be set"))
	}
	if cfg.Logger == nil {
		errs = append(errs, errors.New("kafka: logger must be set"))
	}
	if cfg.TLS != nil && cfg.Dialer != nil {
		errs = append(errs, errors.New("kafka: only one of TLS or Dialer can be set"))
	}
	return errors.Join(errs...)
}

func (cfg *CommonConfig) tracerProvider() trace.TracerProvider {
	if cfg.TracerProvider != nil {
		return cfg.TracerProvider
	}
	return otel.GetTracerProvider()
}

func (cfg *CommonConfig) newClient(additionalOpts ...kgo.Opt) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.WithLogger(kzap.New(cfg.Logger.Named("kafka"))),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
		if cfg.Version != "" {
			opts = append(opts, kgo.SoftwareNameAndVersion(
				cfg.ClientID, cfg.Version,
			))
		}
	}
	if cfg.Dialer != nil {
		opts = append(opts, kgo.Dialer(cfg.Dialer))
	} else if cfg.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(cfg.TLS.Clone()))
	}
	if cfg.SASL != nil {
		opts = append(opts, kgo.SASL(cfg.SASL))
	}
	if !cfg.DisableTelemetry {
		kotelService := kotel.NewKotel(
			kotel.WithTracer(kotel.NewTracer(kotel.TracerProvider(cfg.tracerProvider()))),
			kotel.WithMeter(kotel.NewMeter()),
		)
		opts = append(opts, kgo.WithHooks(kotelService.Hooks()...))
	}
	opts = append(opts, additionalOpts...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed creating kafka client: %w", err)
	}
	// Issue a metadata refresh request on construction, so the broker list is populated.
	client.ForceMetadataRefresh()
	return client, nil
}
