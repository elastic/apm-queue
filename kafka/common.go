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
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/plugin/kotel"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// SASLMechanism type alias to sasl.Mechanism
type SASLMechanism = sasl.Mechanism

// CommonConfig defines common configuration for Kafka consumers, producers,
// and managers.
type CommonConfig struct {
	// Brokers is the list of kafka brokers used to seed the Kafka client.
	//
	// If Brokers is unspecified, but $KAFKA_BROKERS is specified, it will
	// be parsed as a comma-separated list of broker addresses and used.
	Brokers []string

	// ClientID to use when connecting to Kafka. This is used for logging
	// and client identification purposes.
	ClientID string

	// Version is the software version to use in the Kafka client. This is
	// useful since it shows up in Kafka metrics and logs.
	Version string

	// SASL configures the kgo.Client to use SASL authorization.
	//
	// If SASL is unspecified, then it may be derived from environment
	// variables as follows:
	//  - if $KAFKA_USERNAME and $KAFKA_PASSWORD are both specified, then
	//    SASL/PLAIN will be configured
	//  - if $AWS_ACCESS_KEY_ID and $AWS_SECRET_ACCESS_KEY are specified,
	//    then SASL/AWS_MSK_IAM wil be configured
	SASL SASLMechanism

	// TLS configures the kgo.Client to use TLS for authentication.
	// This option conflicts with Dialer. Only one can be used.
	//
	// If neither TLS nor Dialer are specified, then TLS will be configured
	// by default unless the environment variable $KAFKA_PLAINTEXT is set to
	// "true". In case TLS is auto-configured, $KAFKA_TLS_INSECURE may be
	// set to "true" to disable server certificate and hostname verification.
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

	// MeterProvider allows specifying a custom otel meter provider.
	// Defaults to the global one.
	MeterProvider metric.MeterProvider
}

// finalize ensures the configuration is valid, setting default values from
// environment variables as described in doc comments, returning an error if
// any configuration is invalid.
func (cfg *CommonConfig) finalize() error {
	var errs []error
	if len(cfg.Brokers) == 0 {
		if v := os.Getenv("KAFKA_BROKERS"); v != "" {
			cfg.Brokers = strings.Split(v, ",")
		} else {
			errs = append(errs, errors.New("kafka: at least one broker must be set"))
		}
	}
	if cfg.Logger == nil {
		errs = append(errs, errors.New("kafka: logger must be set"))
	}
	switch {
	case cfg.TLS != nil && cfg.Dialer != nil:
		errs = append(errs, errors.New("kafka: only one of TLS or Dialer can be set"))
	case cfg.TLS == nil && cfg.Dialer == nil && os.Getenv("KAFKA_PLAINTEXT") != "true":
		// Auto-configure TLS from environment variables.
		cfg.TLS = &tls.Config{}
		if os.Getenv("KAFKA_TLS_INSECURE") == "true" {
			cfg.TLS.InsecureSkipVerify = true
		}
	}
	if cfg.SASL == nil {
		// SASL not specified, default to SASL/PLAIN if username and password
		// environment variables are specified, or SASL/AWS_MSK_IAM if AWS
		// credential environment variables are specified.
		plainAuth := plain.Auth{
			User: os.Getenv("KAFKA_USERNAME"),
			Pass: os.Getenv("KAFKA_PASSWORD"),
		}
		if plainAuth != (plain.Auth{}) {
			cfg.SASL = plainAuth.AsMechanism()
		} else {
			awsAuth := aws.Auth{
				AccessKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
				SecretKey:    os.Getenv("AWS_SECRET_ACCESS_KEY"),
				SessionToken: os.Getenv("AWS_SESSION_TOKEN"),
			}
			if awsAuth.AccessKey != "" && awsAuth.SecretKey != "" {
				cfg.SASL = awsAuth.AsManagedStreamingIAMMechanism()
			}
		}
	}
	return errors.Join(errs...)
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

func (cfg *CommonConfig) newClient(additionalOpts ...kgo.Opt) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.WithLogger(kzap.New(cfg.Logger.Named("kafka"))),
		kgo.SeedBrokers(cfg.Brokers...),
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
	opts = append(opts, additionalOpts...)
	if !cfg.DisableTelemetry {
		kotelService := kotel.NewKotel(
			kotel.WithTracer(kotel.NewTracer(kotel.TracerProvider(cfg.tracerProvider()))),
			kotel.WithMeter(kotel.NewMeter(kotel.MeterProvider(cfg.meterProvider()))),
		)
		opts = append(opts,
			kgo.WithHooks(NewKgoHooks(cfg.meterProvider())),
			kgo.WithHooks(kotelService.Hooks()...),
		)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed creating kafka client: %w", err)
	}
	// Issue a metadata refresh request on construction, so the broker list is populated.
	client.ForceMetadataRefresh()
	return client, nil
}
