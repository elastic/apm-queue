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
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SASLMechanism type alias to sasl.Mechanism
type SASLMechanism = sasl.Mechanism

type TopicLogFieldFunc func(topic string) zap.Field

// CommonConfig defines common configuration for Kafka consumers, producers,
// and managers.
type CommonConfig struct {
	// ConfigFile holds the path to an optional YAML configuration file,
	// which configures Brokers and SASL.
	//
	// If ConfigFile is unspecified, but $KAFKA_CONFIG_FILE is specified,
	// it will be used to populate ConfigFile. Either way if a file is
	// specified, it must exist when a client is initially created.
	//
	// The following properties from
	// https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
	// are honoured:
	//
	//   - bootstrap.servers ($KAFKA_BROKERS)
	//   - sasl.mechanism ($KAFKA_SASL_MECHANISM)
	//   - sasl.username ($KAFKA_USERNAME)
	//   - sasl.password ($KAFKA_PASSWORD)
	//
	// If bootstrap.servers is defined, then it takes precedence over
	// CommonCnfig.Brokers. When a connection to a broker fails, the
	// config file will be reloaded, and the seed brokers will be
	// updated if bootstrap.servers has changed.
	//
	// If sasl.mechanism is set to PLAIN, or if sasl.username is defined,
	// then SASL/PLAIN will be configured. Whenever a new connection is
	// created, the config will be reloaded in case the username or
	// password has been updated. If sasl.mechanism is set to AWS_MSK_IAM,
	// then SASL/AWS_MSK_IAM is configured using the AWS SDK. Dynamic
	// changes to the sasl.mechanism value are not supported.
	ConfigFile string

	// Namespace holds a namespace for Kafka topics.
	//
	// This is added as a prefix for topics names, and acts as a filter
	// on topics monitored or described by the manager.
	//
	// Namespace is always removed from topic names before they are
	// returned to callers. The only way Namespace will surface is in
	// telemetry (e.g. metrics), as an independent dimension. This
	// enables users to filter metrics by namespace, while maintaining
	// stable topic names.
	Namespace string

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
	//
	//  - if $KAFKA_SASL_MECHANISM is set to PLAIN, or if $KAFKA_USERNAME
	//    and $KAFKA_PASSWORD are both specified, then SASL/PLAIN will be
	//    configured
	//  - if $KAFKA_SASL_MECHANISM is set to AWS_MSK_IAM, then
	//    SASL/AWS_MSK_IAM will be configured using the AWS SDK
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

	// TopicAttributeFunc can be used to create custom dimensions from a Kafka
	// topic for these metrics:
	// - producer.messages.count
	// - consumer.messages.fetched
	TopicAttributeFunc TopicAttributeFunc

	// TopicAttributeFunc can be used to create custom dimensions from a Kafka
	// topic for log messages
	TopicLogFieldFunc TopicLogFieldFunc

	// MetadataMaxAge is the maximum age of metadata before it is refreshed.
	// The lower the value the more frequently new topics will be discovered.
	// If zero, the default value of 5 minutes is used.
	MetadataMaxAge time.Duration

	hooks []kgo.Hook
}

// finalize ensures the configuration is valid, setting default values from
// environment variables as described in doc comments, returning an error if
// any configuration is invalid.
func (cfg *CommonConfig) finalize() error {
	var errs []error
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop() // cfg.Logger may be used below
		errs = append(errs, errors.New("kafka: logger must be set"))
	} else {
		cfg.Logger = cfg.Logger.Named("kafka")
	}
	if cfg.Namespace != "" {
		cfg.Logger = cfg.Logger.With(zap.String("namespace", cfg.Namespace))
	}
	if cfg.ConfigFile == "" {
		cfg.ConfigFile = os.Getenv("KAFKA_CONFIG_FILE")
	}
	if cfg.ConfigFile != "" {
		configFileHook, brokers, saslMechanism, err := newConfigFileHook(cfg.ConfigFile, cfg.Logger)
		if err != nil {
			errs = append(errs, fmt.Errorf("kafka: error loading config file: %w", err))
		} else {
			cfg.hooks = append(cfg.hooks, configFileHook)
			if len(brokers) != 0 {
				cfg.Brokers = brokers
			}
			if saslMechanism != nil {
				cfg.SASL = saslMechanism
			}
		}
	}
	if len(cfg.Brokers) == 0 {
		if v := os.Getenv("KAFKA_BROKERS"); v != "" {
			cfg.Brokers = strings.Split(v, ",")
		} else {
			errs = append(errs, errors.New("kafka: at least one broker must be set"))
		}
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
		saslConfig := saslConfigProperties{
			Mechanism: os.Getenv("KAFKA_SASL_MECHANISM"),
			Username:  os.Getenv("KAFKA_USERNAME"),
			Password:  os.Getenv("KAFKA_PASSWORD"),
		}
		if err := saslConfig.finalize(); err != nil {
			errs = append(errs, fmt.Errorf("kafka: error configuring SASL: %w", err))
		} else {
			switch saslConfig.Mechanism {
			case "PLAIN":
				plainAuth := plain.Auth{
					User: saslConfig.Username,
					Pass: saslConfig.Password,
				}
				if plainAuth != (plain.Auth{}) {
					cfg.SASL = plainAuth.AsMechanism()
				}
			case "AWS_MSK_IAM":
				var err error
				cfg.SASL, err = newAWSMSKIAMSASL()
				if err != nil {
					errs = append(errs, fmt.Errorf("kafka: error configuring SASL/AWS_MSK_IAM: %w", err))
				}
			}
		}
	}
	cfg.TopicLogFieldFunc = topicFieldFunc(cfg.TopicLogFieldFunc)
	return errors.Join(errs...)
}

func (cfg *CommonConfig) namespacePrefix() string {
	if cfg.Namespace == "" {
		return ""
	}
	return cfg.Namespace + "-"
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

func (cfg *CommonConfig) newClient(topicAttributeFunc TopicAttributeFunc, additionalOpts ...kgo.Opt) (*kgo.Client, error) {
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
		metricHooks, err := newKgoHooks(cfg.meterProvider(),
			cfg.Namespace, cfg.namespacePrefix(), topicAttributeFunc,
		)
		if err != nil {
			return nil, fmt.Errorf("kafka: failed creating kgo metrics hooks: %w", err)
		}
		opts = append(opts,
			kgo.WithHooks(metricHooks, &loggerHook{logger: cfg.Logger}),
		)
	}
	if cfg.MetadataMaxAge > 0 {
		opts = append(opts, kgo.MetadataMaxAge(cfg.MetadataMaxAge))
	}
	if len(cfg.hooks) != 0 {
		opts = append(opts, kgo.WithHooks(cfg.hooks...))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed creating kafka client: %w", err)
	}
	// Issue a metadata refresh request on construction, so the broker list is populated.
	client.ForceMetadataRefresh()
	return client, nil
}

func newAWSMSKIAMSASL() (sasl.Mechanism, error) {
	return aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
		awscfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return aws.Auth{}, fmt.Errorf("kafka: error loading AWS config: %w", err)
		}
		creds, err := awscfg.Credentials.Retrieve(ctx)
		if err != nil {
			return aws.Auth{}, err
		}
		return aws.Auth{
			AccessKey:    creds.AccessKeyID,
			SecretKey:    creds.SecretAccessKey,
			SessionToken: creds.SessionToken,
		}, nil
	}), nil
}

func topicFieldFunc(f TopicLogFieldFunc) TopicLogFieldFunc {
	return func(t string) zap.Field {
		if f == nil {
			return zap.Skip()
		}
		if field := f(t); field.Type > zapcore.UnknownType {
			return field
		}
		return zap.Skip()
	}
}
