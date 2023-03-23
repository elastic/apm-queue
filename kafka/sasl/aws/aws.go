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

// Package saslaws wraps the creation the AWS MSK IAM sasl.Mechanism.
package saslaws

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/aws"
)

// New returns a new sasl.Mechanism from the AWS session credentials.
func New(session *session.Session, userAgent string) sasl.Mechanism {
	return aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
		val, err := session.Config.Credentials.GetWithContext(ctx)
		if err != nil {
			return aws.Auth{}, err
		}
		return aws.Auth{
			AccessKey:    val.AccessKeyID,
			SecretKey:    val.SecretAccessKey,
			SessionToken: val.SessionToken,
			UserAgent:    userAgent,
		}, nil
	})
}
