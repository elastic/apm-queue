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

// Package json provides a JSON encoder/decoder.
package json

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel/metric"

	"github.com/elastic/apm-data/model"
)

// JSON wraps the standard json library.
type JSON struct {
	encoded metric.Int64Counter
	decoded metric.Int64Counter
}

// Encode accepts a model.APMEvent and returns the encoded JSON representation.
func (e JSON) Encode(in model.APMEvent) ([]byte, error) {
	b, err := json.Marshal(in)
	if e.encoded != nil {
		e.encoded.Add(context.Background(), int64(len(b)))
	}
	return b, err
}

// Decode decodes an encoded model.APM Event into its struct form.
func (e JSON) Decode(in []byte, out *model.APMEvent) error {
	if e.decoded != nil {
		e.decoded.Add(context.Background(), int64(len(in)))
	}
	return json.Unmarshal(in, out)
}

// RecordEncodedBytes configures the passed JSON codec to record the number of
// bytes that have been encoded into JSON.
func RecordEncodedBytes(c *JSON, m metric.Int64Counter) *JSON {
	c.encoded = m
	return c
}

// RecordDecodedBytes configures the passed JSON codec to record the number of
// bytes that have been decoded.
func RecordDecodedBytes(c *JSON, m metric.Int64Counter) *JSON {
	c.decoded = m
	return c
}
