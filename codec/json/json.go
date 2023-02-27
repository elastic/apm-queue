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
	"encoding/json"

	"github.com/elastic/apm-data/model"
)

// JSON wraps the standard json library.
type JSON struct{}

// Encode accepts a model.APMEvent and returns the encoded JSON representation.
func (e JSON) Encode(in model.APMEvent) ([]byte, error) {
	return json.Marshal(in)
}

// Decode decodes an encoded model.APM Event into its struct form.
func (e JSON) Decode(in []byte, out *model.APMEvent) error {
	return json.Unmarshal(in, out)
}
