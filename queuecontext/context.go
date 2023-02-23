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

// Package queuecontext provides convenient wrappers for storing and
// accessing a stored project identifier.
package queuecontext

import "context"

type projectIDKey struct{}

// WithProject enriches a context with a project.
func WithProject(ctx context.Context, project string) context.Context {
	return context.WithValue(ctx, projectIDKey{}, project)
}

// ProjectFromContext returns the project ID from the passed context and a bool
// indicating whether the value is present or not.
func ProjectFromContext(ctx context.Context) (string, bool) {
	if v := ctx.Value(projectIDKey{}); v != nil {
		project, ok := v.(string)
		return project, ok
	}
	return "", false
}
