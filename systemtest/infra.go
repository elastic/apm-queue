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

package systemtest

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/hc-install/product"
	"github.com/hashicorp/hc-install/releases"
	"github.com/hashicorp/terraform-exec/tfexec"
)

// NewTerraform returns a terraform executable for the specified path. It
// installs the latest version.
func NewTerraform(ctx context.Context, path string) (*tfexec.Terraform, error) {
	execPath, err := installTerraform(ctx)
	if err != nil {
		return nil, err
	}
	return tfexec.NewTerraform(path, execPath)
}

var terraformExecOnce sync.Once
var terraformExec string

func installTerraform(ctx context.Context) (string, error) {
	var err error
	terraformExecOnce.Do(func() {
		installer := &releases.LatestVersion{
			Product: product.Terraform,
			Timeout: 5 * time.Minute,
		}
		terraformExec, err = installer.Install(ctx)
		return
	})
	return terraformExec, err
}

var destroyMu sync.Mutex
var destroyFuncs map[string]func()

// RegisterDestroy registers a cleanup or destroy function to be run after the
// tests have been run.
func RegisterDestroy(key string, f func()) {
	destroyMu.Lock()
	defer destroyMu.Unlock()
	if destroyFuncs == nil {
		destroyFuncs = make(map[string]func())
	}
	destroyFuncs[key] = f
}

// Destroy runs all the registered destroy hooks.
func Destroy() {
	destroyMu.Lock()
	defer destroyMu.Unlock()
	for _, f := range destroyFuncs {
		f()
	}
}

var persistentSuffix string

func init() {
	rand.Seed(time.Now().Unix())
	persistentSuffix = RandomSuffix()
	googleProject = os.Getenv("GOOGLE_PROJECT")
	if googleProject == "" {
		logger().Warn("GOOGLE_PROJECT environment variable not set")
	}
	googleRegion = os.Getenv("GOOGLE_REGION")
	if googleRegion == "" {
		logger().Warn("GOOGLE_REGION environment variable not set")
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyz"

// RandomSuffix generates a lowercase alphabetic 8 character random string
func RandomSuffix() string {
	b := make([]byte, 8)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

// SuffixTopics suffixes the received topics with a random suffix.
func SuffixTopics(topics ...string) []string {
	suffix := RandomSuffix()
	suffixed := make([]string, len(topics))
	for i := range suffixed {
		suffixed[i] = fmt.Sprintf("%s.%s", strings.ToLower(topics[i]), suffix)
	}
	return suffixed
}
