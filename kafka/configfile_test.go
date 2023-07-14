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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLoadConfigFile(t *testing.T) {
	logger := zap.NewNop()

	t.Run("file does not exist", func(t *testing.T) {
		// create a temp dir, but don't create any file inside
		tempdir := t.TempDir()
		configFilePath := filepath.Join(tempdir, "config.yaml")
		_, err := loadConfigFile(configFilePath, logger)
		require.Error(t, err)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("file contents are invalid", func(t *testing.T) {
		configFilePath := writeConfigFile(t, "invalid!")
		_, err := loadConfigFile(configFilePath, logger)
		require.Error(t, err)
		assert.Regexp(t, "error parsing kafka config file .*", err.Error())
	})

	t.Run("file contents are empty", func(t *testing.T) {
		configFilePath := writeConfigFile(t, "")
		config, err := loadConfigFile(configFilePath, logger)
		require.NoError(t, err)
		assert.Zero(t, config)
	})

	t.Run("file contents are non-empty", func(t *testing.T) {
		configFilePath := writeConfigFile(t, `# a comment
bootstrap:
  servers: "a,b,c"
sasl:
  username: "user_name" # another password
  password: "pass_word"`)
		config, err := loadConfigFile(configFilePath, logger)
		require.NoError(t, err)
		assert.Equal(t, "a,b,c", config.Bootstrap.Servers)
		assert.Equal(t, "user_name", config.SASL.Username)
		assert.Equal(t, "pass_word", config.SASL.Password)
	})
}

func writeConfigFile(t testing.TB, content string) string {
	t.Helper()
	tempdir := t.TempDir()
	path := filepath.Join(tempdir, "config.yaml")
	err := os.WriteFile(path, []byte(content), 0644)
	require.NoError(t, err)
	return path
}
