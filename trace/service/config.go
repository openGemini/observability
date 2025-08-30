// Copyright 2024 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"fmt"
	"log/slog"

	"github.com/goccy/go-yaml"
	"github.com/openGemini/opengemini-client-go/opengemini"
)

const (
	DefaultHost               = "0.0.0.0"
	DefaultPort               = 18086
	DefaultOpenGeminiHost     = "0.0.0.0"
	DefaultOpenGeminiPort     = 8086
	DefaultOpenGeminiGrpcPort = 8305
	DefaultDatabase           = "jaeger_storage"
	DefaultRetentionPolicy    = "trace"
	DefaultRetentionDuration  = "3d"
)

type Config struct {
	Host              string `yaml:"host"`
	Port              int    `yaml:"port"`
	Log               *Log   `yaml:"log"`
	*OpenGeminiConfig `yaml:"opengemini"`
}

func (c *Config) String() {
	content, err := yaml.Marshal(c)
	if err != nil {
		slog.Error("print config failed", "reason", err)
		return
	}
	fmt.Printf("used yaml config:\n%s\n", string(content))
}

type OpenGeminiConfig struct {
	Address           []*OpenGeminiConfigAddress `yaml:"address"`
	Auth              *OpenGeminiConfigAuth      `yaml:"auth"`
	Database          string                     `yaml:"database"`
	RetentionPolicy   string                     `yaml:"retention_policy"`
	RetentionDuration string                     `yaml:"retention_duration"`
	GrpcWrite         *OpenGeminiGrpcWrite       `yaml:"grpc_write"`
}

func (oc *OpenGeminiConfig) GetOpenGeminiAddress(address []*OpenGeminiConfigAddress) []opengemini.Address {
	var oa []opengemini.Address
	for _, addr := range address {
		oa = append(oa, opengemini.Address{
			Host: addr.Host,
			Port: addr.Port,
		})
	}
	return oa
}

func (oc *OpenGeminiConfig) GetOpenGeminiAuth() *opengemini.AuthConfig {
	if oc.Auth == nil {
		return nil
	}
	return &opengemini.AuthConfig{
		AuthType: opengemini.AuthTypePassword,
		Username: oc.Auth.Username,
		Password: oc.Auth.Password,
	}
}

type OpenGeminiConfigAddress struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type OpenGeminiConfigAuth struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type OpenGeminiGrpcWrite struct {
	Address []*OpenGeminiConfigAddress `yaml:"address"`
}

func (oc *OpenGeminiConfig) GetOpenGeminiGrpcWrite() *opengemini.GrpcConfig {
	if oc.GrpcWrite == nil {
		return nil
	}
	return &opengemini.GrpcConfig{
		Addresses:  oc.GetOpenGeminiAddress(oc.GrpcWrite.Address),
		AuthConfig: oc.GetOpenGeminiAuth(),
	}
}

type Log struct {
	Type   string `yaml:"type"`
	Level  string `yaml:"level"`
	Output string `yaml:"output"`
}
