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
	"context"
	"io"
	"log/slog"
	"os"

	"github.com/openGemini/opengemini-client-go/opengemini"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	otlptracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/openGemini/observability/trace/utils"
)

const (
	ColumnNameTimestamp = "time"
	ServiceName         = "service.name"
	TraceID             = "trace_id"
	SpanID              = "span_id"
	ParentSpanID        = "parent_span_id"
	SpanKind            = "span.kind"
	SpanName            = "span.name"
	Flags               = "flags"
	StatusCode          = "status_code"
	StatusMessage       = "status_message"
	StartTimeUnixNano   = "start_time_unix_nano"
	EndTimeUnixNano     = "end_time_unix_nano"
	DurationUnixNano    = "duration_unix_nano"
	ProcessTags         = "process_tags"
	SchemaUrl           = "schema_url"
)

const (
	LogTypeJson = "json"
	LogTypeText = "text"
)

var (
	_ otlptrace.Client = (*OpenGeminiStorage)(nil)
)

type OpenGeminiStorage struct {
	client opengemini.Client
	config *Config
	logger *slog.Logger
}

func (o *OpenGeminiStorage) Start(ctx context.Context) error {
	return nil
}

func (o *OpenGeminiStorage) Stop(ctx context.Context) error {
	return nil
}

// UploadTraces [api] push span to openGemini server from rest api
func (o *OpenGeminiStorage) UploadTraces(ctx context.Context, protoSpans []*otlptracev1.ResourceSpans) error {
	var points []*opengemini.Point
	for _, resourceSpan := range protoSpans {
		resource := resourceSpan.GetResource()
		for _, scopeSpan := range resourceSpan.ScopeSpans {
			scope := scopeSpan.GetScope()
			for _, span := range scopeSpan.Spans {
				point := o.convertSpanToPoint(resource, scope, span)
				point.Fields["schema_url"] = scopeSpan.SchemaUrl
				points = append(points, point)
			}
		}
	}

	if len(points) > 0 {
		err := o.pushTraceData(ctx, points)
		if err != nil {
			o.logger.Error("failed to write points to OpenGemini", "reason", err, "count", len(points))
			return err
		}
		o.logger.Info("successfully exported traces to OpenGemini", "count", len(points))
	}

	return nil
}

func NewOpenGeminiStorage(cfg *Config) (*OpenGeminiStorage, error) {
	client, err := opengemini.NewClient(&opengemini.Config{
		Addresses:  cfg.GetOpenGeminiAddress(cfg.Address),
		AuthConfig: cfg.GetOpenGeminiAuth(),
		GrpcConfig: cfg.GetOpenGeminiGrpcWrite(),
	})
	if err != nil {
		return nil, err
	}
	var engine = &OpenGeminiStorage{client: client, config: cfg}

	var logOuts = []io.Writer{os.Stdout}
	if cfg.Log != nil {
		if cfg.Log.Output != "" {
			// TODO file close
			logFile, err := os.OpenFile(cfg.Log.Output, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				slog.Error("failed to open log file", "reason", err, "file", cfg.Log.Output)
				return nil, err
			}
			logOuts = append(logOuts, logFile)
		}
		multiWriter := io.MultiWriter(logOuts...)
		switch cfg.Log.Type {
		case LogTypeJson:
			engine.logger = slog.New(slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{Level: utils.SlogLevel(cfg.Log.Level)}))
		case LogTypeText:
			fallthrough
		default:
			engine.logger = slog.New(slog.NewTextHandler(multiWriter, &slog.HandlerOptions{Level: utils.SlogLevel(cfg.Log.Level)}))
		}
	}

	// create database
	err = client.CreateDatabase(cfg.Database)
	if err != nil {
		engine.logger.Error("failed to create database", "database", cfg.Database, "reason", err)
		return nil, err
	}
	// create retention policy
	err = client.CreateRetentionPolicy(cfg.Database, opengemini.RpConfig{
		Name:     cfg.RetentionPolicy,
		Duration: cfg.RetentionDuration,
	}, true)
	if err != nil {
		engine.logger.Error("failed to create retention policy", "database", cfg.Database, "rp", cfg.RetentionPolicy,
			"duration", cfg.RetentionDuration, "reason", err)
		return nil, err
	}
	return engine, nil
}
