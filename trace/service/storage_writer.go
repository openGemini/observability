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
	"encoding/hex"
	"strings"
	"time"

	"github.com/openGemini/opengemini-client-go/opengemini"
	otlpcommonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	otlpresourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	otlptracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	collectortrace "github.com/openGemini/observability/trace/gen/otlp/collector/trace/v1"
	"github.com/openGemini/observability/trace/utils"
)

var (
	_ collectortrace.TraceServiceServer = (*OpenGeminiStorage)(nil)
)

// Export [grpc] push span to openGemini server between one Application instrumented with
// OpenTelemetry and a collector, or between a collector and a central collector
func (o *OpenGeminiStorage) Export(ctx context.Context, request *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	o.logger.Debug("Export action request body", "content", utils.MarshalAnything(request))
	var points []*opengemini.Point
	for _, resourceSpan := range request.ResourceSpans {
		resource := resourceSpan.GetResource()
		for _, scopeSpan := range resourceSpan.ScopeSpans {
			scope := scopeSpan.GetScope()
			for _, span := range scopeSpan.Spans {
				point := o.convertSpanToPoint(resource, scope, span)
				if scopeSpan.SchemaUrl != "" {
					point.Fields[SchemaUrl] = scopeSpan.SchemaUrl
				} else if resourceSpan.SchemaUrl != "" {
					point.Fields[SchemaUrl] = resourceSpan.SchemaUrl
				}
				points = append(points, point)
			}
		}
	}

	err := o.pushTraceData(ctx, points)
	if err != nil {
		o.logger.Error("failed to write points to OpenGemini", "reason", err, "count", len(points))
		return nil, err
	}
	o.logger.Debug("successfully exported traces to OpenGemini", "count", len(points))

	var response = new(collectortrace.ExportTraceServiceResponse)
	return response, nil
}

// convertSpanToPoint convert OTLP span to OpenGemini point
func (o *OpenGeminiStorage) convertSpanToPoint(resource *otlpresourcev1.Resource, scope *otlpcommonv1.InstrumentationScope, span *otlptracev1.Span) *opengemini.Point {
	point := &opengemini.Point{
		Tags:   make(map[string]string),
		Fields: make(map[string]interface{}),
	}

	var processTags []string
	for _, attr := range resource.Attributes {
		if attr.Key == ServiceName {
			point.Measurement = utils.GetAnyValueString(attr.Value)
		}
		if attr.Key != "" && attr.Value != nil {
			point.Tags[attr.Key] = utils.GetAnyValueString(attr.Value)
			processTags = append(processTags, attr.Key)
		}
	}

	// merge scope attributes to tags
	if scope != nil {
		if scope.Name != "" {
			point.Tags["scope.name"] = scope.Name
			processTags = append(processTags, "scope.name")
		}
		if scope.Version != "" {
			point.Tags["scope.version"] = scope.Version
			processTags = append(processTags, "scope.version")
		}
		for _, attr := range scope.GetAttributes() {
			if attr.Key != "" && attr.Value != nil {
				point.Tags["scope."+attr.Key] = utils.GetAnyValueString(attr.Value)
				processTags = append(processTags, "scope."+attr.Key)
			}
		}
	}

	if span.Name != "" {
		point.Tags[SpanName] = span.Name
		processTags = append(processTags, SpanName)
	} else {
		point.Tags[SpanName] = "UnknownOperationName"
	}

	point.Fields[ProcessTags] = strings.Join(processTags, ",")

	// put span attribute to fields
	if span.TraceId != nil {
		point.Fields[TraceID] = hex.EncodeToString(span.TraceId)
	}
	if span.SpanId != nil {
		point.Fields[SpanID] = hex.EncodeToString(span.SpanId)
	}
	if len(span.ParentSpanId) > 0 {
		point.Fields[ParentSpanID] = hex.EncodeToString(span.ParentSpanId)
	}

	point.Fields[SpanKind] = int64(span.Kind)
	point.Fields[Flags] = int64(span.Flags)

	for _, attr := range span.Attributes {
		point.Fields[attr.Key] = utils.GetAnyValue(attr.Value)
	}

	if span.Status != nil {
		if span.Status.Code != otlptracev1.Status_STATUS_CODE_UNSET {
			point.Fields[StatusCode] = span.Status.Code.String()
		}
		if span.Status.Message != "" {
			point.Fields[StatusMessage] = span.Status.Message
		}
	}

	if span.StartTimeUnixNano > 0 {
		point.Timestamp = int64(span.StartTimeUnixNano)
	} else {
		point.Timestamp = time.Now().UnixNano()
	}

	point.Fields[DurationUnixNano] = int64(span.EndTimeUnixNano - span.StartTimeUnixNano)
	point.Fields[StartTimeUnixNano] = int64(span.StartTimeUnixNano)
	point.Fields[EndTimeUnixNano] = int64(span.EndTimeUnixNano)

	if len(span.Events) > 0 {
		point.Fields["events_count"] = len(span.Events)
	}

	if len(span.Links) > 0 {
		point.Fields["links_count"] = len(span.Links)
	}

	return point
}

func (o *OpenGeminiStorage) pushTraceData(ctx context.Context, points []*opengemini.Point) error {
	if len(points) == 0 {
		o.logger.Warn("no points to push")
		return nil
	}
	// request builder
	builder, err := opengemini.NewWriteRequestBuilder(o.config.Database, o.config.RetentionPolicy)
	if err != nil {
		return err
	}
	var lines []opengemini.RecordLine
	for _, point := range points {
		// record build
		recordBuilder, err := opengemini.NewRecordBuilder(point.Measurement)
		if err != nil {
			return err
		}
		line := recordBuilder.NewLine().AddTags(point.Tags).AddFields(point.Fields).Build(point.Timestamp)
		lines = append(lines, line)
	}
	builder.AddRecord(lines...)
	writeRequest, err := builder.Build()
	if err != nil {
		return err
	}
	return o.client.WriteByGrpc(ctx, writeRequest)
}
