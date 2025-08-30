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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/opengemini-client-go/opengemini"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	otlpcommonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	otlpresourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	otlptracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	"github.com/openGemini/observability/trace/gen/jaeger_storage/v2"
	collectortrace "github.com/openGemini/observability/trace/gen/otlp/collector/trace/v1"
)

const (
	OperationName     = "operation_name"
	ServiceName       = "service.name"
	TraceID           = "trace_id"
	SpanID            = "span_id"
	ParentSpanID      = "parent_span_id"
	SpanKind          = "span_kind"
	Flags             = "flags"
	StatusCode        = "status_code"
	StatusMessage     = "status_message"
	StartTimeUnixNano = "start_time_unix_ns"
	EndTimeUnixNano   = "end_time_unix_ns"
	DurationNano      = "duration_ns"
	ProcessTags       = "process_tags"
	SchemaUrl         = "schema_url"
)

const (
	LogTypeJson = "json"
	LogTypeText = "text"
)

var (
	_ jaeger_storage.TraceReaderServer      = (*OpenGeminiStorage)(nil)
	_ jaeger_storage.DependencyReaderServer = (*OpenGeminiStorage)(nil)
	_ otlptrace.Client                      = (*OpenGeminiStorage)(nil)
	_ collectortrace.TraceServiceServer     = (*OpenGeminiStorage)(nil)
)

type OpenGeminiStorage struct {
	client opengemini.Client
	config *Config
	logger *slog.Logger
}

// getAnyValue convert OTLP AttributeValue to raw value
func getAnyValue(value *otlpcommonv1.AnyValue) any {
	switch v := value.Value.(type) {
	case *otlpcommonv1.AnyValue_StringValue:
		return v.StringValue
	case *otlpcommonv1.AnyValue_IntValue:
		return v.IntValue
	case *otlpcommonv1.AnyValue_DoubleValue:
		return v.DoubleValue
	case *otlpcommonv1.AnyValue_BoolValue:
		return v.BoolValue
	case *otlpcommonv1.AnyValue_ArrayValue:
		// 数组类型转换为JSON字符串
		if jsonBytes, err := json.Marshal(v.ArrayValue); err == nil {
			return string(jsonBytes)
		}
		return "[]"
	case *otlpcommonv1.AnyValue_KvlistValue:
		// KV列表类型转换为JSON字符串
		if jsonBytes, err := json.Marshal(v.KvlistValue); err == nil {
			return string(jsonBytes)
		}
		return "{}"
	default:
		return ""
	}
}

func getTagValueFromKeyValue(value *otlpcommonv1.AnyValue) string {
	anyValue := getAnyValue(value)
	switch av := anyValue.(type) {
	case string:
		return av
	case int64:
		return strconv.FormatInt(av, 10)
	case int:
		return strconv.Itoa(av)
	case int32:
		return strconv.Itoa(int(av))
	case float32:
		return strconv.FormatFloat(float64(av), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(av, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(av)
	}
	return ""
}

// convertSpanToPoint 将单个span转换为OpenGemini point
func (o *OpenGeminiStorage) convertSpanToPoint(resource *otlpresourcev1.Resource, scope *otlpcommonv1.InstrumentationScope, span *otlptracev1.Span) *opengemini.Point {
	point := &opengemini.Point{
		Tags:   make(map[string]string),
		Fields: make(map[string]interface{}),
	}

	var processTags []string
	for _, attr := range resource.Attributes {
		if attr.Key == ServiceName {
			point.Measurement = getTagValueFromKeyValue(attr.Value)
		}
		if attr.Key != "" && attr.Value != nil {
			point.Tags[attr.Key] = getTagValueFromKeyValue(attr.Value)
			processTags = append(processTags, attr.Key)
		}
	}

	// 合并scope attributes到tags
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
				point.Tags["scope."+attr.Key] = getTagValueFromKeyValue(attr.Value)
				processTags = append(processTags, "scope."+attr.Key)
			}
		}
	}

	point.Fields[ProcessTags] = strings.Join(processTags, ",")

	// 添加span的基本信息到tags
	if span.TraceId != nil {
		point.Fields[TraceID] = hex.EncodeToString(span.TraceId)
	}
	if span.SpanId != nil {
		point.Fields[SpanID] = hex.EncodeToString(span.SpanId)
	}
	if len(span.ParentSpanId) > 0 {
		point.Fields[ParentSpanID] = hex.EncodeToString(span.ParentSpanId)
	}
	if span.Name != "" {
		point.Fields[OperationName] = span.Name
	}
	point.Fields[SpanKind] = int64(span.Kind)
	point.Fields[Flags] = int64(span.Flags)

	for _, attr := range span.Attributes {
		point.Fields[attr.Key] = getAnyValue(attr.Value)
	}

	// 添加status信息到tags
	if span.Status != nil {
		if span.Status.Code != otlptracev1.Status_STATUS_CODE_UNSET {
			point.Fields[StatusCode] = span.Status.Code.String()
		}
		if span.Status.Message != "" {
			point.Fields[StatusMessage] = span.Status.Message
		}
	}

	// 设置时间戳
	if span.StartTimeUnixNano > 0 {
		point.Timestamp = int64(span.StartTimeUnixNano)
	} else {
		point.Timestamp = time.Now().UnixNano()
	}

	// 设置fields
	point.Fields[DurationNano] = int64(span.EndTimeUnixNano - span.StartTimeUnixNano)
	point.Fields[StartTimeUnixNano] = int64(span.StartTimeUnixNano)
	point.Fields[EndTimeUnixNano] = int64(span.EndTimeUnixNano)

	// 添加events数量
	if len(span.Events) > 0 {
		point.Fields["events_count"] = len(span.Events)
	}

	// 添加links数量
	if len(span.Links) > 0 {
		point.Fields["links_count"] = len(span.Links)
	}

	return point
}

// Export [grpc] push span to openGemini server between one Application instrumented with
// OpenTelemetry and a collector, or between a collector and a central collector
func (o *OpenGeminiStorage) Export(ctx context.Context, request *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	var points []*opengemini.Point
	for _, resourceSpan := range request.ResourceSpans {
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
			return nil, err
		}
		o.logger.Info("successfully exported traces to OpenGemini", "count", len(points))
	}

	var response = new(collectortrace.ExportTraceServiceResponse)
	return response, nil
}

func (o *OpenGeminiStorage) pushTraceData(ctx context.Context, points []*opengemini.Point) error {
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
			engine.logger = slog.New(slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{Level: levelString2SlogLevel(cfg.Log.Level)}))
		case LogTypeText:
			fallthrough
		default:
			engine.logger = slog.New(slog.NewTextHandler(multiWriter, &slog.HandlerOptions{Level: levelString2SlogLevel(cfg.Log.Level)}))
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

func (o *OpenGeminiStorage) GetDependencies(ctx context.Context, request *jaeger_storage.GetDependenciesRequest) (*jaeger_storage.GetDependenciesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (o *OpenGeminiStorage) GetTraces(request *jaeger_storage.GetTracesRequest, g grpc.ServerStreamingServer[otlptracev1.TracesData]) error {
	//TODO implement me
	slog.Info("GetTraces called")
	return nil
}

func (o *OpenGeminiStorage) GetServices(ctx context.Context, request *jaeger_storage.GetServicesRequest) (*jaeger_storage.GetServicesResponse, error) {
	measurements, err := o.client.ShowMeasurements(opengemini.NewMeasurementBuilder().Database(o.config.Database).
		RetentionPolicy(o.config.RetentionPolicy).Show())
	if err != nil {
		slog.Error("failed to show measurements", "database", o.config.Database, "reason", err)
		return nil, err
	}
	var response = new(jaeger_storage.GetServicesResponse)
	response.Services = measurements
	return response, nil
}

func (o *OpenGeminiStorage) GetOperations(ctx context.Context, request *jaeger_storage.GetOperationsRequest) (*jaeger_storage.GetOperationsResponse, error) {
	values, err := o.client.ShowTagValues(opengemini.NewShowTagValuesBuilder().Database(o.config.Database).
		RetentionPolicy(o.config.RetentionPolicy).Measurement(request.Service).With(OperationName),
	)
	if err != nil && errors.Is(err, opengemini.ErrEmptyTagKey) {
		slog.Error("failed to show tag values", "database", o.config.Database, "service", request.Service, "reason", err)
		return nil, err
	}
	var response = new(jaeger_storage.GetOperationsResponse)
	for _, value := range values {
		response.Operations = append(response.Operations, &jaeger_storage.Operation{
			Name:     value,
			SpanKind: request.SpanKind,
		})
	}
	return response, nil
}

func (o *OpenGeminiStorage) FindTraces(request *jaeger_storage.FindTracesRequest, g grpc.ServerStreamingServer[otlptracev1.TracesData]) error {
	var queryBuilder = opengemini.CreateQueryBuilder()
	var query = request.GetQuery()
	if query.ServiceName != "" {
		queryBuilder = queryBuilder.From(query.ServiceName)
	}
	var conditions []opengemini.Condition
	if query.OperationName != "" {
		conditions = append(conditions, opengemini.NewComparisonCondition(OperationName, opengemini.Equals, query.OperationName))
	}
	if query.StartTimeMin != nil && query.StartTimeMin.AsTime().UnixNano() != 0 {
		duration := query.StartTimeMin.AsTime().UnixNano()
		conditions = append(conditions, opengemini.NewComparisonCondition(StartTimeUnixNano, opengemini.GreaterThan, duration))
	}
	if query.StartTimeMax != nil && query.StartTimeMax.AsTime().UnixNano() != 0 {
		duration := query.StartTimeMax.AsTime().UnixNano()
		conditions = append(conditions, opengemini.NewComparisonCondition(EndTimeUnixNano, opengemini.LessThan, duration))
	}
	if query.DurationMin != nil && query.DurationMin.AsDuration().Nanoseconds() != 0 {
		duration := query.DurationMin.AsDuration().Nanoseconds()
		conditions = append(conditions, opengemini.NewComparisonCondition(DurationNano, opengemini.GreaterThan, duration))
	}
	if query.DurationMax != nil && query.DurationMax.AsDuration().Nanoseconds() != 0 {
		duration := query.DurationMax.AsDuration().Nanoseconds()
		conditions = append(conditions, opengemini.NewComparisonCondition(DurationNano, opengemini.LessThan, duration))
	}
	if len(conditions) != 0 {
		queryBuilder = queryBuilder.Where(opengemini.NewCompositeCondition(opengemini.And, conditions...))
	}
	if query.SearchDepth != 0 {
		queryBuilder.Limit(int64(query.SearchDepth))
	}
	var requestData = *queryBuilder.Build()
	requestData.Database = o.config.Database
	queryResult, err := o.client.Query(requestData)
	if err != nil {
		o.logger.Error("failed to find traces", "query", requestData.Command, "reason", err)
		return err
	}
	if queryResult.Error != "" {
		o.logger.Error("failed to find traces", "query", requestData.Command, "reason", queryResult.Error)
		return errors.New(queryResult.Error)
	}

	traceData, err := o.convertTraceData(queryResult.Results)
	if err != nil {
		o.logger.Error("failed to convert traces", "query", requestData.Command, "reason", err)
		return err
	}

	err = g.Send(traceData)
	if err != nil {
		o.logger.Error("failed to send traces", "query", requestData.Command, "reason", err)
		return err
	}
	return nil
}

func (o *OpenGeminiStorage) convertTraceData(results []*opengemini.SeriesResult) (*otlptracev1.TracesData, error) {
	var response = new(otlptracev1.TracesData)
	if len(results) == 0 {
		o.logger.Debug("no results to convert")
		return response, nil
	}

	o.logger.Debug("converting trace data", "results_count", len(results))

	// 按trace_id分组spans
	traceGroups := make(map[string][]*opengemini.SeriesResult)

	for _, result := range results {
		if result == nil || len(result.Series) == 0 {
			continue
		}

		for _, series := range result.Series {
			if series == nil || len(series.Values) == 0 || len(series.Columns) == 0 {
				continue
			}

			// 找到trace_id字段的索引
			traceIDIdx := -1
			for idx, column := range series.Columns {
				if column == TraceID {
					traceIDIdx = idx
				}
			}

			if traceIDIdx == -1 {
				continue
			}

			// 为每一行数据创建span
			for _, values := range series.Values {
				if values == nil || len(values) <= traceIDIdx {
					continue
				}

				traceID, ok := values[traceIDIdx].(string)
				if !ok || traceID == "" {
					continue
				}

				// 将当前series添加到对应的trace组
				if traceGroups[traceID] == nil {
					traceGroups[traceID] = make([]*opengemini.SeriesResult, 0)
				}

				// 创建一个新的SeriesResult，只包含当前行
				newSeries := &opengemini.Series{
					Columns: series.Columns,
					Values:  opengemini.SeriesValues{values},
				}
				newResult := &opengemini.SeriesResult{
					Series: []*opengemini.Series{newSeries},
				}
				traceGroups[traceID] = append(traceGroups[traceID], newResult)
			}
		}
	}

	// 为每个trace创建ResourceSpans
	for traceID, spanResults := range traceGroups {
		resourceSpans := o.convertTraceToResourceSpans(traceID, spanResults)
		if resourceSpans != nil {
			response.ResourceSpans = append(response.ResourceSpans, resourceSpans)
		}
	}

	return response, nil
}

func (o *OpenGeminiStorage) convertTraceToResourceSpans(traceID string, spanResults []*opengemini.SeriesResult) *otlptracev1.ResourceSpans {
	if len(spanResults) == 0 {
		return nil
	}

	resourceSpans := &otlptracev1.ResourceSpans{
		Resource: &otlpresourcev1.Resource{
			Attributes: make([]*otlpcommonv1.KeyValue, 0),
		},
		ScopeSpans: make([]*otlptracev1.ScopeSpans, 0),
	}

	// 按scope分组spans
	scopeGroups := make(map[string][]*opengemini.SeriesResult)

	for _, result := range spanResults {
		if result == nil || len(result.Series) == 0 {
			continue
		}

		for _, series := range result.Series {
			if series == nil || len(series.Values) == 0 || len(series.Columns) == 0 {
				continue
			}

			// 找到关键字段的索引
			fieldIndices := o.getFieldIndices(series.Columns)

			for _, values := range series.Values {
				if fieldIndices.processTagsIdx >= 0 && len(values) <= fieldIndices.processTagsIdx {
					continue
				}

				// 获取scope信息
				scopeKey := o.getScopeKey(values, fieldIndices)

				if scopeGroups[scopeKey] == nil {
					scopeGroups[scopeKey] = make([]*opengemini.SeriesResult, 0)
				}

				// 创建新的SeriesResult，只包含当前行
				newSeries := &opengemini.Series{
					Columns: series.Columns,
					Values:  opengemini.SeriesValues{values},
				}
				newResult := &opengemini.SeriesResult{
					Series: []*opengemini.Series{newSeries},
				}
				scopeGroups[scopeKey] = append(scopeGroups[scopeKey], newResult)
			}
		}
	}

	// 处理resource attributes（从第一个span获取）
	if len(spanResults) > 0 && len(spanResults[0].Series) > 0 {
		series := spanResults[0].Series[0]
		if len(series.Values) > 0 && len(series.Columns) > 0 {
			fieldIndices := o.getFieldIndices(series.Columns)
			resourceSpans.Resource.Attributes = o.extractResourceAttributes(series.Values[0], fieldIndices, series.Columns)
		}
	}

	// 为每个scope创建ScopeSpans
	for scopeKey, scopeResults := range scopeGroups {
		scopeSpans := o.convertScopeToScopeSpans(scopeKey, scopeResults)
		if scopeSpans != nil {
			resourceSpans.ScopeSpans = append(resourceSpans.ScopeSpans, scopeSpans)
		}
	}

	return resourceSpans
}

func (o *OpenGeminiStorage) convertScopeToScopeSpans(scopeKey string, scopeResults []*opengemini.SeriesResult) *otlptracev1.ScopeSpans {
	if len(scopeResults) == 0 {
		return nil
	}

	scopeSpans := &otlptracev1.ScopeSpans{
		Scope: &otlpcommonv1.InstrumentationScope{},
		Spans: make([]*otlptracev1.Span, 0),
	}

	// 从第一个span获取scope信息
	if len(scopeResults) > 0 && len(scopeResults[0].Series) > 0 {
		series := scopeResults[0].Series[0]
		if len(series.Values) > 0 && len(series.Columns) > 0 {
			fieldIndices := o.getFieldIndices(series.Columns)
			scopeSpans.Scope = o.extractScopeInfo(series.Values[0], fieldIndices, series.Columns)
		}
	}

	// 转换所有spans
	for _, result := range scopeResults {
		if result == nil || len(result.Series) == 0 {
			continue
		}

		for _, series := range result.Series {
			if series == nil || len(series.Values) == 0 || len(series.Columns) == 0 {
				continue
			}

			fieldIndices := o.getFieldIndices(series.Columns)

			for _, values := range series.Values {
				span := o.convertValuesToSpan(values, fieldIndices, series.Columns)
				if span != nil {
					scopeSpans.Spans = append(scopeSpans.Spans, span)
				}
			}
		}
	}

	return scopeSpans
}

func (o *OpenGeminiStorage) convertValuesToSpan(values []interface{}, indices *fieldIndices, columns []string) *otlptracev1.Span {
	if indices.processTagsIdx >= 0 && len(values) <= indices.processTagsIdx {
		return nil
	}

	span := &otlptracev1.Span{
		Attributes: make([]*otlpcommonv1.KeyValue, 0),
		Events:     make([]*otlptracev1.Span_Event, 0),
		Links:      make([]*otlptracev1.Span_Link, 0),
	}

	// 设置trace_id
	if indices.traceIDIdx >= 0 && indices.traceIDIdx < len(values) {
		if traceIDStr, ok := values[indices.traceIDIdx].(string); ok && traceIDStr != "" {
			if traceIDBytes, err := hex.DecodeString(traceIDStr); err == nil {
				span.TraceId = traceIDBytes
			}
		}
	}

	// 设置span_id
	if indices.spanIDIdx >= 0 && indices.spanIDIdx < len(values) {
		if spanIDStr, ok := values[indices.spanIDIdx].(string); ok && spanIDStr != "" {
			if spanIDBytes, err := hex.DecodeString(spanIDStr); err == nil {
				span.SpanId = spanIDBytes
			}
		}
	}

	// 设置parent_span_id
	if indices.parentSpanIDIdx >= 0 && indices.parentSpanIDIdx < len(values) {
		if parentSpanIDStr, ok := values[indices.parentSpanIDIdx].(string); ok && parentSpanIDStr != "" {
			if parentSpanIDBytes, err := hex.DecodeString(parentSpanIDStr); err == nil {
				span.ParentSpanId = parentSpanIDBytes
			}
		}
	}

	// 设置operation_name
	if indices.operationNameIdx >= 0 && indices.operationNameIdx < len(values) {
		if operationName, ok := values[indices.operationNameIdx].(string); ok {
			span.Name = operationName
		}
	}

	// 设置span_kind
	if indices.spanKindIdx >= 0 && indices.spanKindIdx < len(values) {
		if spanKind, ok := values[indices.spanKindIdx].(int64); ok {
			span.Kind = otlptracev1.Span_SpanKind(spanKind)
		}
	}

	// 设置flags
	if indices.flagsIdx >= 0 && indices.flagsIdx < len(values) {
		if flags, ok := values[indices.flagsIdx].(int64); ok {
			span.Flags = uint32(flags)
		}
	}

	// 设置时间戳
	if indices.startTimeIdx >= 0 && indices.startTimeIdx < len(values) {
		if startTime, ok := values[indices.startTimeIdx].(int64); ok {
			span.StartTimeUnixNano = uint64(startTime)
		}
	}

	if indices.endTimeIdx >= 0 && indices.endTimeIdx < len(values) {
		if endTime, ok := values[indices.endTimeIdx].(int64); ok {
			span.EndTimeUnixNano = uint64(endTime)
		}
	}

	// 设置status
	if indices.statusCodeIdx >= 0 && indices.statusCodeIdx < len(values) {
		if statusCodeStr, ok := values[indices.statusCodeIdx].(string); ok {
			span.Status = &otlptracev1.Status{
				Code: o.parseStatusCode(statusCodeStr),
			}
		}
	}

	if indices.statusMessageIdx >= 0 && indices.statusMessageIdx < len(values) {
		if span.Status == nil {
			span.Status = &otlptracev1.Status{}
		}
		if statusMessage, ok := values[indices.statusMessageIdx].(string); ok {
			span.Status.Message = statusMessage
		}
	}

	// 设置attributes（排除已知字段）
	span.Attributes = o.extractSpanAttributes(values, indices, columns)

	return span
}

type fieldIndices struct {
	traceIDIdx       int
	spanIDIdx        int
	parentSpanIDIdx  int
	operationNameIdx int
	spanKindIdx      int
	flagsIdx         int
	startTimeIdx     int
	endTimeIdx       int
	durationIdx      int
	statusCodeIdx    int
	statusMessageIdx int
	processTagsIdx   int
}

func (o *OpenGeminiStorage) getFieldIndices(columns []string) *fieldIndices {
	indices := &fieldIndices{
		traceIDIdx:       -1,
		spanIDIdx:        -1,
		parentSpanIDIdx:  -1,
		operationNameIdx: -1,
		spanKindIdx:      -1,
		flagsIdx:         -1,
		startTimeIdx:     -1,
		endTimeIdx:       -1,
		durationIdx:      -1,
		statusCodeIdx:    -1,
		statusMessageIdx: -1,
		processTagsIdx:   -1,
	}

	for idx, column := range columns {
		switch column {
		case TraceID:
			indices.traceIDIdx = idx
		case SpanID:
			indices.spanIDIdx = idx
		case ParentSpanID:
			indices.parentSpanIDIdx = idx
		case OperationName:
			indices.operationNameIdx = idx
		case SpanKind:
			indices.spanKindIdx = idx
		case Flags:
			indices.flagsIdx = idx
		case StartTimeUnixNano:
			indices.startTimeIdx = idx
		case EndTimeUnixNano:
			indices.endTimeIdx = idx
		case DurationNano:
			indices.durationIdx = idx
		case StatusCode:
			indices.statusCodeIdx = idx
		case StatusMessage:
			indices.statusMessageIdx = idx
		case ProcessTags:
			indices.processTagsIdx = idx
		}
	}

	return indices
}

func (o *OpenGeminiStorage) getScopeKey(values []interface{}, indices *fieldIndices) string {
	// 构建scope key，用于分组
	scopeName := ""
	scopeVersion := ""

	// 从process tags中提取scope信息
	if indices.processTagsIdx >= 0 && indices.processTagsIdx < len(values) {
		if processTagsStr, ok := values[indices.processTagsIdx].(string); ok {
			tags := strings.Split(processTagsStr, ",")
			for _, tag := range tags {
				if strings.HasPrefix(tag, "scope.name=") {
					scopeName = strings.TrimPrefix(tag, "scope.name=")
				}
				if strings.HasPrefix(tag, "scope.version=") {
					scopeVersion = strings.TrimPrefix(tag, "scope.version=")
				}
			}
		}
	}

	return scopeName + ":" + scopeVersion
}

func (o *OpenGeminiStorage) extractResourceAttributes(values []interface{}, indices *fieldIndices, columns []string) []*otlpcommonv1.KeyValue {
	attributes := make([]*otlpcommonv1.KeyValue, 0)

	if indices.processTagsIdx >= 0 && indices.processTagsIdx < len(values) {
		if processTagsStr, ok := values[indices.processTagsIdx].(string); ok {
			tags := strings.Split(processTagsStr, ",")

			for idx, value := range values {
				if idx == indices.processTagsIdx || idx >= len(columns) {
					continue
				}

				fieldName := columns[idx]
				if fieldName == "" {
					continue
				}

				// 检查是否是resource tag
				isResourceTag := false
				for _, tag := range tags {
					if tag == fieldName && !strings.HasPrefix(fieldName, "scope.") {
						isResourceTag = true
						break
					}
				}

				if isResourceTag {
					attr := &otlpcommonv1.KeyValue{
						Key:   fieldName,
						Value: o.convertValueToAnyValue(value),
					}
					attributes = append(attributes, attr)
				}
			}
		}
	}

	return attributes
}

func (o *OpenGeminiStorage) extractScopeInfo(values []interface{}, indices *fieldIndices, columns []string) *otlpcommonv1.InstrumentationScope {
	scope := &otlpcommonv1.InstrumentationScope{}

	// 从process tags中提取scope信息
	if indices.processTagsIdx >= 0 && indices.processTagsIdx < len(values) {
		if processTagsStr, ok := values[indices.processTagsIdx].(string); ok {
			tags := strings.Split(processTagsStr, ",")

			for idx, value := range values {
				if idx == indices.processTagsIdx || idx >= len(columns) {
					continue
				}

				fieldName := columns[idx]
				if fieldName == "" {
					continue
				}

				// 检查是否是scope tag
				isScopeTag := false
				for _, tag := range tags {
					if tag == fieldName && strings.HasPrefix(fieldName, "scope.") {
						isScopeTag = true
						break
					}
				}

				if isScopeTag {
					if fieldName == "scope.name" {
						if name, ok := value.(string); ok {
							scope.Name = name
						}
					} else if fieldName == "scope.version" {
						if version, ok := value.(string); ok {
							scope.Version = version
						}
					}
				}
			}
		}
	}

	return scope
}

func (o *OpenGeminiStorage) extractSpanAttributes(values []interface{}, indices *fieldIndices, columns []string) []*otlpcommonv1.KeyValue {
	attributes := make([]*otlpcommonv1.KeyValue, 0)

	// 排除已知的span字段，其余作为attributes
	knownFields := map[string]bool{
		TraceID:           true,
		SpanID:            true,
		ParentSpanID:      true,
		OperationName:     true,
		SpanKind:          true,
		Flags:             true,
		StartTimeUnixNano: true,
		EndTimeUnixNano:   true,
		DurationNano:      true,
		StatusCode:        true,
		StatusMessage:     true,
		ProcessTags:       true,
	}

	for idx, value := range values {
		if idx >= len(columns) {
			continue
		}

		fieldName := columns[idx]
		if fieldName == "" || knownFields[fieldName] {
			continue
		}

		// 检查是否是resource或scope tag
		if indices.processTagsIdx >= 0 && indices.processTagsIdx < len(values) {
			if processTagsStr, ok := values[indices.processTagsIdx].(string); ok {
				tags := strings.Split(processTagsStr, ",")
				isTag := false
				for _, tag := range tags {
					if tag == fieldName {
						isTag = true
						break
					}
				}
				if isTag {
					continue // 跳过resource和scope tags
				}
			}
		}

		attr := &otlpcommonv1.KeyValue{
			Key:   fieldName,
			Value: o.convertValueToAnyValue(value),
		}
		attributes = append(attributes, attr)
	}

	return attributes
}

func (o *OpenGeminiStorage) convertValueToAnyValue(value interface{}) *otlpcommonv1.AnyValue {
	switch v := value.(type) {
	case string:
		return &otlpcommonv1.AnyValue{Value: &otlpcommonv1.AnyValue_StringValue{StringValue: v}}
	case int64:
		return &otlpcommonv1.AnyValue{Value: &otlpcommonv1.AnyValue_IntValue{IntValue: v}}
	case int:
		return &otlpcommonv1.AnyValue{Value: &otlpcommonv1.AnyValue_IntValue{IntValue: int64(v)}}
	case int32:
		return &otlpcommonv1.AnyValue{Value: &otlpcommonv1.AnyValue_IntValue{IntValue: int64(v)}}
	case float64:
		return &otlpcommonv1.AnyValue{Value: &otlpcommonv1.AnyValue_DoubleValue{DoubleValue: v}}
	case float32:
		return &otlpcommonv1.AnyValue{Value: &otlpcommonv1.AnyValue_DoubleValue{DoubleValue: float64(v)}}
	case bool:
		return &otlpcommonv1.AnyValue{Value: &otlpcommonv1.AnyValue_BoolValue{BoolValue: v}}
	default:
		return &otlpcommonv1.AnyValue{Value: &otlpcommonv1.AnyValue_StringValue{StringValue: fmt.Sprintf("%v", v)}}
	}
}

func (o *OpenGeminiStorage) parseStatusCode(statusCodeStr string) otlptracev1.Status_StatusCode {
	switch statusCodeStr {
	case "STATUS_CODE_UNSET":
		return otlptracev1.Status_STATUS_CODE_UNSET
	case "STATUS_CODE_OK":
		return otlptracev1.Status_STATUS_CODE_OK
	case "STATUS_CODE_ERROR":
		return otlptracev1.Status_STATUS_CODE_ERROR
	default:
		return otlptracev1.Status_STATUS_CODE_UNSET
	}
}

func (o *OpenGeminiStorage) FindTraceIDs(ctx context.Context, request *jaeger_storage.FindTracesRequest) (*jaeger_storage.FindTraceIDsResponse, error) {
	//TODO implement me
	slog.Error("FindTraceIDs called")
	return &jaeger_storage.FindTraceIDsResponse{}, nil
}

func levelString2SlogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
