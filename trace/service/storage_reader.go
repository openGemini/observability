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
	"errors"
	"fmt"
	"log/slog"

	"github.com/openGemini/opengemini-client-go/opengemini"
	otlpcommonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	otlpresourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	otlptracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	"github.com/openGemini/observability/trace/gen/jaeger_storage/v2"
	"github.com/openGemini/observability/trace/utils"
)

var (
	_ jaeger_storage.TraceReaderServer      = (*OpenGeminiStorage)(nil)
	_ jaeger_storage.DependencyReaderServer = (*OpenGeminiStorage)(nil)
)

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
		o.logger.Error("failed to show measurements", "database", o.config.Database, "reason", err)
		return nil, err
	}
	var response = new(jaeger_storage.GetServicesResponse)
	response.Services = measurements
	return response, nil
}

func (o *OpenGeminiStorage) GetOperations(ctx context.Context, request *jaeger_storage.GetOperationsRequest) (*jaeger_storage.GetOperationsResponse, error) {
	values, err := o.client.ShowTagValues(opengemini.NewShowTagValuesBuilder().Database(o.config.Database).
		RetentionPolicy(o.config.RetentionPolicy).Measurement(request.Service).With(SpanName),
	)
	if err != nil && errors.Is(err, opengemini.ErrEmptyTagKey) {
		o.logger.Error("failed to show tag values", "database", o.config.Database, "service", request.Service, "reason", err)
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
		conditions = append(conditions, opengemini.NewComparisonCondition(SpanName, opengemini.Equals, query.OperationName))
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
		conditions = append(conditions, opengemini.NewComparisonCondition(DurationUnixNano, opengemini.GreaterThan, duration))
	}
	if query.DurationMax != nil && query.DurationMax.AsDuration().Nanoseconds() != 0 {
		duration := query.DurationMax.AsDuration().Nanoseconds()
		conditions = append(conditions, opengemini.NewComparisonCondition(DurationUnixNano, opengemini.LessThan, duration))
	}
	if len(conditions) != 0 {
		queryBuilder = queryBuilder.Where(opengemini.NewCompositeCondition(opengemini.And, conditions...))
	}
	if query.SearchDepth != 0 {
		queryBuilder.Limit(int64(query.SearchDepth))
	}
	var requestData = queryBuilder.Build()
	requestData.Database = o.config.Database
	o.logger.Debug("FindTraces query execute", "cmd", requestData.Command)
	queryResult, err := o.client.Query(*requestData)
	if err != nil {
		o.logger.Error("failed to find traces", "query", requestData.Command, "reason", err)
		return err
	}
	o.logger.Debug("FindTraces query execute", "cmd", requestData.Command, "body", utils.MarshalAnything(queryResult))
	if queryResult.Error != "" {
		o.logger.Error("failed to find traces", "query", requestData.Command, "reason", queryResult.Error)
		return errors.New(queryResult.Error)
	}

	traceData, err := o.convertTraceData(query, queryResult.Results)
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

func (o *OpenGeminiStorage) convertTraceData(query *jaeger_storage.TraceQueryParameters, results []*opengemini.SeriesResult) (*otlptracev1.TracesData, error) {
	var response = new(otlptracev1.TracesData)
	if len(results) == 0 {
		o.logger.Warn("no results to convert")
		return response, nil
	}

	seriesResult := results[0]
	if seriesResult.Error != "" {
		o.logger.Error("convertTraceData failed", "reason", seriesResult.Error)
		return response, nil
	}

	seriesList := seriesResult.Series
	var resourceSpans = new(otlptracev1.ResourceSpans)
	var scopeSpans = new(otlptracev1.ScopeSpans)
	for _, series := range seriesList {
		var lines = series.Values
		for _, columns := range lines {
			var span = new(otlptracev1.Span)
			for idx, columnValue := range columns {
				if series.Columns[idx] == ColumnNameTimestamp || series.Columns[idx] == ProcessTags ||
					series.Columns[idx] == DurationUnixNano {
					continue
				}
				if series.Columns[idx] == TraceID {
					span.TraceId, _ = hex.DecodeString(columnValue.(string))
					continue
				}
				if series.Columns[idx] == SpanID {
					span.SpanId, _ = hex.DecodeString(columnValue.(string))
					continue
				}
				if series.Columns[idx] == ParentSpanID {
					span.ParentSpanId, _ = hex.DecodeString(columnValue.(string))
					continue
				}
				if series.Columns[idx] == Flags {
					span.Flags = uint32(columnValue.(float64))
					continue
				}
				if series.Columns[idx] == SpanName {
					span.Name = columnValue.(string)
					continue
				}
				if series.Columns[idx] == SpanKind {
					span.Kind = otlptracev1.Span_SpanKind(columnValue.(float64))
					continue
				}
				if series.Columns[idx] == StartTimeUnixNano {
					span.StartTimeUnixNano = uint64(columnValue.(float64))
					continue
				}
				if series.Columns[idx] == EndTimeUnixNano {
					span.EndTimeUnixNano = uint64(columnValue.(float64))
					continue
				}
				span.Attributes = append(span.Attributes, &otlpcommonv1.KeyValue{
					Key:   series.Columns[idx],
					Value: &otlpcommonv1.AnyValue{Value: &otlpcommonv1.AnyValue_StringValue{StringValue: fmt.Sprintf("%v", columnValue)}},
				})
			}
			scopeSpans.Spans = append(scopeSpans.Spans, span)
		}
	}
	resourceSpans.ScopeSpans = append(resourceSpans.ScopeSpans, scopeSpans)
	resourceSpans.Resource = &otlpresourcev1.Resource{
		Attributes: []*otlpcommonv1.KeyValue{
			{
				Key:   ServiceName,
				Value: &otlpcommonv1.AnyValue{Value: &otlpcommonv1.AnyValue_StringValue{StringValue: query.ServiceName}},
			},
		},
	}
	response.ResourceSpans = append(response.ResourceSpans, resourceSpans)
	return response, nil
}

func (o *OpenGeminiStorage) FindTraceIDs(ctx context.Context, request *jaeger_storage.FindTracesRequest) (*jaeger_storage.FindTraceIDsResponse, error) {
	//TODO implement me
	slog.Error("FindTraceIDs called")
	return &jaeger_storage.FindTraceIDsResponse{}, nil
}
