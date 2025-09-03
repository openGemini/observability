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
	"time"

	"github.com/openGemini/opengemini-client-go/opengemini"
	otlpcommonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	otlpresourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	otlptracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/openGemini/observability/trace/gen/jaeger_storage/v2"
	"github.com/openGemini/observability/trace/utils"
)

var (
	_ jaeger_storage.TraceReaderServer      = (*OpenGeminiStorage)(nil)
	_ jaeger_storage.DependencyReaderServer = (*OpenGeminiStorage)(nil)
)

func (o *OpenGeminiStorage) GetDependencies(ctx context.Context, request *jaeger_storage.GetDependenciesRequest) (*jaeger_storage.GetDependenciesResponse, error) {
	resp := &jaeger_storage.GetDependenciesResponse{}
	qb := opengemini.CreateQueryBuilder()
	if request != nil {
		var conds []opengemini.Condition
		if request.StartTime != nil && request.StartTime.AsTime().UnixNano() != 0 {
			conds = append(conds, opengemini.NewComparisonCondition(StartTimeUnixNano, opengemini.GreaterThan, request.StartTime.AsTime().UnixNano()))
		}
		if request.EndTime != nil && request.EndTime.AsTime().UnixNano() != 0 {
			conds = append(conds, opengemini.NewComparisonCondition(EndTimeUnixNano, opengemini.LessThan, request.EndTime.AsTime().UnixNano()))
		}
		if len(conds) > 0 {
			qb = qb.Where(opengemini.NewCompositeCondition(opengemini.And, conds...))
		}
	}
	rd := qb.Build()
	rd.Database = o.config.Database
	qr, err := o.client.Query(*rd)
	if err != nil {
		return nil, err
	}
	if qr.Error != "" {
		return nil, errors.New(qr.Error)
	}
	if len(qr.Results) == 0 || qr.Results[0] == nil || qr.Results[0].Error != "" {
		return resp, nil
	}
	type spanMeta struct {
		service string
		parent  string
		kind    int
	}
	traces := map[string]map[string]spanMeta{}
	for _, series := range qr.Results[0].Series {
		for _, row := range series.Values {
			var traceHex, spanHex, parentHex, serviceName string
			var kind int
			for idx, v := range row {
				col := series.Columns[idx]
				switch col {
				case TraceID:
					if v != nil {
						traceHex = v.(string)
					}
				case SpanID:
					if v != nil {
						spanHex = v.(string)
					}
				case ParentSpanID:
					if v != nil {
						parentHex = v.(string)
					}
				case SpanKind:
					if v != nil {
						kind = int(v.(float64))
					}
				case ServiceName:
					if v != nil {
						serviceName = fmt.Sprintf("%v", v)
					}
				}
			}
			if traceHex == "" || spanHex == "" {
				continue
			}
			m := traces[traceHex]
			if m == nil {
				m = map[string]spanMeta{}
			}
			m[spanHex] = spanMeta{service: serviceName, parent: parentHex, kind: kind}
			traces[traceHex] = m
		}
	}
	type edgeKey struct{ parent, child string }
	counts := map[edgeKey]uint64{}
	for _, spans := range traces {
		for _, sm := range spans {
			childSvc := sm.service
			if childSvc == "" {
				continue
			}
			parentSvc := ""
			if sm.parent != "" {
				if p, ok := spans[sm.parent]; ok {
					parentSvc = p.service
				}
			}
			if parentSvc == "" || parentSvc == childSvc {
				continue
			}
			counts[edgeKey{parent: parentSvc, child: childSvc}]++
		}
	}
	for ek, c := range counts {
		resp.Dependencies = append(resp.Dependencies, &jaeger_storage.Dependency{
			Parent:    ek.parent,
			Child:     ek.child,
			CallCount: c,
			Source:    "opengemini",
		})
	}
	return resp, nil
}

func (o *OpenGeminiStorage) GetTraces(request *jaeger_storage.GetTracesRequest, g grpc.ServerStreamingServer[otlptracev1.TracesData]) error {
	if request == nil || len(request.Query) == 0 {
		return nil
	}
	for _, qp := range request.Query {
		// 以 service 维度不在 GetTraces 提供，因此这里可能需要全库扫描；
		// 简化：若用户通过 FindTraceIDs 获得时间窗口，则带上窗口过滤。
		qb := opengemini.CreateQueryBuilder()
		// 无 measurement 信息时无法指定 From，这里选择全库由后端处理。
		conds := []opengemini.Condition{
			opengemini.NewComparisonCondition(TraceID, opengemini.Equals, hex.EncodeToString(qp.TraceId)),
		}
		if qp.StartTime != nil && qp.StartTime.AsTime().UnixNano() != 0 {
			conds = append(conds, opengemini.NewComparisonCondition(StartTimeUnixNano, opengemini.GreaterThan, qp.StartTime.AsTime().UnixNano()))
		}
		if qp.EndTime != nil && qp.EndTime.AsTime().UnixNano() != 0 {
			conds = append(conds, opengemini.NewComparisonCondition(EndTimeUnixNano, opengemini.LessThan, qp.EndTime.AsTime().UnixNano()))
		}
		qb = qb.Where(opengemini.NewCompositeCondition(opengemini.And, conds...))
		requestData := qb.Build()
		requestData.Database = o.config.Database
		resp, err := o.client.Query(*requestData)
		if err != nil {
			return err
		}
		if resp.Error != "" {
			return errors.New(resp.Error)
		}
		traceDataList, err := o.convertTraceData(&jaeger_storage.TraceQueryParameters{}, resp.Results)
		if err != nil {
			return err
		}
		for _, td := range traceDataList {
			if err := g.Send(td); err != nil {
				return err
			}
		}
	}
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

	traceDataList, err := o.convertTraceData(query, queryResult.Results)
	if err != nil {
		o.logger.Error("failed to convert traces", "query", requestData.Command, "reason", err)
		return err
	}
	for _, td := range traceDataList {
		if err := g.Send(td); err != nil {
			o.logger.Error("failed to send traces", "query", requestData.Command, "reason", err)
			return err
		}
	}
	return nil
}

func (o *OpenGeminiStorage) convertTraceData(query *jaeger_storage.TraceQueryParameters, results []*opengemini.SeriesResult) ([]*otlptracev1.TracesData, error) {
	var responseList []*otlptracev1.TracesData
	if len(results) == 0 {
		o.logger.Warn("no results to convert")
		return responseList, nil
	}

	seriesResult := results[0]
	if seriesResult.Error != "" {
		o.logger.Error("convertTraceData failed", "reason", seriesResult.Error)
		return responseList, nil
	}

	// group by trace_id
	type grouped struct {
		resourceSpans *otlptracev1.ResourceSpans
		scopeSpans    *otlptracev1.ScopeSpans
	}
	groups := make(map[string]*grouped)

	seriesList := seriesResult.Series
	for _, series := range seriesList {
		var lines = series.Values
		for _, columns := range lines {
			var span = new(otlptracev1.Span)
			var traceIDHex string
			for idx, columnValue := range columns {
				if series.Columns[idx] == ColumnNameTimestamp || series.Columns[idx] == ProcessTags ||
					series.Columns[idx] == DurationUnixNano {
					continue
				}
				if series.Columns[idx] == TraceID {
					hexStr := columnValue.(string)
					span.TraceId, _ = hex.DecodeString(hexStr)
					traceIDHex = hexStr
					continue
				}
				if series.Columns[idx] == SpanID {
					span.SpanId, _ = hex.DecodeString(columnValue.(string))
					continue
				}
				if series.Columns[idx] == ParentSpanID {
					if columnValue == nil {
						continue // if parent_span_id nil, not set
					}
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

			if traceIDHex == "" {
				continue
			}

			grp, ok := groups[traceIDHex]
			if !ok {
				grp = &grouped{
					resourceSpans: &otlptracev1.ResourceSpans{},
					scopeSpans:    &otlptracev1.ScopeSpans{},
				}
				grp.resourceSpans.Resource = &otlpresourcev1.Resource{
					Attributes: []*otlpcommonv1.KeyValue{
						{
							Key:   ServiceName,
							Value: &otlpcommonv1.AnyValue{Value: &otlpcommonv1.AnyValue_StringValue{StringValue: query.ServiceName}},
						},
					},
				}
				groups[traceIDHex] = grp
			}
			grp.scopeSpans.Spans = append(grp.scopeSpans.Spans, span)
		}
	}

	for _, grp := range groups {
		grp.resourceSpans.ScopeSpans = append(grp.resourceSpans.ScopeSpans, grp.scopeSpans)
		td := &otlptracev1.TracesData{ResourceSpans: []*otlptracev1.ResourceSpans{grp.resourceSpans}}
		responseList = append(responseList, td)
	}

	return responseList, nil
}

func (o *OpenGeminiStorage) FindTraceIDs(ctx context.Context, request *jaeger_storage.FindTracesRequest) (*jaeger_storage.FindTraceIDsResponse, error) {
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
	// 直接查询所有列，由代码侧抽取 trace_id 和时间窗口
	requestData := queryBuilder.Build()
	requestData.Database = o.config.Database
	resp, err := o.client.Query(*requestData)
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}

	result := &jaeger_storage.FindTraceIDsResponse{}
	if len(resp.Results) == 0 || resp.Results[0] == nil || resp.Results[0].Error != "" {
		return result, nil
	}
	seriesList := resp.Results[0].Series
	// 用 map 去重 trace_id，并记录最小 start / 最大 end
	type spanWindow struct{ start, end uint64 }
	windows := map[string]spanWindow{}
	for _, series := range seriesList {
		for _, columns := range series.Values {
			var tidHex string
			var start uint64
			var end uint64
			for idx, v := range columns {
				switch series.Columns[idx] {
				case TraceID:
					if v == nil {
						continue
					}
					tidHex = v.(string)
				case StartTimeUnixNano:
					if v != nil {
						start = uint64(v.(float64))
					}
				case EndTimeUnixNano:
					if v != nil {
						end = uint64(v.(float64))
					}
				}
			}
			if tidHex == "" {
				continue
			}
			w := windows[tidHex]
			if w.start == 0 || start < w.start {
				w.start = start
			}
			if end > w.end {
				w.end = end
			}
			windows[tidHex] = w
		}
	}
	for tidHex, w := range windows {
		idBytes, _ := hex.DecodeString(tidHex)
		fi := &jaeger_storage.FoundTraceID{TraceId: idBytes}
		if w.start != 0 {
			fi.Start = timestamppb.New(time.Unix(0, int64(w.start)))
		}
		if w.end != 0 {
			fi.End = timestamppb.New(time.Unix(0, int64(w.end)))
		}
		result.TraceIds = append(result.TraceIds, fi)
	}
	return result, nil
}
