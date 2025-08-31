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

package utils

import (
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"

	otlpcommonv1 "go.opentelemetry.io/proto/otlp/common/v1"
)

// GetAnyValue convert OTLP AttributeValue to raw value
func GetAnyValue(value *otlpcommonv1.AnyValue) any {
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
		// array to json
		if jsonBytes, err := json.Marshal(v.ArrayValue); err == nil {
			return string(jsonBytes)
		}
		return "[]"
	case *otlpcommonv1.AnyValue_KvlistValue:
		// kv list to json
		if jsonBytes, err := json.Marshal(v.KvlistValue); err == nil {
			return string(jsonBytes)
		}
		return "{}"
	default:
		return ""
	}
}

// GetAnyValueString convert OTLP AttributeValue to string
func GetAnyValueString(value *otlpcommonv1.AnyValue) string {
	anyValue := GetAnyValue(value)
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

func MarshalAnything(data interface{}) string {
	content, err := json.Marshal(data)
	if err != nil {
		return "marshal failed: " + err.Error()
	}
	return string(content)
}

func SlogLevel(level string) slog.Level {
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
