// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

// Package graph implements the typed graph transport, not FalkorDB wire decoding.
package graph

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"unicode/utf8"

	"github.com/valkey-io/valkey-glide/go/v2/internal/protobuf"
)

const maxParameterDepth = 100

type visit struct {
	typ    reflect.Type
	ptr    uintptr
	length int
}

// NewRequest validates typed parameters without converting integers through JSON.
func NewRequest(
	name, query string,
	database uint32,
	readonly bool,
	params map[string]any,
	timeout *int64,
) (*protobuf.QueryRequest, error) {
	if name == "" || !utf8.ValidString(name) {
		return nil, fmt.Errorf("graph name must be a nonempty UTF-8 string")
	}
	if !utf8.ValidString(query) {
		return nil, fmt.Errorf("graph query must be a UTF-8 string")
	}
	if timeout != nil && *timeout < 0 {
		return nil, fmt.Errorf("graph timeout must be nonnegative")
	}
	value, err := encodeParameter(reflect.ValueOf(params), "$", make(map[visit]bool), 0)
	if err != nil {
		return nil, err
	}
	request := &protobuf.QueryRequest{
		GraphName:  name,
		Query:      query,
		Database:   database,
		Readonly:   readonly,
		Parameters: value.GetMapValue(),
	}
	if timeout != nil {
		milliseconds := uint64(*timeout)
		request.TimeoutMs = &milliseconds
	}
	return request, nil
}

func parameterError(path, message string) error {
	return fmt.Errorf("graph parameter %s: %s", path, message)
}

func encodeParameter(value reflect.Value, path string, active map[visit]bool, depth int) (*protobuf.Parameter, error) {
	if depth > maxParameterDepth {
		return nil, parameterError(path, "maximum nesting depth exceeded")
	}
	if !value.IsValid() {
		return &protobuf.Parameter{Value: &protobuf.Parameter_NullValue{NullValue: true}}, nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return encodeParameter(reflect.Value{}, path, active, depth)
		}
		return encodeParameter(value.Elem(), path, active, depth)
	}
	if value.Type().PkgPath() != "" {
		return nil, parameterError(path, fmt.Sprintf("unsupported type %s", value.Type()))
	}
	result := &protobuf.Parameter{}
	switch value.Kind() {
	case reflect.Bool:
		result.Value = &protobuf.Parameter_BooleanValue{BooleanValue: value.Bool()}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		result.Value = &protobuf.Parameter_IntegerValue{IntegerValue: value.Int()}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if value.Uint() > math.MaxInt64 {
			return nil, parameterError(path, "integer overflows signed 64-bit range")
		}
		result.Value = &protobuf.Parameter_IntegerValue{IntegerValue: int64(value.Uint())}
	case reflect.Float32, reflect.Float64:
		number := value.Float()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return nil, parameterError(path, "floating-point value must be finite")
		}
		result.Value = &protobuf.Parameter_DoubleValue{DoubleValue: number}
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return nil, parameterError(path, "string is not valid UTF-8")
		}
		result.Value = &protobuf.Parameter_StringValue{StringValue: value.String()}
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return nil, parameterError(path, "map keys must be strings")
		}
		if value.IsNil() {
			return encodeParameter(reflect.Value{}, path, active, depth)
		}
		key := visit{typ: value.Type(), ptr: value.Pointer()}
		if active[key] {
			return nil, parameterError(path, "cyclic map")
		}
		active[key] = true
		defer delete(active, key)
		keys := value.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
		entries := make([]*protobuf.ParameterEntry, 0, len(keys))
		for _, mapKey := range keys {
			childPath := path + "[" + strconv.Quote(mapKey.String()) + "]"
			if !utf8.ValidString(mapKey.String()) {
				return nil, parameterError(childPath, "map key is not valid UTF-8")
			}
			child, err := encodeParameter(value.MapIndex(mapKey), childPath, active, depth+1)
			if err != nil {
				return nil, err
			}
			entries = append(entries, &protobuf.ParameterEntry{Key: mapKey.String(), Value: child})
		}
		result.Value = &protobuf.Parameter_MapValue{MapValue: &protobuf.ParameterMap{Entries: entries}}
	case reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice {
			if value.IsNil() {
				return encodeParameter(reflect.Value{}, path, active, depth)
			}
			key := visit{typ: value.Type(), ptr: value.Pointer(), length: value.Len()}
			if active[key] {
				return nil, parameterError(path, "cyclic list")
			}
			active[key] = true
			defer delete(active, key)
		}
		values := make([]*protobuf.Parameter, value.Len())
		for index := 0; index < value.Len(); index++ {
			child, err := encodeParameter(value.Index(index), fmt.Sprintf("%s[%d]", path, index), active, depth+1)
			if err != nil {
				return nil, err
			}
			values[index] = child
		}
		result.Value = &protobuf.Parameter_ListValue{ListValue: &protobuf.ParameterList{Values: values}}
	default:
		return nil, parameterError(path, fmt.Sprintf("unsupported type %s", value.Type()))
	}
	return result, nil
}
