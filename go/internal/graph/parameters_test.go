// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package graph

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-glide/go/v2/internal/protobuf"
	"github.com/valkey-io/valkey-glide/go/v2/models"
	"google.golang.org/protobuf/proto"
)

func TestTypedParametersRoundTrip(t *testing.T) {
	zero := int64(0)
	params := map[string]any{
		"null": nil, "bool": true, "min": int64(math.MinInt64), "max": int64(math.MaxInt64),
		"double": 1.25, "unicode": "שלום 世界\x00\n\"\\", "emptyList": []any{}, "emptyMap": map[string]any{},
		"list": []any{int64(9007199254740993), map[string]any{"nested": []string{"x", "y"}}},
	}
	request, err := NewRequest("graph{slot}", "RETURN $max", 3, true, params, &zero)
	if err != nil {
		t.Fatal(err)
	}
	request.DatabaseEpoch = 8
	wire, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded protobuf.QueryRequest
	if err := proto.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(request, &decoded) || decoded.TimeoutMs == nil || *decoded.TimeoutMs != 0 ||
		!decoded.Readonly || decoded.Database != 3 || decoded.DatabaseEpoch != 8 {
		t.Fatalf("request changed during protobuf round trip: %v", &decoded)
	}
	entries := make(map[string]*protobuf.Parameter)
	for _, entry := range decoded.Parameters.Entries {
		entries[entry.Key] = entry.Value
	}
	if entries["min"].GetIntegerValue() != math.MinInt64 || entries["max"].GetIntegerValue() != math.MaxInt64 ||
		entries["list"].GetListValue().Values[0].GetIntegerValue() != 9007199254740993 {
		t.Fatal("integer precision was lost")
	}
	if entries["unicode"].GetStringValue() != params["unicode"] ||
		entries["emptyList"].GetListValue() == nil || entries["emptyMap"].GetMapValue() == nil {
		t.Fatal("string or empty collection changed")
	}
}

func TestParameterRejectionIncludesPath(t *testing.T) {
	mapCycle := map[string]any{}
	mapCycle["self"] = mapCycle
	listCycle := make([]any, 1)
	listCycle[0] = listCycle
	integer := 1
	for _, test := range []struct {
		name  string
		value any
		path  string
	}{
		{"unsigned overflow", uint64(math.MaxUint64), `$["bad"]`},
		{"nan", math.NaN(), `$["bad"]`},
		{"infinity", math.Inf(-1), `$["bad"]`},
		{"date", time.Now(), `$["bad"]`},
		{"temporal wrapper", models.GraphDate(1), `$["bad"]`},
		{"entity", models.GraphNode{ID: 1}, `$["bad"]`},
		{"pointer", &integer, `$["bad"]`},
		{"function", func() {}, `$["bad"]`},
		{"nonstring key", map[int]string{1: "x"}, `$["bad"]`},
		{"invalid utf8", "\xff", `$["bad"]`},
		{"map cycle", mapCycle, `$["bad"]["self"]`},
		{"list cycle", listCycle, `$["bad"][0]`},
		{"nested object", []any{map[string]any{"object": struct{}{}}}, `$["bad"][0]["object"]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewRequest("g", "RETURN 1", 0, false, map[string]any{"bad": test.value}, nil)
			if err == nil || !strings.Contains(err.Error(), test.path) {
				t.Fatalf("expected error at %s, got %v", test.path, err)
			}
		})
	}
}

func TestParameterSharedValuesAndTypedCollections(t *testing.T) {
	child := map[string]any{"value": int16(-2)}
	params := map[string]any{
		"first": child, "second": child, "array": [2]uint32{1, 2}, "float32": float32(0.1),
		"nilList": []any(nil), "nilMap": map[string]any(nil),
	}
	request, err := NewRequest("g", "", 0, false, params, nil)
	if err != nil {
		t.Fatal(err)
	}
	if request.TimeoutMs != nil || len(request.Parameters.Entries) != len(params) {
		t.Fatalf("unexpected request: %v", request)
	}
	for index := 1; index < len(request.Parameters.Entries); index++ {
		if request.Parameters.Entries[index-1].Key >= request.Parameters.Entries[index].Key {
			t.Fatal("parameter map serialization must be deterministic")
		}
	}
	integer := func(value int64) *protobuf.Parameter {
		return &protobuf.Parameter{Value: &protobuf.Parameter_IntegerValue{IntegerValue: value}}
	}
	sharedMap := &protobuf.Parameter{Value: &protobuf.Parameter_MapValue{
		MapValue: &protobuf.ParameterMap{Entries: []*protobuf.ParameterEntry{{Key: "value", Value: integer(-2)}}},
	}}
	want := &protobuf.ParameterMap{Entries: []*protobuf.ParameterEntry{
		{Key: "array", Value: &protobuf.Parameter{Value: &protobuf.Parameter_ListValue{
			ListValue: &protobuf.ParameterList{Values: []*protobuf.Parameter{integer(1), integer(2)}},
		}}},
		{Key: "first", Value: sharedMap},
		{
			Key:   "float32",
			Value: &protobuf.Parameter{Value: &protobuf.Parameter_DoubleValue{DoubleValue: float64(float32(0.1))}},
		},
		{Key: "nilList", Value: &protobuf.Parameter{Value: &protobuf.Parameter_NullValue{NullValue: true}}},
		{Key: "nilMap", Value: &protobuf.Parameter{Value: &protobuf.Parameter_NullValue{NullValue: true}}},
		{Key: "second", Value: sharedMap},
	}}
	if !proto.Equal(request.Parameters, want) {
		t.Fatalf("typed collections changed:\ngot  %v\nwant %v", request.Parameters, want)
	}
	child["value"] = int16(9)
	first, second := request.Parameters.Entries[1].Value, request.Parameters.Entries[5].Value
	if first == second || first.GetMapValue() == second.GetMapValue() ||
		first.GetMapValue().Entries[0].Value.GetIntegerValue() != -2 ||
		second.GetMapValue().Entries[0].Value.GetIntegerValue() != -2 {
		t.Fatal("shared maps must be independently encoded snapshots")
	}
}

func TestParameterOverlappingSlicesAndCycles(t *testing.T) {
	overlap := make([]any, 2)
	overlap[0] = int64(1)
	overlap[1] = overlap[:1]
	request, err := NewRequest("g", "RETURN $list", 0, false, map[string]any{"list": overlap}, nil)
	if err != nil {
		t.Fatalf("acyclic overlapping subslice rejected: %v", err)
	}
	list := request.Parameters.Entries[0].Value.GetListValue().Values
	if len(list) != 2 || list[0].GetIntegerValue() != 1 ||
		len(list[1].GetListValue().Values) != 1 || list[1].GetListValue().Values[0].GetIntegerValue() != 1 {
		t.Fatalf("overlapping slice did not encode as [1, [1]]: %v", request.Parameters)
	}
	overlap[1] = overlap[:0]
	if _, err := NewRequest("g", "", 0, false, map[string]any{"list": overlap}, nil); err != nil {
		t.Fatalf("empty overlapping subslice rejected: %v", err)
	}
	overlap[1] = overlap
	if _, err := NewRequest("g", "", 0, false, map[string]any{"list": overlap}, nil); err == nil ||
		!strings.Contains(err.Error(), `$["list"][1]: cyclic list`) {
		t.Fatalf("true slice cycle must be rejected at its path: %v", err)
	}
	overlap[0] = overlap[:1]
	overlap[1] = nil
	if _, err := NewRequest("g", "", 0, false, map[string]any{"list": overlap}, nil); err == nil ||
		!strings.Contains(err.Error(), `$["list"][0][0]: cyclic list`) {
		t.Fatalf("cycle through a shorter view must still be rejected: %v", err)
	}
}

func TestRequestValidation(t *testing.T) {
	negative := int64(-1)
	for _, test := range []struct {
		name, query string
		timeout     *int64
	}{
		{"", "RETURN 1", nil}, {"\xff", "RETURN 1", nil}, {"g", "\xff", nil}, {"g", "RETURN 1", &negative},
	} {
		if _, err := NewRequest(test.name, test.query, 0, false, nil, test.timeout); err == nil {
			t.Fatalf("expected validation failure for %+v", test)
		}
	}
	var nested any = int64(1)
	for index := 0; index <= maxParameterDepth; index++ {
		nested = []any{nested}
	}
	if _, err := NewRequest("g", "RETURN 1", 0, false, map[string]any{"deep": nested}, nil); err == nil {
		t.Fatal("expected excessive nesting to fail")
	}
	maximum := int64(math.MaxInt64)
	if _, err := NewRequest("g", "RETURN 1", 0, false, nil, &maximum); err != nil {
		t.Fatalf("server-side timeout range must not be restricted locally: %v", err)
	}
}
