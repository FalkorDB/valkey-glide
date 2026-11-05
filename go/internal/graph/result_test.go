// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package graph

import (
	"math"
	"reflect"
	"testing"

	"github.com/valkey-io/valkey-glide/go/v2/models"
)

func envelope(kind string, value any) map[string]any {
	return map[string]any{"kind": kind, "value": value}
}

func resultWithCells(cells ...any) map[string]any {
	columns := make([]any, len(cells))
	for index := range columns {
		columns[index] = "duplicate"
	}
	return map[string]any{
		"columns": columns, "rows": []any{cells}, "statistics": map[string]any{"raw": []any{}},
	}
}

func TestMaterializeAllGraphValueKinds(t *testing.T) {
	properties := []any{[]any{"same", envelope("integer", int64(math.MaxInt64))}, []any{"same", envelope("null", nil)}}
	node := envelope(
		"node",
		map[string]any{"id": int64(math.MaxInt64), "labels": []any{"人", "Person"}, "properties": properties},
	)
	relationship := envelope("relationship", map[string]any{
		"id": int64(4), "type": "KNOWS", "source_id": int64(math.MaxInt64), "destination_id": int64(2), "properties": nil,
	})
	tests := []struct {
		kind  string
		value any
		want  any
	}{
		{"null", nil, nil},
		{"string", "世界\x00", "世界\x00"},
		{"integer", int64(math.MinInt64), int64(math.MinInt64)},
		{"boolean", false, false},
		{"double", 0.125, 0.125},
		{"list", []any{envelope("integer", int64(1)), envelope("null", nil)}, []any{int64(1), nil}},
		{"map", properties, models.GraphMap{{Key: "same", Value: int64(math.MaxInt64)}, {Key: "same", Value: nil}}},
		{"node", node["value"], models.GraphNode{
			ID: math.MaxInt64, Labels: []string{"人", "Person"},
			Properties: models.GraphMap{{Key: "same", Value: int64(math.MaxInt64)}, {Key: "same", Value: nil}},
		}},
		{"relationship", relationship["value"], models.GraphRelationship{
			ID: 4, Type: "KNOWS", SourceID: math.MaxInt64, DestinationID: 2, Properties: models.GraphMap{},
		}},
		{"path", map[string]any{"nodes": []any{node}, "relationships": []any{relationship}}, models.GraphPath{
			Nodes: []models.GraphNode{{
				ID: math.MaxInt64, Labels: []string{"人", "Person"},
				Properties: models.GraphMap{{Key: "same", Value: int64(math.MaxInt64)}, {Key: "same", Value: nil}},
			}},
			Relationships: []models.GraphRelationship{{
				ID: 4, Type: "KNOWS", SourceID: math.MaxInt64, DestinationID: 2, Properties: models.GraphMap{},
			}},
		}},
		{"point", map[string]any{"latitude": 51.5, "longitude": -0.125}, models.GraphPoint{Latitude: 51.5, Longitude: -0.125}},
		{"vector", []any{float64(float32(0.1)), float64(float32(-2))}, models.GraphVector{0.1, -2}},
		{"datetime", int64(math.MaxInt64), models.GraphDateTime(math.MaxInt64)},
		{"date", int64(-86400), models.GraphDate(-86400)},
		{"time", int64(12345), models.GraphTime(12345)},
		{"duration", int64(math.MinInt64), models.GraphDuration(math.MinInt64)},
	}
	cells := make([]any, len(tests))
	for index, test := range tests {
		cells[index] = envelope(test.kind, test.value)
	}
	result, err := DecodeResult(resultWithCells(cells...))
	if err != nil {
		t.Fatal(err)
	}
	for index, test := range tests {
		if !reflect.DeepEqual(result.Rows[0][index], test.want) {
			t.Errorf("%s: got %#v, want %#v", test.kind, result.Rows[0][index], test.want)
		}
		if result.Columns[index] != "duplicate" {
			t.Fatal("duplicate column names changed")
		}
	}
}

func TestGraphStatisticsPreserveOptionalAndRaw(t *testing.T) {
	canonical := resultWithCells()
	statistics := map[string]any{
		"raw": []any{"Future statistic: x", "Nodes created: 0"}, "labels_added": int64(0), "labels_removed": int64(1),
		"nodes_created": int64(2), "nodes_deleted": int64(3), "properties_set": int64(4), "properties_removed": int64(5),
		"relationships_created": int64(6), "relationships_deleted": int64(7), "indices_created": int64(8),
		"indices_deleted": int64(
			9,
		), "cached_execution": false, "execution_time_ms": 0.0, "graph_version": "18446744073709551615",
	}
	canonical["statistics"] = statistics
	result, err := DecodeResult(canonical)
	if err != nil {
		t.Fatal(err)
	}
	stats := result.Statistics
	if stats.LabelsAdded == nil || *stats.LabelsAdded != 0 || stats.CachedExecution == nil || *stats.CachedExecution ||
		stats.ExecutionTimeMs == nil || *stats.ExecutionTimeMs != 0 || stats.GraphVersion == nil ||
		*stats.GraphVersion != math.MaxUint64 || len(stats.Raw) != 2 || *stats.IndicesDeleted != 9 {
		t.Fatalf("statistics lost data: %+v", stats)
	}
	minimal, err := DecodeResult(resultWithCells())
	if err != nil || minimal.Statistics.LabelsAdded != nil || minimal.Statistics.CachedExecution != nil {
		t.Fatalf("omitted statistics must remain absent: %+v %v", minimal, err)
	}
	statistics["graph_version"] = int64(math.MaxInt64)
	if _, err := DecodeResult(canonical); err != nil {
		t.Fatal(err)
	}
}

func TestMalformedCanonicalGraphResults(t *testing.T) {
	invalidCells := []any{
		nil,
		[]any{int64(1)},
		envelope("integer", 1.0), envelope("string", int64(1)), envelope("boolean", int64(0)),
		envelope("double", int64(1)), envelope("null", "x"), envelope("unknown", nil),
		map[string]any{"kind": "null"},
		envelope("list", "x"), envelope("date", "date"),
		envelope("map", []any{[]any{"key"}}), envelope("map", []any{[]any{int64(1), envelope("null", nil)}}),
		envelope("node", map[string]any{"id": "1"}), envelope("relationship", map[string]any{"id": int64(1)}),
		envelope("path", map[string]any{"nodes": []any{envelope("null", nil)}, "relationships": nil}),
		envelope("path", map[string]any{"nodes": nil, "relationships": []any{envelope("null", nil)}}),
		envelope("point", map[string]any{"latitude": "1", "longitude": 0.0}),
		envelope("vector", []any{0.1}), envelope("vector", []any{"1"}),
	}
	for index, cell := range invalidCells {
		if _, err := DecodeResult(resultWithCells(cell)); err == nil {
			t.Errorf("invalid cell %d accepted: %#v", index, cell)
		}
	}
	for _, invalid := range []any{
		nil,
		map[string]any{},
		map[string]any{"columns": nil, "rows": nil, "statistics": nil},
		map[string]any{"columns": []any{"a"}, "rows": []any{[]any{}}, "statistics": map[string]any{"raw": nil}},
	} {
		if _, err := DecodeResult(invalid); err == nil {
			t.Errorf("invalid result accepted: %#v", invalid)
		}
	}
	for _, statistics := range []any{
		nil,
		map[string]any{},
		map[string]any{"raw": nil, "labels_added": "1"},
		map[string]any{"raw": nil, "cached_execution": int64(1)},
		map[string]any{"raw": nil, "execution_time_ms": "0"},
		map[string]any{"raw": nil, "graph_version": int64(-1)},
		map[string]any{"raw": nil, "graph_version": "18446744073709551616"},
	} {
		canonical := resultWithCells()
		canonical["statistics"] = statistics
		if _, err := DecodeResult(canonical); err == nil {
			t.Errorf("invalid statistics accepted: %#v", statistics)
		}
	}
}

func TestEmptyCanonicalGraphResult(t *testing.T) {
	result, err := DecodeResult(map[string]any{"columns": nil, "rows": nil, "statistics": map[string]any{"raw": nil}})
	if err != nil || result.Columns == nil || result.Rows == nil || result.Statistics.Raw == nil {
		t.Fatalf("empty collections must be materialized: %+v %v", result, err)
	}
}
