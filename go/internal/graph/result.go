// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package graph

import (
	"fmt"
	"math"
	"strconv"

	"github.com/valkey-io/valkey-glide/go/v2/models"
)

func resultError(path, expected string) error {
	return fmt.Errorf("invalid canonical graph result at %s: expected %s", path, expected)
}

func asMap(value any, path string) (map[string]any, error) {
	result, ok := value.(map[string]any)
	if !ok {
		return nil, resultError(path, "named map")
	}
	return result, nil
}

func asArray(value any, path string) ([]any, error) {
	if value == nil {
		// The existing FFI converter represents an empty array as nil.
		return []any{}, nil
	}
	result, ok := value.([]any)
	if !ok {
		return nil, resultError(path, "array")
	}
	return result, nil
}

func asString(value any, path string) (string, error) {
	result, ok := value.(string)
	if !ok {
		return "", resultError(path, "string")
	}
	return result, nil
}

func asInt(value any, path string) (int64, error) {
	result, ok := value.(int64)
	if !ok {
		return 0, resultError(path, "signed 64-bit integer")
	}
	return result, nil
}

func asDouble(value any, path string) (float64, error) {
	result, ok := value.(float64)
	if !ok {
		return 0, resultError(path, "double")
	}
	return result, nil
}

func stringArray(value any, path string) ([]string, error) {
	array, err := asArray(value, path)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(array))
	for index, value := range array {
		result[index], err = asString(value, fmt.Sprintf("%s[%d]", path, index))
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// DecodeResult only materializes the core's named, typed canonical result.
// It never interprets raw FalkorDB compact response arrays or schema IDs.
func DecodeResult(value any) (*models.GraphResult, error) {
	top, err := asMap(value, "$")
	if err != nil {
		return nil, err
	}
	for _, field := range []string{"columns", "rows", "statistics"} {
		if _, ok := top[field]; !ok {
			return nil, resultError("$."+field, "required field")
		}
	}
	columns, err := stringArray(top["columns"], "$.columns")
	if err != nil {
		return nil, err
	}
	rows, err := asArray(top["rows"], "$.rows")
	if err != nil {
		return nil, err
	}
	result := &models.GraphResult{Columns: columns, Rows: make([][]any, len(rows))}
	for index, value := range rows {
		path := fmt.Sprintf("$.rows[%d]", index)
		row, err := asArray(value, path)
		if err != nil {
			return nil, err
		}
		if len(row) != len(columns) {
			return nil, resultError(path, "one value per column")
		}
		result.Rows[index] = make([]any, len(row))
		for column, cell := range row {
			result.Rows[index][column], err = decodeValue(cell, fmt.Sprintf("%s[%d]", path, column), 0)
			if err != nil {
				return nil, err
			}
		}
	}
	result.Statistics, err = decodeStatistics(top["statistics"])
	if err != nil {
		return nil, err
	}
	return result, nil
}

func decodeValue(value any, path string, depth int) (any, error) {
	if depth > maxParameterDepth {
		return nil, resultError(path, "bounded nesting depth")
	}
	envelope, err := asMap(value, path)
	if err != nil {
		return nil, err
	}
	kind, err := asString(envelope["kind"], path+".kind")
	if err != nil {
		return nil, err
	}
	payload, exists := envelope["value"]
	if !exists {
		return nil, resultError(path+".value", "required field")
	}
	path += ".value"
	switch kind {
	case "null":
		if payload != nil {
			return nil, resultError(path, "null")
		}
		return nil, nil
	case "string":
		return asString(payload, path)
	case "integer":
		return asInt(payload, path)
	case "boolean":
		result, ok := payload.(bool)
		if !ok {
			return nil, resultError(path, "boolean")
		}
		return result, nil
	case "double":
		return asDouble(payload, path)
	case "datetime", "date", "time", "duration":
		number, err := asInt(payload, path)
		if err != nil {
			return nil, err
		}
		switch kind {
		case "datetime":
			return models.GraphDateTime(number), nil
		case "date":
			return models.GraphDate(number), nil
		case "time":
			return models.GraphTime(number), nil
		default:
			return models.GraphDuration(number), nil
		}
	case "list":
		array, err := asArray(payload, path)
		if err != nil {
			return nil, err
		}
		result := make([]any, len(array))
		for index, value := range array {
			result[index], err = decodeValue(value, fmt.Sprintf("%s[%d]", path, index), depth+1)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	case "map":
		return decodeProperties(payload, path, depth+1)
	case "node":
		return decodeNode(payload, path, depth+1)
	case "relationship":
		return decodeRelationship(payload, path, depth+1)
	case "path":
		return decodePath(payload, path, depth+1)
	case "point":
		point, err := asMap(payload, path)
		if err != nil {
			return nil, err
		}
		latitude, err := asDouble(point["latitude"], path+".latitude")
		if err != nil {
			return nil, err
		}
		longitude, err := asDouble(point["longitude"], path+".longitude")
		return models.GraphPoint{Latitude: latitude, Longitude: longitude}, err
	case "vector":
		array, err := asArray(payload, path)
		if err != nil {
			return nil, err
		}
		result := make(models.GraphVector, len(array))
		for index, value := range array {
			number, err := asDouble(value, fmt.Sprintf("%s[%d]", path, index))
			if err != nil {
				return nil, err
			}
			converted := float32(number)
			if float64(converted) != number && !math.IsNaN(number) {
				return nil, resultError(path, "lossless 32-bit vector components")
			}
			result[index] = converted
		}
		return result, nil
	default:
		return nil, resultError(path, "known graph value kind, got "+strconv.Quote(kind))
	}
}

func decodeProperties(value any, path string, depth int) (models.GraphMap, error) {
	array, err := asArray(value, path)
	if err != nil {
		return nil, err
	}
	result := make(models.GraphMap, len(array))
	for index, value := range array {
		entryPath := fmt.Sprintf("%s[%d]", path, index)
		pair, err := asArray(value, entryPath)
		if err != nil {
			return nil, err
		}
		if len(pair) != 2 {
			return nil, resultError(entryPath, "key/value pair")
		}
		key, err := asString(pair[0], entryPath+"[0]")
		if err != nil {
			return nil, err
		}
		decoded, err := decodeValue(pair[1], entryPath+"[1]", depth)
		if err != nil {
			return nil, err
		}
		result[index] = models.GraphMapEntry{Key: key, Value: decoded}
	}
	return result, nil
}

func decodeNode(value any, path string, depth int) (models.GraphNode, error) {
	node, err := asMap(value, path)
	if err != nil {
		return models.GraphNode{}, err
	}
	id, err := asInt(node["id"], path+".id")
	if err != nil {
		return models.GraphNode{}, err
	}
	labels, err := stringArray(node["labels"], path+".labels")
	if err != nil {
		return models.GraphNode{}, err
	}
	properties, err := decodeProperties(node["properties"], path+".properties", depth)
	return models.GraphNode{ID: id, Labels: labels, Properties: properties}, err
}

func decodeRelationship(value any, path string, depth int) (models.GraphRelationship, error) {
	relationship, err := asMap(value, path)
	if err != nil {
		return models.GraphRelationship{}, err
	}
	id, err := asInt(relationship["id"], path+".id")
	if err != nil {
		return models.GraphRelationship{}, err
	}
	name, err := asString(relationship["type"], path+".type")
	if err != nil {
		return models.GraphRelationship{}, err
	}
	source, err := asInt(relationship["source_id"], path+".source_id")
	if err != nil {
		return models.GraphRelationship{}, err
	}
	destination, err := asInt(relationship["destination_id"], path+".destination_id")
	if err != nil {
		return models.GraphRelationship{}, err
	}
	properties, err := decodeProperties(relationship["properties"], path+".properties", depth)
	return models.GraphRelationship{
		ID: id, Type: name, SourceID: source, DestinationID: destination, Properties: properties,
	}, err
}

func decodePath(value any, path string, depth int) (models.GraphPath, error) {
	fields, err := asMap(value, path)
	if err != nil {
		return models.GraphPath{}, err
	}
	nodes, err := asArray(fields["nodes"], path+".nodes")
	if err != nil {
		return models.GraphPath{}, err
	}
	relationships, err := asArray(fields["relationships"], path+".relationships")
	if err != nil {
		return models.GraphPath{}, err
	}
	result := models.GraphPath{
		Nodes: make([]models.GraphNode, len(nodes)), Relationships: make([]models.GraphRelationship, len(relationships)),
	}
	for index, value := range nodes {
		itemPath := fmt.Sprintf("%s.nodes[%d]", path, index)
		decoded, err := decodeValue(value, itemPath, depth)
		if err != nil {
			return models.GraphPath{}, err
		}
		node, ok := decoded.(models.GraphNode)
		if !ok {
			return models.GraphPath{}, resultError(itemPath, "node")
		}
		result.Nodes[index] = node
	}
	for index, value := range relationships {
		itemPath := fmt.Sprintf("%s.relationships[%d]", path, index)
		decoded, err := decodeValue(value, itemPath, depth)
		if err != nil {
			return models.GraphPath{}, err
		}
		relationship, ok := decoded.(models.GraphRelationship)
		if !ok {
			return models.GraphPath{}, resultError(itemPath, "relationship")
		}
		result.Relationships[index] = relationship
	}
	return result, nil
}

func decodeStatistics(value any) (models.GraphStatistics, error) {
	fields, err := asMap(value, "$.statistics")
	if err != nil {
		return models.GraphStatistics{}, err
	}
	if _, ok := fields["raw"]; !ok {
		return models.GraphStatistics{}, resultError("$.statistics.raw", "required field")
	}
	raw, err := stringArray(fields["raw"], "$.statistics.raw")
	if err != nil {
		return models.GraphStatistics{}, err
	}
	result := models.GraphStatistics{Raw: raw}
	counters := []struct {
		name   string
		target **int64
	}{
		{"labels_added", &result.LabelsAdded},
		{"labels_removed", &result.LabelsRemoved},
		{"nodes_created", &result.NodesCreated},
		{"nodes_deleted", &result.NodesDeleted},
		{"properties_set", &result.PropertiesSet},
		{"properties_removed", &result.PropertiesRemoved},
		{"relationships_created", &result.RelationshipsCreated},
		{"relationships_deleted", &result.RelationshipsDeleted},
		{"indices_created", &result.IndicesCreated},
		{"indices_deleted", &result.IndicesDeleted},
	}
	for _, counter := range counters {
		if value, ok := fields[counter.name]; ok {
			number, err := asInt(value, "$.statistics."+counter.name)
			if err != nil {
				return models.GraphStatistics{}, err
			}
			*counter.target = &number
		}
	}
	if value, ok := fields["cached_execution"]; ok {
		flag, ok := value.(bool)
		if !ok {
			return models.GraphStatistics{}, resultError("$.statistics.cached_execution", "boolean")
		}
		result.CachedExecution = &flag
	}
	if value, ok := fields["execution_time_ms"]; ok {
		number, err := asDouble(value, "$.statistics.execution_time_ms")
		if err != nil {
			return models.GraphStatistics{}, err
		}
		result.ExecutionTimeMs = &number
	}
	if value, ok := fields["graph_version"]; ok {
		var version uint64
		switch value := value.(type) {
		case int64:
			if value < 0 {
				return models.GraphStatistics{}, resultError("$.statistics.graph_version", "unsigned 64-bit integer")
			}
			version = uint64(value)
		case string:
			version, err = strconv.ParseUint(value, 10, 64)
			if err != nil {
				return models.GraphStatistics{}, resultError("$.statistics.graph_version", "unsigned 64-bit integer")
			}
		default:
			return models.GraphStatistics{}, resultError("$.statistics.graph_version", "unsigned 64-bit integer")
		}
		result.GraphVersion = &version
	}
	return result, nil
}
