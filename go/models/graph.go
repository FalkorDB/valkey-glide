// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

package models

// GraphResult is a fully materialized FalkorDB query result. Column order and
// duplicate column names are preserved. Each row has one value per column.
// Scalars are nil, string, int64, bool, or float64; composite values use []any,
// GraphMap, GraphNode, GraphRelationship, GraphPath, GraphPoint, GraphVector,
// GraphDateTime, GraphDate, GraphTime, and GraphDuration.
type GraphResult struct {
	Columns    []string
	Rows       [][]any
	Statistics GraphStatistics
}

// GraphMapEntry is one ordered entry in a graph map or entity's properties.
type GraphMapEntry struct {
	Key   string
	Value any
}

// GraphMap preserves the ordering and duplicate keys of a Cypher map.
type GraphMap []GraphMapEntry

// GraphNode is a graph entity with a lossless signed 64-bit identifier.
type GraphNode struct {
	ID         int64
	Labels     []string
	Properties GraphMap
}

// GraphRelationship is a directed edge between two node identifiers.
type GraphRelationship struct {
	ID            int64
	Type          string
	SourceID      int64
	DestinationID int64
	Properties    GraphMap
}

// GraphPath preserves the traversal order of nodes and relationships.
type GraphPath struct {
	Nodes         []GraphNode
	Relationships []GraphRelationship
}

// GraphPoint is a geographic point, in degrees.
type GraphPoint struct {
	Latitude  float64
	Longitude float64
}

// GraphVector preserves the server's 32-bit floating-point vector components.
type GraphVector []float32

// GraphDateTime preserves the server's raw epoch-seconds datetime value.
type GraphDateTime int64

// GraphDate preserves the server's raw epoch-seconds date value.
type GraphDate int64

// GraphTime preserves the server's raw seconds-offset time value.
type GraphTime int64

// GraphDuration preserves the server's raw seconds duration value.
type GraphDuration int64

// GraphStatistics retains all raw server statistics, including unknown fields.
// Nil optional fields mean the server omitted that statistic, not zero.
type GraphStatistics struct {
	Raw                  []string
	LabelsAdded          *int64
	LabelsRemoved        *int64
	NodesCreated         *int64
	NodesDeleted         *int64
	PropertiesSet        *int64
	PropertiesRemoved    *int64
	RelationshipsCreated *int64
	RelationshipsDeleted *int64
	IndicesCreated       *int64
	IndicesDeleted       *int64
	CachedExecution      *bool
	ExecutionTimeMs      *float64
	GraphVersion         *uint64
}
