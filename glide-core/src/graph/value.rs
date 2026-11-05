// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Protocol-independent Graph API values. Ordered pairs preserve map/property order.

use std::collections::BTreeMap;

/// A literal parameter supplied separately from the query text.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphParameter {
    /// Cypher null.
    Null,
    /// A boolean.
    Boolean(bool),
    /// A signed, exact 64-bit integer.
    Integer(i64),
    /// A finite double-precision number.
    Double(f64),
    /// A Unicode string.
    String(String),
    /// An ordered list of parameters.
    List(Vec<GraphParameter>),
    /// A deterministically ordered dictionary of parameters.
    Map(BTreeMap<String, GraphParameter>),
}

/// Options for one graph query. A timeout is sent as a command option, not Cypher.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GraphQueryOptions {
    /// Literal parameters, serialized without modifying the query body.
    pub parameters: BTreeMap<String, GraphParameter>,
    /// Optional server-side timeout in milliseconds, at most `i64::MAX`.
    /// The server's configured TIMEOUT_MAX may impose a lower limit.
    pub timeout_ms: Option<u64>,
}

/// A decoded compact graph value, independent of protobuf and language bindings.
///
/// Temporal representations match FalkorDB v6.0.3 `graph/src/runtime/value.rs`
/// and `src/reply.rs`: compact temporal payloads are **integers**, not doubles.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphValue {
    /// Compact tag 1.
    Null,
    /// Compact tag 2: a valid UTF-8 string.
    String(String),
    /// Compact tag 3: an exact signed integer.
    Integer(i64),
    /// Compact tag 4.
    Boolean(bool),
    /// Compact tag 5.
    Double(f64),
    /// Compact tag 6.
    List(Vec<GraphValue>),
    /// Compact tag 7.
    Relationship(GraphRelationship),
    /// Compact tag 8.
    Node(GraphNode),
    /// Compact tag 9.
    Path(GraphPath),
    /// Compact tag 10: ordered entries, including duplicate keys if received.
    Map(Vec<(String, GraphValue)>),
    /// Compact tag 11.
    Point(GraphPoint),
    /// Compact tag 12: native float32 elements.
    Vector(Vec<f32>),
    /// Compact tag 13: Unix timestamp in seconds.
    DateTime(i64),
    /// Compact tag 14: Unix timestamp in seconds at midnight UTC.
    Date(i64),
    /// Compact tag 15: seconds from the epoch, using base date 1970-01-01.
    Time(i64),
    /// Compact tag 16: the server's raw epoch-offset duration encoding.
    ///
    /// This is not an ordinary elapsed number of seconds. It must not be
    /// converted to `std::time::Duration` or interpreted without that encoding.
    Duration(i64),
}

/// A node with dictionary identifiers resolved against an operation's schema.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphNode {
    /// Nonnegative, exact node identifier.
    pub id: i64,
    /// Labels in the order received.
    pub labels: Vec<String>,
    /// Ordered property names and values.
    pub properties: Vec<(String, GraphValue)>,
}

/// A directed relationship with resolved type and property names.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphRelationship {
    /// Nonnegative, exact relationship identifier.
    pub id: i64,
    /// Resolved relationship type name.
    pub r#type: String,
    /// Nonnegative source-node identifier.
    pub source_id: i64,
    /// Nonnegative destination-node identifier.
    pub destination_id: i64,
    /// Ordered property names and values.
    pub properties: Vec<(String, GraphValue)>,
}

/// A traversal's ordered node and relationship sequences.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphPath {
    /// The nodes, with exactly one more entry than `relationships`.
    pub nodes: Vec<GraphNode>,
    /// The relationships in traversal order (possibly traversed in reverse).
    pub relationships: Vec<GraphRelationship>,
}

/// A geographic point in degrees.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphPoint {
    /// Latitude in degrees.
    pub latitude: f64,
    /// Longitude in degrees.
    pub longitude: f64,
}

/// Dictionaries fetched on the same connection for the current graph operation.
///
/// This object is not a cross-query schema cache: `Graph version` statistics
/// describe the graph's write version, not a dictionary/schema version.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GraphSchema {
    /// Label names indexed by compact label ID.
    pub labels: Vec<String>,
    /// Relationship type names indexed by compact relationship type ID.
    pub relationship_types: Vec<String>,
    /// Property names indexed by compact property ID.
    pub property_keys: Vec<String>,
}

/// Execution statistics, preserving all original strings including unknown ones.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GraphStatistics {
    /// Labels added, if reported.
    pub labels_added: Option<u64>,
    /// Labels removed, if reported.
    pub labels_removed: Option<u64>,
    /// Nodes created, if reported.
    pub nodes_created: Option<u64>,
    /// Nodes deleted, if reported.
    pub nodes_deleted: Option<u64>,
    /// Properties set, if reported.
    pub properties_set: Option<u64>,
    /// Properties removed, if reported.
    pub properties_removed: Option<u64>,
    /// Relationships created, if reported.
    pub relationships_created: Option<u64>,
    /// Relationships deleted, if reported.
    pub relationships_deleted: Option<u64>,
    /// Indices created, if reported.
    pub indices_created: Option<u64>,
    /// Indices deleted, if reported.
    pub indices_deleted: Option<u64>,
    /// Whether a cached execution plan was used, if reported.
    pub cached_execution: Option<bool>,
    /// Server-reported internal execution time in milliseconds.
    pub execution_time_ms: Option<f64>,
    /// Graph write version, not a schema version.
    pub graph_version: Option<u64>,
    /// Exact statistic strings, in their original order.
    pub raw: Vec<String>,
}

/// A query result retaining column order, duplicate column names and exact IDs.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GraphQueryResult {
    /// Column names, including duplicate names.
    pub columns: Vec<String>,
    /// Rows in server order; every row has `columns.len()` values.
    pub rows: Vec<Vec<GraphValue>>,
    /// Execution statistics, also present for queries without a RETURN clause.
    pub statistics: GraphStatistics,
}
