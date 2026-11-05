// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Lossless binding transport using named Redis maps, never JSON.
//!
//! The result is `{columns: [string], rows: [[envelope]], statistics: {...}}`.
//! Every graph value is `{kind: string, value: payload}`, including scalars.
//! Kinds are `null`, `string`, `integer`, `boolean`, `double`, `list`, `map`,
//! `node`, `relationship`, `path`, `point`, `vector`, `datetime`, `date`, `time`,
//! and `duration`. Lists contain envelopes; graph maps and entity properties
//! contain ordered `[key, envelope]` pairs, preserving duplicate keys.
//!
//! Node payloads are `{id, labels, properties}`; relationships are
//! `{id, type, source_id, destination_id, properties}`. Paths have
//! `{nodes: [node envelope], relationships: [relationship envelope]}`. Points
//! have `{latitude, longitude}`; vectors contain exactly widened float32
//! elements. All temporal payloads are raw signed integers (duration uses the
//! server's offset encoding). Integer/ID/temporal values never pass through
//! floating-point numbers. Statistics omit absent known fields and always
//! include `raw`, the original ordered statistic strings.

use redis::{ErrorKind, RedisError, RedisResult, Value};

use super::value::*;

const MAX_DEPTH: usize = 128;

fn invalid(detail: &str) -> RedisError {
    (
        ErrorKind::TypeError,
        "Invalid graph result for binding transport",
        detail.to_owned(),
    )
        .into()
}

fn string(value: String) -> Value {
    Value::BulkString(value.into())
}

fn map(fields: Vec<(&str, Value)>) -> Value {
    Value::Map(
        fields
            .into_iter()
            .map(|(key, value)| (string(key.to_owned()), value))
            .collect(),
    )
}

fn envelope(kind: &str, value: Value) -> Value {
    map(vec![("kind", string(kind.to_owned())), ("value", value)])
}

fn identifier(value: i64) -> RedisResult<Value> {
    if value < 0 {
        return Err(invalid("Entity IDs must be nonnegative"));
    }
    Ok(Value::Int(value))
}

fn entries(values: Vec<(String, GraphValue)>, depth: usize) -> RedisResult<Value> {
    Ok(Value::Array(
        values
            .into_iter()
            .map(|(key, value)| {
                Ok(Value::Array(vec![
                    string(key),
                    encode_value(value, depth + 1)?,
                ]))
            })
            .collect::<RedisResult<_>>()?,
    ))
}

fn node(value: GraphNode, depth: usize) -> RedisResult<Value> {
    if depth > MAX_DEPTH {
        return Err(invalid("Graph value nesting exceeds 128 levels"));
    }
    Ok(envelope(
        "node",
        map(vec![
            ("id", identifier(value.id)?),
            (
                "labels",
                Value::Array(value.labels.into_iter().map(string).collect()),
            ),
            ("properties", entries(value.properties, depth)?),
        ]),
    ))
}

fn relationship(value: GraphRelationship, depth: usize) -> RedisResult<Value> {
    if depth > MAX_DEPTH {
        return Err(invalid("Graph value nesting exceeds 128 levels"));
    }
    Ok(envelope(
        "relationship",
        map(vec![
            ("id", identifier(value.id)?),
            ("type", string(value.r#type)),
            ("source_id", identifier(value.source_id)?),
            ("destination_id", identifier(value.destination_id)?),
            ("properties", entries(value.properties, depth)?),
        ]),
    ))
}

fn path(value: GraphPath, depth: usize) -> RedisResult<Value> {
    if value.nodes.len().checked_sub(1) != Some(value.relationships.len()) {
        return Err(invalid("A path must have one more node than relationships"));
    }
    for (pair, relationship) in value.nodes.windows(2).zip(&value.relationships) {
        if !((pair[0].id == relationship.source_id && pair[1].id == relationship.destination_id)
            || (pair[1].id == relationship.source_id && pair[0].id == relationship.destination_id))
        {
            return Err(invalid(
                "Path relationship endpoints do not match its adjacent nodes",
            ));
        }
    }
    Ok(map(vec![
        (
            "nodes",
            Value::Array(
                value
                    .nodes
                    .into_iter()
                    .map(|value| node(value, depth + 1))
                    .collect::<RedisResult<_>>()?,
            ),
        ),
        (
            "relationships",
            Value::Array(
                value
                    .relationships
                    .into_iter()
                    .map(|value| relationship(value, depth + 1))
                    .collect::<RedisResult<_>>()?,
            ),
        ),
    ]))
}

fn encode_value(value: GraphValue, depth: usize) -> RedisResult<Value> {
    if depth > MAX_DEPTH {
        return Err(invalid("Graph value nesting exceeds 128 levels"));
    }
    let (kind, payload) = match value {
        GraphValue::Null => ("null", Value::Nil),
        GraphValue::String(value) => ("string", string(value)),
        GraphValue::Integer(value) => ("integer", Value::Int(value)),
        GraphValue::Boolean(value) => ("boolean", Value::Boolean(value)),
        GraphValue::Double(value) => ("double", Value::Double(value)),
        GraphValue::List(values) => (
            "list",
            Value::Array(
                values
                    .into_iter()
                    .map(|value| encode_value(value, depth + 1))
                    .collect::<RedisResult<_>>()?,
            ),
        ),
        GraphValue::Map(values) => ("map", entries(values, depth)?),
        GraphValue::Node(value) => return node(value, depth),
        GraphValue::Relationship(value) => return relationship(value, depth),
        GraphValue::Path(value) => ("path", path(value, depth)?),
        GraphValue::Point(value) => (
            "point",
            map(vec![
                ("latitude", Value::Double(value.latitude)),
                ("longitude", Value::Double(value.longitude)),
            ]),
        ),
        GraphValue::Vector(value) => (
            "vector",
            Value::Array(
                value
                    .into_iter()
                    .map(|value| Value::Double(f64::from(value)))
                    .collect(),
            ),
        ),
        GraphValue::DateTime(value) => ("datetime", Value::Int(value)),
        GraphValue::Date(value) => ("date", Value::Int(value)),
        GraphValue::Time(value) => ("time", Value::Int(value)),
        GraphValue::Duration(value) => ("duration", Value::Int(value)),
    };
    Ok(envelope(kind, payload))
}

fn statistics(value: GraphStatistics) -> RedisResult<Value> {
    let mut fields = vec![(
        "raw",
        Value::Array(value.raw.into_iter().map(string).collect()),
    )];
    for (name, value) in [
        ("labels_added", value.labels_added),
        ("labels_removed", value.labels_removed),
        ("nodes_created", value.nodes_created),
        ("nodes_deleted", value.nodes_deleted),
        ("properties_set", value.properties_set),
        ("properties_removed", value.properties_removed),
        ("relationships_created", value.relationships_created),
        ("relationships_deleted", value.relationships_deleted),
        ("indices_created", value.indices_created),
        ("indices_deleted", value.indices_deleted),
        ("graph_version", value.graph_version),
    ] {
        if let Some(value) = value {
            fields.push((
                name,
                match i64::try_from(value) {
                    Ok(value) => Value::Int(value),
                    Err(_) => Value::BigNumber(value.into()),
                },
            ));
        }
    }
    if let Some(value) = value.cached_execution {
        fields.push(("cached_execution", Value::Boolean(value)));
    }
    if let Some(value) = value.execution_time_ms {
        if !value.is_finite() || value < 0.0 {
            return Err(invalid("Execution time must be finite and nonnegative"));
        }
        fields.push(("execution_time_ms", Value::Double(value)));
    }
    Ok(map(fields))
}

/// Consume a decoded result into the canonical named, typed binding envelope.
///
/// Row-width mismatches, invalid IDs/paths and excessive recursion fail before
/// any result is returned. Exact integers remain Redis integers, while counts
/// beyond signed 64-bit range use Redis big numbers rather than rounded doubles.
pub fn encode_result(value: GraphQueryResult) -> RedisResult<Value> {
    let width = value.columns.len();
    let rows = value
        .rows
        .into_iter()
        .map(|row| {
            if row.len() != width {
                return Err(invalid("Row width must match the number of columns"));
            }
            Ok(Value::Array(
                row.into_iter()
                    .map(|value| encode_value(value, 0))
                    .collect::<RedisResult<_>>()?,
            ))
        })
        .collect::<RedisResult<_>>()?;
    Ok(map(vec![
        (
            "columns",
            Value::Array(value.columns.into_iter().map(string).collect()),
        ),
        ("rows", Value::Array(rows)),
        ("statistics", statistics(value.statistics)?),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
        let Value::Map(fields) = value else {
            panic!("Expected a named map")
        };
        fields
            .iter()
            .find_map(|(key, value)| match key {
                Value::BulkString(key) if key.as_ref() == name.as_bytes() => Some(value),
                _ => None,
            })
            .unwrap_or_else(|| panic!("Missing field {name}"))
    }

    fn n(id: i64) -> GraphNode {
        GraphNode {
            id,
            labels: vec!["Person".into()],
            properties: vec![("id".into(), GraphValue::Integer(i64::MAX))],
        }
    }

    fn r() -> GraphRelationship {
        GraphRelationship {
            id: i64::MAX,
            r#type: "KNOWS".into(),
            source_id: 1,
            destination_id: 2,
            properties: vec![],
        }
    }

    #[test]
    fn preserves_every_kind_and_named_special_payload() {
        let values = vec![
            GraphValue::Null,
            GraphValue::String("界".into()),
            GraphValue::Integer(i64::MIN),
            GraphValue::Boolean(true),
            GraphValue::Double(-0.0),
            GraphValue::List(vec![GraphValue::Integer(i64::MAX)]),
            GraphValue::Relationship(r()),
            GraphValue::Node(n(i64::MAX)),
            GraphValue::Path(GraphPath {
                nodes: vec![n(1), n(2)],
                relationships: vec![r()],
            }),
            GraphValue::Map(vec![
                ("k".into(), GraphValue::Null),
                ("k".into(), GraphValue::Integer(1)),
            ]),
            GraphValue::Point(GraphPoint {
                latitude: 1.25,
                longitude: 2.5,
            }),
            GraphValue::Vector(vec![0.1_f32, -0.0]),
            GraphValue::DateTime(i64::MAX),
            GraphValue::Date(i64::MIN),
            GraphValue::Time(11045),
            GraphValue::Duration(36907506),
        ];
        let encoded = encode_result(GraphQueryResult {
            columns: vec!["x".into(); values.len()],
            rows: vec![values],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            field(&encoded, "columns"),
            &Value::Array(vec![string("x".into()); 16])
        );
        let Value::Array(rows) = field(&encoded, "rows") else {
            panic!("Expected rows")
        };
        let Value::Array(row) = &rows[0] else {
            panic!("Expected row")
        };
        for (value, kind) in row.iter().zip([
            "null",
            "string",
            "integer",
            "boolean",
            "double",
            "list",
            "relationship",
            "node",
            "path",
            "map",
            "point",
            "vector",
            "datetime",
            "date",
            "time",
            "duration",
        ]) {
            assert_eq!(field(value, "kind"), &string(kind.into()));
        }
        assert_eq!(field(&row[2], "value"), &Value::Int(i64::MIN));
        let node = field(&row[7], "value");
        assert_eq!(field(node, "id"), &Value::Int(i64::MAX));
        assert_eq!(
            field(node, "labels"),
            &Value::Array(vec![string("Person".into())])
        );
        assert!(matches!(field(node, "properties"), Value::Array(_)));
        assert_eq!(
            field(field(&row[6], "value"), "type"),
            &string("KNOWS".into())
        );
        let path = field(&row[8], "value");
        let Value::Array(nodes) = field(path, "nodes") else {
            panic!("Expected nodes")
        };
        assert_eq!(field(&nodes[0], "kind"), &string("node".into()));
        let Value::Array(relationships) = field(path, "relationships") else {
            panic!("Expected relationships")
        };
        assert_eq!(
            field(&relationships[0], "kind"),
            &string("relationship".into())
        );
        let Value::Array(entries) = field(&row[9], "value") else {
            panic!("Expected ordered pairs")
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(
            field(field(&row[10], "value"), "latitude"),
            &Value::Double(1.25)
        );
        assert_eq!(
            field(&row[11], "value"),
            &Value::Array(vec![Value::Double(f64::from(0.1_f32)), Value::Double(-0.0)])
        );
        assert_eq!(field(&row[12], "value"), &Value::Int(i64::MAX));
        assert_eq!(field(&row[13], "value"), &Value::Int(i64::MIN));
        assert_eq!(field(&row[14], "value"), &Value::Int(11045));
        assert_eq!(field(&row[15], "value"), &Value::Int(36907506));
    }

    #[test]
    fn statistics_are_named_optional_and_lossless() {
        let value = encode_result(GraphQueryResult {
            statistics: GraphStatistics {
                graph_version: Some(u64::MAX),
                nodes_created: Some(1),
                cached_execution: Some(false),
                execution_time_ms: Some(0.125),
                raw: vec!["Future stat: exact".into()],
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap();
        let stats = field(&value, "statistics");
        assert_eq!(
            field(stats, "graph_version"),
            &Value::BigNumber(u64::MAX.into())
        );
        assert_eq!(field(stats, "nodes_created"), &Value::Int(1));
        assert_eq!(field(stats, "cached_execution"), &Value::Boolean(false));
        assert_eq!(field(stats, "execution_time_ms"), &Value::Double(0.125));
        assert_eq!(
            field(stats, "raw"),
            &Value::Array(vec![string("Future stat: exact".into())])
        );
        let Value::Map(fields) = stats else {
            panic!("Expected statistics map")
        };
        assert_eq!(fields.len(), 5);
    }

    #[test]
    fn rejects_invalid_manually_constructed_results_and_depth() {
        assert!(
            encode_result(GraphQueryResult {
                columns: vec!["x".into()],
                ..Default::default()
            })
            .is_ok()
        );
        assert!(
            encode_result(GraphQueryResult {
                rows: vec![vec![GraphValue::Null]],
                ..Default::default()
            })
            .is_err()
        );
        assert!(encode_value(GraphValue::Node(n(-1)), 0).is_err());
        let mut relationship = r();
        relationship.destination_id = -1;
        assert!(encode_value(GraphValue::Relationship(relationship), 0).is_err());
        assert!(
            encode_value(
                GraphValue::Path(GraphPath {
                    nodes: vec![],
                    relationships: vec![]
                }),
                0
            )
            .is_err()
        );
        assert!(
            encode_value(
                GraphValue::Path(GraphPath {
                    nodes: vec![n(1), n(3)],
                    relationships: vec![r()]
                }),
                0
            )
            .is_err()
        );
        for value in [f64::NAN, f64::INFINITY, -1.0] {
            assert!(
                encode_result(GraphQueryResult {
                    statistics: GraphStatistics {
                        execution_time_ms: Some(value),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .is_err()
            );
        }
        let mut value = GraphValue::Null;
        for _ in 0..MAX_DEPTH {
            value = GraphValue::List(vec![value]);
        }
        assert!(encode_value(value.clone(), 0).is_ok());
        assert!(encode_value(GraphValue::List(vec![value]), 0).is_err());
    }
}
