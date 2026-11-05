// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Strict decoder for FalkorDB v6.0.3 `src/reply.rs` compact replies.
//!
//! A result is `[header, rows, statistics]`, or `[statistics]` without RETURN.
//! Headers are `[1, name]`; values use tags 1–16. Dictionaries are scoped to
//! one operation and are never inferred from the `Graph version` statistic.

use redis::{ErrorKind, RedisError, RedisResult, Value};

use super::value::*;

const MAX_DEPTH: usize = 128;
const MAX_WIRE_DEPTH: usize = 512;

/// Dictionaries referenced by compact entities anywhere in a query's values.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RequiredDictionaries {
    /// At least one node references a label ID.
    pub labels: bool,
    /// At least one relationship references a type ID.
    pub relationship_types: bool,
    /// At least one entity references a property ID.
    pub property_keys: bool,
}

fn invalid(detail: &str) -> RedisError {
    (
        ErrorKind::TypeError,
        "Invalid compact graph response",
        detail.to_owned(),
    )
        .into()
}

fn array(value: &Value) -> RedisResult<&[Value]> {
    match value {
        Value::Array(values) => Ok(values),
        Value::ServerError(error) => Err(error.clone().into()),
        _ => Err(invalid("Expected an array")),
    }
}

fn fixed(value: &Value, length: usize) -> RedisResult<&[Value]> {
    let values = array(value)?;
    if values.len() != length {
        return Err(invalid("Unexpected compact array length"));
    }
    Ok(values)
}

fn text(value: &Value) -> RedisResult<&str> {
    match value {
        Value::BulkString(bytes) => {
            std::str::from_utf8(bytes).map_err(|_| invalid("Expected valid UTF-8"))
        }
        Value::SimpleString(value) => Ok(value),
        Value::ServerError(error) => Err(error.clone().into()),
        _ => Err(invalid("Expected a string")),
    }
}

fn integer(value: &Value) -> RedisResult<i64> {
    match value {
        Value::Int(value) => Ok(*value),
        Value::ServerError(error) => Err(error.clone().into()),
        _ => Err(invalid("Expected an exact integer")),
    }
}

fn id(value: &Value) -> RedisResult<i64> {
    let value = integer(value)?;
    if value < 0 {
        return Err(invalid("Entity and dictionary IDs must be nonnegative"));
    }
    Ok(value)
}

fn number(value: &Value) -> RedisResult<f64> {
    match value {
        Value::Double(value) => Ok(*value),
        _ => text(value)?
            .parse()
            .map_err(|_| invalid("Expected a floating-point number")),
    }
}

fn check_errors(value: &Value, depth: usize) -> RedisResult<()> {
    if depth > MAX_WIRE_DEPTH {
        return Err(invalid("Wire response nesting exceeds 512 levels"));
    }
    match value {
        Value::ServerError(error) => return Err(error.clone().into()),
        Value::Array(values) | Value::Set(values) | Value::Push { data: values, .. } => {
            for value in values {
                check_errors(value, depth + 1)?;
            }
        }
        Value::Map(values) => {
            for (key, value) in values {
                check_errors(key, depth + 1)?;
                check_errors(value, depth + 1)?;
            }
        }
        Value::Attribute { data, attributes } => {
            check_errors(data, depth + 1)?;
            for (key, value) in attributes {
                check_errors(key, depth + 1)?;
                check_errors(value, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

struct ResultParts<'a> {
    columns: &'a [Value],
    rows: &'a [Value],
    statistics: &'a Value,
}

fn parts(value: &Value) -> RedisResult<ResultParts<'_>> {
    let values = array(value)?;
    match values {
        [statistics] => Ok(ResultParts {
            columns: &[],
            rows: &[],
            statistics,
        }),
        [columns, rows, statistics] => {
            let columns = array(columns)?;
            for column in columns {
                let column = fixed(column, 2)?;
                if integer(&column[0])? != 1 {
                    return Err(invalid("Unknown compact column type"));
                }
                text(&column[1])?;
            }
            let rows = array(rows)?;
            for row in rows {
                fixed(row, columns.len())?;
            }
            Ok(ResultParts {
                columns,
                rows,
                statistics,
            })
        }
        _ => Err(invalid("Expected a one- or three-element result envelope")),
    }
}

fn tagged(value: &Value) -> RedisResult<(i64, &Value)> {
    let pair = fixed(value, 2)?;
    Ok((integer(&pair[0])?, &pair[1]))
}

fn inspect_properties(
    value: &Value,
    requirements: &mut RequiredDictionaries,
    depth: usize,
) -> RedisResult<()> {
    let values = array(value)?;
    if !values.is_empty() {
        requirements.property_keys = true;
    }
    for property in values {
        let property = fixed(property, 3)?;
        id(&property[0])?;
        let tag = integer(&property[1])?;
        inspect(tag, &property[2], requirements, depth + 1)?;
    }
    Ok(())
}

fn inspect(
    tag: i64,
    value: &Value,
    requirements: &mut RequiredDictionaries,
    depth: usize,
) -> RedisResult<()> {
    if depth > MAX_DEPTH {
        return Err(invalid("Graph value nesting exceeds 128 levels"));
    }
    match tag {
        1 if matches!(value, Value::Nil) => {}
        2 => {
            text(value)?;
        }
        3 | 13..=16 => {
            integer(value)?;
        }
        4 => match value {
            Value::Boolean(_) => {}
            _ if matches!(text(value)?, "true" | "false") => {}
            _ => return Err(invalid("Expected true or false")),
        },
        5 => {
            number(value)?;
        }
        6 => {
            for item in array(value)? {
                let (tag, value) = tagged(item)?;
                inspect(tag, value, requirements, depth + 1)?;
            }
        }
        7 => {
            let entity = fixed(value, 5)?;
            for value in &entity[..4] {
                id(value)?;
            }
            requirements.relationship_types = true;
            inspect_properties(&entity[4], requirements, depth)?;
        }
        8 => {
            let entity = fixed(value, 3)?;
            id(&entity[0])?;
            let labels = array(&entity[1])?;
            requirements.labels |= !labels.is_empty();
            for label in labels {
                id(label)?;
            }
            inspect_properties(&entity[2], requirements, depth)?;
        }
        9 => {
            let path = fixed(value, 2)?;
            let mut lengths = [0; 2];
            for (index, (list, expected)) in path.iter().zip([8, 7]).enumerate() {
                let (tag, value) = tagged(list)?;
                if tag != 6 {
                    return Err(invalid("Path components must be tagged lists"));
                }
                let values = array(value)?;
                lengths[index] = values.len();
                for value in values {
                    let (tag, value) = tagged(value)?;
                    if tag != expected {
                        return Err(invalid("Path lists must contain nodes and relationships"));
                    }
                    inspect(tag, value, requirements, depth + 1)?;
                }
            }
            if lengths[0].checked_sub(1) != Some(lengths[1]) {
                return Err(invalid("A path must have one more node than relationships"));
            }
        }
        10 => {
            let entries = array(value)?;
            if entries.len() % 2 != 0 {
                return Err(invalid(
                    "Map payload must contain alternating keys and values",
                ));
            }
            for entry in entries.chunks_exact(2) {
                text(&entry[0])?;
                let (tag, value) = tagged(&entry[1])?;
                inspect(tag, value, requirements, depth + 1)?;
            }
        }
        11 => {
            let point = fixed(value, 2)?;
            number(&point[0])?;
            number(&point[1])?;
        }
        12 => {
            for value in array(value)? {
                let value = number(value)?;
                let narrowed = value as f32;
                if value.is_finite() && (!narrowed.is_finite() || (value != 0.0 && narrowed == 0.0))
                {
                    return Err(invalid("Vector component exceeds the float32 range"));
                }
            }
        }
        _ => return Err(invalid("Unknown compact value tag or invalid null payload")),
    }
    Ok(())
}

/// Inspect only typed query values to determine which dictionaries are needed.
///
/// Traverses lists, maps, paths and entity properties with bounded recursion.
/// It validates the compact envelope and propagates nested server errors; it
/// never mistakes arbitrary integer pairs or metadata for schema references.
pub fn required_dictionaries(value: &Value) -> RedisResult<RequiredDictionaries> {
    check_errors(value, 0)?;
    let parts = parts(value)?;
    statistics(parts.statistics)?;
    let mut requirements = RequiredDictionaries::default();
    for row in parts.rows {
        for value in array(row)? {
            let (tag, value) = tagged(value)?;
            inspect(tag, value, &mut requirements, 0)?;
        }
    }
    Ok(requirements)
}

fn lookup(dictionary: &[String], value: &Value) -> RedisResult<String> {
    let index = usize::try_from(id(value)?)
        .map_err(|_| invalid("Dictionary ID exceeds the platform index range"))?;
    dictionary
        .get(index)
        .cloned()
        .ok_or_else(|| invalid("Dictionary ID is not present in the operation's schema"))
}

fn decode_properties(
    value: &Value,
    schema: &GraphSchema,
    depth: usize,
) -> RedisResult<Vec<(String, GraphValue)>> {
    array(value)?
        .iter()
        .map(|value| {
            let property = fixed(value, 3)?;
            Ok((
                lookup(&schema.property_keys, &property[0])?,
                decode_value(integer(&property[1])?, &property[2], schema, depth + 1)?,
            ))
        })
        .collect()
}

fn node(value: &Value, schema: &GraphSchema, depth: usize) -> RedisResult<GraphNode> {
    let value = fixed(value, 3)?;
    Ok(GraphNode {
        id: id(&value[0])?,
        labels: array(&value[1])?
            .iter()
            .map(|label| lookup(&schema.labels, label))
            .collect::<RedisResult<_>>()?,
        properties: decode_properties(&value[2], schema, depth)?,
    })
}

fn relationship(
    value: &Value,
    schema: &GraphSchema,
    depth: usize,
) -> RedisResult<GraphRelationship> {
    let value = fixed(value, 5)?;
    Ok(GraphRelationship {
        id: id(&value[0])?,
        r#type: lookup(&schema.relationship_types, &value[1])?,
        source_id: id(&value[2])?,
        destination_id: id(&value[3])?,
        properties: decode_properties(&value[4], schema, depth)?,
    })
}

fn path(value: &Value, schema: &GraphSchema, depth: usize) -> RedisResult<GraphPath> {
    let value = fixed(value, 2)?;
    let nodes = array(tagged(&value[0])?.1)?
        .iter()
        .map(|value| node(tagged(value)?.1, schema, depth + 1))
        .collect::<RedisResult<Vec<_>>>()?;
    let relationships = array(tagged(&value[1])?.1)?
        .iter()
        .map(|value| relationship(tagged(value)?.1, schema, depth + 1))
        .collect::<RedisResult<Vec<_>>>()?;
    for (pair, relationship) in nodes.windows(2).zip(&relationships) {
        if !((pair[0].id == relationship.source_id && pair[1].id == relationship.destination_id)
            || (pair[1].id == relationship.source_id && pair[0].id == relationship.destination_id))
        {
            return Err(invalid(
                "Path relationship endpoints do not match its adjacent nodes",
            ));
        }
    }
    Ok(GraphPath {
        nodes,
        relationships,
    })
}

fn decode_value(
    tag: i64,
    value: &Value,
    schema: &GraphSchema,
    depth: usize,
) -> RedisResult<GraphValue> {
    if depth > MAX_DEPTH {
        return Err(invalid("Graph value nesting exceeds 128 levels"));
    }
    Ok(match tag {
        1 => GraphValue::Null,
        2 => GraphValue::String(text(value)?.to_owned()),
        3 => GraphValue::Integer(integer(value)?),
        4 => GraphValue::Boolean(match value {
            Value::Boolean(value) => *value,
            _ => text(value)? == "true",
        }),
        5 => GraphValue::Double(number(value)?),
        6 => GraphValue::List(
            array(value)?
                .iter()
                .map(|value| {
                    let (tag, value) = tagged(value)?;
                    decode_value(tag, value, schema, depth + 1)
                })
                .collect::<RedisResult<_>>()?,
        ),
        7 => GraphValue::Relationship(relationship(value, schema, depth)?),
        8 => GraphValue::Node(node(value, schema, depth)?),
        9 => GraphValue::Path(path(value, schema, depth)?),
        10 => GraphValue::Map(
            array(value)?
                .chunks_exact(2)
                .map(|entry| {
                    let (tag, value) = tagged(&entry[1])?;
                    Ok((
                        text(&entry[0])?.to_owned(),
                        decode_value(tag, value, schema, depth + 1)?,
                    ))
                })
                .collect::<RedisResult<_>>()?,
        ),
        11 => {
            let point = fixed(value, 2)?;
            GraphValue::Point(GraphPoint {
                latitude: number(&point[0])?,
                longitude: number(&point[1])?,
            })
        }
        12 => GraphValue::Vector(
            array(value)?
                .iter()
                .map(|value| number(value).map(|value| value as f32))
                .collect::<RedisResult<_>>()?,
        ),
        13 => GraphValue::DateTime(integer(value)?),
        14 => GraphValue::Date(integer(value)?),
        15 => GraphValue::Time(integer(value)?),
        16 => GraphValue::Duration(integer(value)?),
        _ => return Err(invalid("Unknown compact value tag")),
    })
}

fn count(value: &str) -> RedisResult<u64> {
    value
        .parse()
        .map_err(|_| invalid("Invalid nonnegative integer statistic"))
}

fn set<T>(field: &mut Option<T>, value: T) -> RedisResult<()> {
    if field.is_some() {
        return Err(invalid("Duplicate known statistic"));
    }
    *field = Some(value);
    Ok(())
}

fn statistics(value: &Value) -> RedisResult<GraphStatistics> {
    let mut result = GraphStatistics::default();
    for value in array(value)? {
        let raw = text(value)?;
        result.raw.push(raw.to_owned());
        let Some((name, value)) = raw.split_once(": ") else {
            continue;
        };
        match name {
            "Labels added" => set(&mut result.labels_added, count(value)?)?,
            "Labels removed" => set(&mut result.labels_removed, count(value)?)?,
            "Nodes created" => set(&mut result.nodes_created, count(value)?)?,
            "Nodes deleted" => set(&mut result.nodes_deleted, count(value)?)?,
            "Properties set" => set(&mut result.properties_set, count(value)?)?,
            "Properties removed" => set(&mut result.properties_removed, count(value)?)?,
            "Relationships created" => set(&mut result.relationships_created, count(value)?)?,
            "Relationships deleted" => set(&mut result.relationships_deleted, count(value)?)?,
            "Indices created" => set(&mut result.indices_created, count(value)?)?,
            "Indices deleted" => set(&mut result.indices_deleted, count(value)?)?,
            "Graph version" => set(&mut result.graph_version, count(value)?)?,
            "Cached execution" => {
                let cached = match value {
                    "0" => false,
                    "1" => true,
                    _ => return Err(invalid("Cached execution must be 0 or 1")),
                };
                set(&mut result.cached_execution, cached)?;
            }
            "Query internal execution time" => {
                let milliseconds = value
                    .strip_suffix(" milliseconds")
                    .ok_or_else(|| invalid("Execution time must be in milliseconds"))?
                    .parse::<f64>()
                    .map_err(|_| invalid("Invalid execution time"))?;
                if !milliseconds.is_finite() || milliseconds < 0.0 {
                    return Err(invalid("Execution time must be finite and nonnegative"));
                }
                set(&mut result.execution_time_ms, milliseconds)?;
            }
            _ => {}
        }
    }
    Ok(result)
}

/// Decode a compact query result using dictionaries from the same operation.
///
/// Unknown tags, malformed shapes, invalid UTF-8, unresolved IDs, over-deep
/// values, row-width mismatches and nested server errors fail explicitly.
pub fn decode_result(value: &Value, schema: &GraphSchema) -> RedisResult<GraphQueryResult> {
    required_dictionaries(value)?;
    let parts = parts(value)?;
    let columns = parts
        .columns
        .iter()
        .map(|column| Ok(text(&fixed(column, 2)?[1])?.to_owned()))
        .collect::<RedisResult<_>>()?;
    let rows = parts
        .rows
        .iter()
        .map(|row| {
            array(row)?
                .iter()
                .map(|value| {
                    let (tag, value) = tagged(value)?;
                    decode_value(tag, value, schema, 0)
                })
                .collect::<RedisResult<_>>()
        })
        .collect::<RedisResult<_>>()?;
    Ok(GraphQueryResult {
        columns,
        rows,
        statistics: statistics(parts.statistics)?,
    })
}

/// Decode a one-column dictionary-procedure result without resolving metadata.
///
/// Only compact string scalars are accepted. Nested lists/maps/entities are
/// rejected rather than recursively looking up their own dictionary entries.
pub fn decode_dictionary(value: &Value) -> RedisResult<Vec<String>> {
    check_errors(value, 0)?;
    let parts = parts(value)?;
    if parts.columns.len() != 1 {
        return Err(invalid(
            "Dictionary procedure must return exactly one column",
        ));
    }
    statistics(parts.statistics)?;
    parts
        .rows
        .iter()
        .map(|row| {
            let row = fixed(row, 1)?;
            let (tag, value) = tagged(&row[0])?;
            if tag != 2 {
                return Err(invalid("Dictionary entries must be string scalars"));
            }
            Ok(text(value)?.to_owned())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(values: Vec<Value>) -> Value {
        Value::Array(values)
    }
    fn s(value: &str) -> Value {
        Value::BulkString(value.to_owned().into())
    }
    fn t(tag: i64, value: Value) -> Value {
        a(vec![Value::Int(tag), value])
    }
    fn result(values: Vec<Value>) -> Value {
        a(vec![
            a(values
                .iter()
                .map(|_| a(vec![Value::Int(1), s("x")]))
                .collect()),
            a(vec![a(values)]),
            a(vec![
                s("Cached execution: 0"),
                s("Query internal execution time: 0.250000 milliseconds"),
                s("Graph version: 42"),
                s("Future counter: 7"),
            ]),
        ])
    }
    fn node_wire(id: i64) -> Value {
        a(vec![
            Value::Int(id),
            a(vec![Value::Int(0)]),
            a(vec![a(vec![
                Value::Int(0),
                Value::Int(3),
                Value::Int(i64::MAX),
            ])]),
        ])
    }
    fn relationship_wire() -> Value {
        a(vec![
            Value::Int(9),
            Value::Int(0),
            Value::Int(1),
            Value::Int(2),
            a(vec![]),
        ])
    }
    fn schema() -> GraphSchema {
        GraphSchema {
            labels: vec!["Person".into()],
            relationship_types: vec!["KNOWS".into()],
            property_keys: vec!["id".into()],
        }
    }

    fn recorded(value: &serde_json::Value) -> Value {
        let payload = &value["value"];
        match value["type"].as_str().unwrap() {
            "array" => a(payload.as_array().unwrap().iter().map(recorded).collect()),
            "bulk" => s(&payload
                .as_str()
                .unwrap()
                .replace("<milliseconds>", "0.000000")),
            "integer" => Value::Int(payload.as_i64().unwrap()),
            "double" => Value::Double(payload.as_str().unwrap().parse().unwrap()),
            "null" => Value::Nil,
            other => panic!("Unexpected recorded compact fixture type: {other}"),
        }
    }

    #[test]
    fn decodes_live_pinned_resp2_and_resp3_recordings_identically() {
        let mut previous = None;
        for source in [
            include_str!("../../tests/fixtures/graph/resp2.json"),
            include_str!("../../tests/fixtures/graph/resp3.json"),
        ] {
            let fixture: serde_json::Value = serde_json::from_str(source).unwrap();
            let case = fixture["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|case| case["name"] == "all_tags")
                .unwrap();
            let steps = case["steps"].as_array().unwrap();
            let schema = GraphSchema {
                labels: decode_dictionary(&recorded(&steps[1]["response"])).unwrap(),
                relationship_types: decode_dictionary(&recorded(&steps[2]["response"])).unwrap(),
                property_keys: decode_dictionary(&recorded(&steps[3]["response"])).unwrap(),
            };
            let wire = recorded(&steps[0]["response"]);
            let result = decode_result(&wire, &schema).unwrap();
            assert_eq!(result.columns.len(), 16);
            assert_eq!(result.rows[0].len(), 16);
            assert_eq!(result.rows[0][2], GraphValue::Integer(i64::MIN));
            assert_eq!(result.rows[0][12], GraphValue::DateTime(1_577_934_245));
            assert_eq!(result.rows[0][13], GraphValue::Date(1_577_923_200));
            assert_eq!(result.rows[0][14], GraphValue::Time(11_045));
            assert_eq!(result.rows[0][15], GraphValue::Duration(36_907_506));
            assert_eq!(result.statistics.nodes_created, Some(2));
            assert_eq!(result.statistics.properties_set, Some(4));
            assert_eq!(result.statistics.graph_version, Some(1));
            super::super::bridge::encode_result(result.clone()).unwrap();
            if let Some(previous) = &previous {
                assert_eq!(&result, previous);
            }
            previous = Some(result);
        }
    }

    #[test]
    fn decodes_all_sixteen_pinned_tags_losslessly() {
        let wire = result(vec![
            t(1, Value::Nil),
            t(2, s("界😀")),
            t(3, Value::Int(i64::MIN)),
            t(4, s("true")),
            t(5, s("-0")),
            t(6, a(vec![t(3, Value::Int(i64::MAX))])),
            t(7, relationship_wire()),
            t(8, node_wire(1)),
            t(
                9,
                a(vec![
                    t(6, a(vec![t(8, node_wire(1)), t(8, node_wire(2))])),
                    t(6, a(vec![t(7, relationship_wire())])),
                ]),
            ),
            t(
                10,
                a(vec![s("k"), t(1, Value::Nil), s("k"), t(4, s("false"))]),
            ),
            t(11, a(vec![s("1.25"), Value::Double(-2.5)])),
            t(12, a(vec![s("1.25"), Value::Double(-2.5)])),
            t(13, Value::Int(i64::MAX)),
            t(14, Value::Int(i64::MIN)),
            t(15, Value::Int(123)),
            t(16, Value::Int(-123)),
        ]);
        assert_eq!(
            required_dictionaries(&wire).unwrap(),
            RequiredDictionaries {
                labels: true,
                relationship_types: true,
                property_keys: true
            }
        );
        let decoded = decode_result(&wire, &schema()).unwrap();
        assert_eq!(decoded.columns, vec!["x"; 16]);
        let row = &decoded.rows[0];
        assert_eq!(row[0], GraphValue::Null);
        assert_eq!(row[1], GraphValue::String("界😀".into()));
        assert_eq!(row[2], GraphValue::Integer(i64::MIN));
        assert_eq!(row[3], GraphValue::Boolean(true));
        let GraphValue::Double(zero) = row[4] else {
            panic!("Expected double")
        };
        assert!(zero.is_sign_negative());
        assert_eq!(
            row[5],
            GraphValue::List(vec![GraphValue::Integer(i64::MAX)])
        );
        let GraphValue::Relationship(rel) = &row[6] else {
            panic!("Expected relationship")
        };
        assert_eq!(rel.r#type, "KNOWS");
        let GraphValue::Node(node) = &row[7] else {
            panic!("Expected node")
        };
        assert_eq!(node.labels, ["Person"]);
        assert_eq!(
            node.properties,
            [("id".into(), GraphValue::Integer(i64::MAX))]
        );
        let GraphValue::Path(path) = &row[8] else {
            panic!("Expected path")
        };
        assert_eq!(path.nodes.len(), 2);
        assert_eq!(path.relationships.len(), 1);
        assert_eq!(
            row[9],
            GraphValue::Map(vec![
                ("k".into(), GraphValue::Null),
                ("k".into(), GraphValue::Boolean(false))
            ])
        );
        assert_eq!(
            row[10],
            GraphValue::Point(GraphPoint {
                latitude: 1.25,
                longitude: -2.5
            })
        );
        assert_eq!(row[11], GraphValue::Vector(vec![1.25, -2.5]));
        assert_eq!(row[12], GraphValue::DateTime(i64::MAX));
        assert_eq!(row[13], GraphValue::Date(i64::MIN));
        assert_eq!(row[14], GraphValue::Time(123));
        assert_eq!(row[15], GraphValue::Duration(-123));
        assert_eq!(decoded.statistics.graph_version, Some(42));
        assert_eq!(decoded.statistics.execution_time_ms, Some(0.25));
        assert_eq!(decoded.statistics.raw[3], "Future counter: 7");
    }

    #[test]
    fn dictionary_lookup_is_scalar_only_and_not_recursive() {
        assert_eq!(
            decode_dictionary(&result(vec![t(2, s("Person"))])).unwrap(),
            ["Person"]
        );
        for value in [
            t(8, node_wire(1)),
            t(6, a(vec![t(2, s("name"))])),
            t(3, Value::Int(0)),
        ] {
            assert!(decode_dictionary(&result(vec![value])).is_err());
        }
        assert!(decode_dictionary(&result(vec![])).is_err());
    }

    #[test]
    fn discovers_entities_nested_in_maps_lists_and_properties() {
        let node = a(vec![
            Value::Int(1),
            a(vec![]),
            a(vec![a(vec![
                Value::Int(0),
                Value::Int(6),
                a(vec![t(7, relationship_wire())]),
            ])]),
        ]);
        let wire = result(vec![t(10, a(vec![s("nodes"), t(6, a(vec![t(8, node)]))]))]);
        assert_eq!(
            required_dictionaries(&wire).unwrap(),
            RequiredDictionaries {
                labels: false,
                relationship_types: true,
                property_keys: true
            }
        );
        assert!(decode_result(&wire, &schema()).is_ok());
        assert!(decode_result(&wire, &GraphSchema::default()).is_err());
    }

    #[test]
    fn rejects_malformed_types_lengths_ids_and_utf8() {
        let malformed = vec![
            t(0, Value::Nil),
            t(17, Value::Nil),
            t(1, Value::Int(0)),
            t(2, Value::BulkString(vec![0xff].into())),
            t(3, s("42")),
            t(4, s("1")),
            t(5, s("no")),
            t(6, a(vec![Value::Int(1)])),
            t(7, a(vec![])),
            t(8, node_wire(-1)),
            t(9, a(vec![a(vec![]), a(vec![])])),
            t(9, a(vec![t(6, a(vec![])), t(6, a(vec![]))])),
            t(10, a(vec![s("odd")])),
            t(10, a(vec![Value::Int(1), t(1, Value::Nil)])),
            t(11, a(vec![s("1")])),
            t(12, a(vec![s("1e100")])),
            t(12, a(vec![s("1e-100")])),
            t(13, Value::Double(1.0)),
            t(14, s("1")),
            t(15, Value::Nil),
            t(16, s("1.0")),
        ];
        for value in malformed {
            assert!(decode_result(&result(vec![value]), &schema()).is_err());
        }
        let unknown_id = a(vec![
            Value::Int(1),
            a(vec![Value::Int(i64::MAX)]),
            a(vec![]),
        ]);
        assert!(decode_result(&result(vec![t(8, unknown_id)]), &schema()).is_err());
        assert!(decode_result(&a(vec![]), &schema()).is_err());
        assert!(
            decode_result(
                &a(vec![
                    a(vec![]),
                    a(vec![a(vec![t(1, Value::Nil)])]),
                    a(vec![])
                ]),
                &schema()
            )
            .is_err()
        );
    }

    #[test]
    fn propagates_nested_server_errors_even_in_invalid_metadata() {
        let error = Value::ServerError(
            RedisError::from((ErrorKind::ResponseError, "nested failure")).into(),
        );
        for wire in [
            result(vec![t(6, a(vec![error.clone()]))]),
            a(vec![a(vec![Value::Map(vec![(
                s("hidden"),
                error.clone(),
            )])])]),
        ] {
            for failure in [
                decode_result(&wire, &schema()).unwrap_err(),
                required_dictionaries(&wire).unwrap_err(),
                decode_dictionary(&wire).unwrap_err(),
            ] {
                assert!(failure.to_string().contains("nested failure"));
            }
        }
    }

    #[test]
    fn bounds_nested_values_and_accepts_statistics_only_mutations() {
        let mut value = t(1, Value::Nil);
        for _ in 0..MAX_DEPTH {
            value = t(6, a(vec![value]));
        }
        assert!(decode_result(&result(vec![value.clone()]), &schema()).is_ok());
        assert!(decode_result(&result(vec![t(6, a(vec![value]))]), &schema()).is_err());
        let wire = a(vec![a(vec![
            s("Nodes created: 1"),
            s("Graph version: 9007199254740993"),
        ])]);
        let decoded = decode_result(&wire, &schema()).unwrap();
        assert!(decoded.columns.is_empty() && decoded.rows.is_empty());
        assert_eq!(decoded.statistics.nodes_created, Some(1));
        assert_eq!(
            decoded.statistics.graph_version,
            Some(9_007_199_254_740_993)
        );
        assert_eq!(
            required_dictionaries(&wire).unwrap(),
            RequiredDictionaries::default()
        );
        for statistic in [
            "Nodes created: -1",
            "Graph version: nope",
            "Cached execution: 2",
            "Query internal execution time: NaN milliseconds",
            "Query internal execution time: -1 milliseconds",
            "Query internal execution time: 1 seconds",
        ] {
            assert!(decode_result(&a(vec![a(vec![s(statistic)])]), &schema()).is_err());
        }
        assert!(
            decode_result(
                &a(vec![a(vec![s("Nodes created: 1"), s("Nodes created: 2")])]),
                &schema()
            )
            .is_err()
        );
    }

    #[test]
    fn decodes_every_known_statistic_and_preserves_unknown_and_duplicate_raw_names() {
        let raw = [
            "Labels added: 1",
            "Labels removed: 2",
            "Nodes created: 3",
            "Nodes deleted: 4",
            "Properties set: 5",
            "Properties removed: 6",
            "Relationships created: 7",
            "Relationships deleted: 8",
            "Indices created: 9",
            "Indices deleted: 10",
            "Cached execution: 1",
            "Query internal execution time: 0.125 milliseconds",
            "Graph version: 18446744073709551615",
            "Future stat: a",
            "Future stat: b",
        ];
        let statistics = statistics(&a(raw.iter().map(|value| s(value)).collect())).unwrap();
        assert_eq!(statistics.labels_added, Some(1));
        assert_eq!(statistics.labels_removed, Some(2));
        assert_eq!(statistics.nodes_created, Some(3));
        assert_eq!(statistics.nodes_deleted, Some(4));
        assert_eq!(statistics.properties_set, Some(5));
        assert_eq!(statistics.properties_removed, Some(6));
        assert_eq!(statistics.relationships_created, Some(7));
        assert_eq!(statistics.relationships_deleted, Some(8));
        assert_eq!(statistics.indices_created, Some(9));
        assert_eq!(statistics.indices_deleted, Some(10));
        assert_eq!(statistics.cached_execution, Some(true));
        assert_eq!(statistics.execution_time_ms, Some(0.125));
        assert_eq!(statistics.graph_version, Some(u64::MAX));
        assert_eq!(statistics.raw, raw);
    }

    #[test]
    fn validates_path_endpoints_properties_and_dictionary_ids() {
        let negative_label = a(vec![Value::Int(1), a(vec![Value::Int(-1)]), a(vec![])]);
        let negative_property = a(vec![
            Value::Int(1),
            a(vec![]),
            a(vec![a(vec![Value::Int(-1), Value::Int(1), Value::Nil])]),
        ]);
        let short_property = a(vec![
            Value::Int(1),
            a(vec![]),
            a(vec![a(vec![Value::Int(0), Value::Int(1)])]),
        ]);
        for value in [negative_label, negative_property, short_property] {
            assert!(required_dictionaries(&result(vec![t(8, value)])).is_err());
        }
        let invalid_path = t(
            9,
            a(vec![
                t(6, a(vec![t(8, node_wire(1)), t(8, node_wire(3))])),
                t(6, a(vec![t(7, relationship_wire())])),
            ]),
        );
        assert!(decode_result(&result(vec![invalid_path]), &schema()).is_err());
        let reverse_path = t(
            9,
            a(vec![
                t(6, a(vec![t(8, node_wire(2)), t(8, node_wire(1))])),
                t(6, a(vec![t(7, relationship_wire())])),
            ]),
        );
        assert!(decode_result(&result(vec![reverse_path]), &schema()).is_ok());
    }
}
