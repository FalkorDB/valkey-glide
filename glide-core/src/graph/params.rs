// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Literal-only CYPHER parameter serialization, matching FalkorDB v6.0.3's
//! `graph/src/parser/{cypher,lexer,string_escape}.rs`.

use std::fmt::Write;

use redis::{ErrorKind, RedisError, RedisResult};

use super::value::{GraphParameter, GraphQueryOptions};

const MAX_DEPTH: usize = 128;
// FalkorDB v6.0.3 graph/src/identifier_limits.rs measures UTF-8 bytes.
const MAX_IDENTIFIER_BYTES: usize = 512;

fn invalid(detail: &str) -> RedisError {
    (
        ErrorKind::ClientError,
        "Invalid graph query parameter",
        detail.to_owned(),
    )
        .into()
}

/// Validate the graph key before dispatch. Graph names are keys, not identifiers.
pub fn validate_graph_name(name: &str) -> RedisResult<()> {
    if name.is_empty() || name.chars().any(char::is_control) {
        return Err(invalid(
            "Graph name must be nonempty and contain no control characters",
        ));
    }
    Ok(())
}

fn identifier(out: &mut String, key: &str) -> RedisResult<()> {
    if key.is_empty() || key.chars().any(|c| c == '`' || c.is_control()) {
        return Err(invalid(
            "Parameter/map keys must be nonempty and contain no backticks or control characters",
        ));
    }
    if key.len() > MAX_IDENTIFIER_BYTES {
        return Err(invalid(
            "Parameter/map keys must not exceed 512 UTF-8 bytes",
        ));
    }
    out.push('`');
    out.push_str(key);
    out.push('`');
    Ok(())
}

fn string(out: &mut String, text: &str) {
    out.push('\'');
    for ch in text.chars() {
        match ch {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if c.is_control() => {
                write!(out, "\\u{:04x}", u32::from(c)).expect("Writing to a String cannot fail");
            }
            c => out.push(c),
        }
    }
    out.push('\'');
}

fn parameter(out: &mut String, value: &GraphParameter, depth: usize) -> RedisResult<()> {
    if depth > MAX_DEPTH {
        return Err(invalid("Parameter nesting exceeds 128 levels"));
    }
    match value {
        GraphParameter::Null => out.push_str("null"),
        GraphParameter::Boolean(value) => out.push_str(if *value { "true" } else { "false" }),
        GraphParameter::Integer(value) => {
            write!(out, "{value}").expect("Writing to a String cannot fail");
        }
        GraphParameter::Double(value) => {
            if !value.is_finite() {
                return Err(invalid("Double parameters must be finite"));
            }
            let number = value.to_string();
            out.push_str(&number);
            if !number.contains(['.', 'e', 'E']) {
                out.push_str(".0");
            }
        }
        GraphParameter::String(value) => string(out, value),
        GraphParameter::List(values) => {
            out.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                parameter(out, value, depth + 1)?;
            }
            out.push(']');
        }
        GraphParameter::Map(values) => {
            out.push('{');
            for (index, (key, value)) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                identifier(out, key)?;
                out.push(':');
                parameter(out, value, depth + 1)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

/// Prefix a query with literal parameter assignments; never substitute query text.
///
/// Keys use server-supported backtick identifiers, and values use Cypher (not
/// JSON) escaping. The timeout remains a separate command option. Strings may
/// contain NUL/control characters because those are escaped inside literals;
/// the query body and identifiers must not contain NUL.
pub fn serialize_query(query: &str, options: &GraphQueryOptions) -> RedisResult<String> {
    if query.contains('\0') {
        return Err(invalid("Query text must not contain NUL"));
    }
    if options
        .timeout_ms
        .is_some_and(|timeout| timeout > i64::MAX as u64)
    {
        return Err(invalid(
            "Server timeout must fit a nonnegative signed 64-bit integer",
        ));
    }
    if options.parameters.is_empty() {
        return Ok(query.to_owned());
    }
    let mut out = String::from("CYPHER ");
    for (key, value) in &options.parameters {
        identifier(&mut out, key)?;
        out.push('=');
        parameter(&mut out, value, 0)?;
        out.push(' ');
    }
    out.push_str(query);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn query(value: GraphParameter) -> RedisResult<String> {
        serialize_query(
            "RETURN $p",
            &GraphQueryOptions {
                parameters: BTreeMap::from([("p".into(), value)]),
                ..Default::default()
            },
        )
    }

    #[test]
    fn serializes_all_literals_and_exact_integers() {
        let value = GraphParameter::List(vec![
            GraphParameter::Null,
            GraphParameter::Boolean(true),
            GraphParameter::Integer(i64::MIN),
            GraphParameter::Integer(i64::MAX),
            GraphParameter::Double(-0.0),
            GraphParameter::Map(BTreeMap::from([(
                "with space:界".into(),
                GraphParameter::Double(1.0),
            )])),
        ]);
        assert_eq!(
            query(value).unwrap(),
            "CYPHER `p`=[null,true,-9223372036854775808,9223372036854775807,-0.0,{`with space:界`:1.0}] RETURN $p"
        );
    }

    #[test]
    fn validates_the_server_timeout_integer_range_without_changing_zero() {
        for timeout in [0, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
            let result = serialize_query(
                "RETURN 1",
                &GraphQueryOptions {
                    timeout_ms: Some(timeout),
                    ..Default::default()
                },
            );
            assert_eq!(result.is_ok(), timeout <= i64::MAX as u64);
        }
    }

    #[test]
    fn escapes_injection_without_substituting_query_body() {
        assert_eq!(
            query(GraphParameter::String(
                "'\\\n\r\t\0\u{1}\u{8}\u{c}界😀\" // RETURN 42".into()
            ))
            .unwrap(),
            "CYPHER `p`='\\'\\\\\\n\\r\\t\\u0000\\u0001\\b\\f界😀\" // RETURN 42' RETURN $p"
        );
        assert_eq!(
            serialize_query("RETURN '$p', $p // $p", &GraphQueryOptions::default()).unwrap(),
            "RETURN '$p', $p // $p"
        );
    }

    #[test]
    fn rejects_nonfinite_values_and_invalid_keys_at_every_level() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(query(GraphParameter::List(vec![GraphParameter::Double(value)])).is_err());
        }
        for key in ["", "a`b", "x\0", "x\n", "x\u{7f}"] {
            assert!(
                query(GraphParameter::Map(BTreeMap::from([(
                    key.into(),
                    GraphParameter::Null
                )])))
                .is_err()
            );
            assert!(
                serialize_query(
                    "RETURN 1",
                    &GraphQueryOptions {
                        parameters: BTreeMap::from([(key.into(), GraphParameter::Null)]),
                        ..Default::default()
                    }
                )
                .is_err()
            );
        }
        assert!(serialize_query("RETURN '\0'", &GraphQueryOptions::default()).is_err());
        assert!(
            query(GraphParameter::Map(BTreeMap::from([(
                "界".repeat(171),
                GraphParameter::Null
            )])))
            .is_err()
        );
        assert!(
            query(GraphParameter::Map(BTreeMap::from([(
                "x".repeat(512),
                GraphParameter::Null
            )])))
            .is_ok()
        );
    }

    #[test]
    fn bounds_parameter_depth_and_preserves_timeout_independence() {
        let mut value = GraphParameter::Null;
        for _ in 0..MAX_DEPTH {
            value = GraphParameter::List(vec![value]);
        }
        assert!(query(value.clone()).is_ok());
        assert!(query(GraphParameter::List(vec![value])).is_err());
        assert_eq!(
            serialize_query(
                "RETURN 1",
                &GraphQueryOptions {
                    timeout_ms: Some(i64::MAX as u64),
                    ..Default::default()
                }
            )
            .unwrap(),
            "RETURN 1"
        );
    }

    #[test]
    fn graph_keys_are_not_cypher_identifiers() {
        for name in ["graph", "{tenant}:graph with space", "图", "a`b"] {
            assert!(validate_graph_name(name).is_ok());
        }
        for name in ["", "x\0", "x\n"] {
            assert!(validate_graph_name(name).is_err());
        }
    }
}
