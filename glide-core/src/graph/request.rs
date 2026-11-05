// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

use super::{GraphContext, GraphParameter, GraphQueryOptions, bridge, within_deadline};
use crate::client::Client;
use crate::graph_request::{Parameter, ParameterMap, QueryRequest, parameter};
use protobuf::Message;
use redis::{ErrorKind, RedisError, RedisResult, Value};
use std::collections::BTreeMap;
use std::future::Future;
use tokio::time::Instant;

/// Execute the graph-only byte bridge, preserving int64 values in protobuf.
pub fn execute_request(
    client: Client,
    bytes: &[u8],
) -> impl Future<Output = RedisResult<Value>> + '_ {
    execute_request_at(client, Ok(bytes), Instant::now())
}

/// Execute using the native submission time, including adaptation and queueing.
///
/// Adaptation failures are checked inside the same deadline as successful bytes.
pub async fn execute_request_at<B: AsRef<[u8]>>(
    client: Client,
    bytes: RedisResult<B>,
    submitted_at: Instant,
) -> RedisResult<Value> {
    let deadline = submitted_at + client.get_request_timeout();
    within_deadline(deadline, async {
        let bytes = bytes?;
        let request = parse_request(bytes.as_ref())?;
        if client.graph_database_epoch() != request.database_epoch {
            return Err(invalid(
                "Graph handle's database changed; select a new graph handle".into(),
            ));
        }
        let graph = GraphContext::new(client, request.graph_name.to_string(), request.database)?;
        execute(graph, request, deadline).await
    })
    .await
}

/// Execute a byte request using a previously selected, shared native context.
///
/// The request identity must match the immutable selection; it cannot redirect a
/// retained handle to another graph or database.
pub fn execute_request_with_context(
    graph: GraphContext,
    bytes: &[u8],
) -> impl Future<Output = RedisResult<Value>> + '_ {
    execute_request_with_context_at(graph, Ok(bytes), Instant::now())
}

/// Execute on a retained context with the original native submission deadline.
pub async fn execute_request_with_context_at<B: AsRef<[u8]>>(
    graph: GraphContext,
    bytes: RedisResult<B>,
    submitted_at: Instant,
) -> RedisResult<Value> {
    let deadline = submitted_at + graph.inner.client.get_request_timeout();
    within_deadline(deadline, async {
        let bytes = bytes?;
        let request = parse_request(bytes.as_ref())?;
        if request.graph_name.as_bytes() != graph.name().as_bytes()
            || request.database != graph.inner.database
            || request.database_epoch != graph.inner.database_epoch
        {
            return Err(invalid(
                "Graph request does not match the selected graph context".into(),
            ));
        }
        execute(graph, request, deadline).await
    })
    .await
}

/// Parse graph bridge bytes with the shared value-depth recursion budget.
pub fn parse_request(bytes: &[u8]) -> RedisResult<QueryRequest> {
    let mut input = protobuf::CodedInputStream::from_bytes(bytes);
    // Each map level has Parameter, ParameterMap, and ParameterEntry messages.
    input.set_recursion_limit(3 * 128 + 8);
    let request = QueryRequest::parse_from(&mut input)
        .map_err(|error| invalid(format!("Invalid graph request: {error}")))?;
    input
        .check_eof()
        .map_err(|error| invalid(format!("Invalid graph request: {error}")))?;
    Ok(request)
}

async fn execute(
    graph: GraphContext,
    request: QueryRequest,
    deadline: tokio::time::Instant,
) -> RedisResult<Value> {
    let options = GraphQueryOptions {
        parameters: convert_map(request.parameters.as_ref(), 0)?,
        timeout_ms: request.timeout_ms,
    };
    let result = graph
        .execute_with_deadline(&request.query, &options, request.readonly, deadline)
        .await?;
    bridge::encode_result(result)
}

fn convert_map(
    map: Option<&ParameterMap>,
    depth: usize,
) -> RedisResult<BTreeMap<String, GraphParameter>> {
    let mut result = BTreeMap::new();
    if let Some(map) = map {
        for entry in &map.entries {
            let value = entry
                .value
                .as_ref()
                .ok_or_else(|| invalid("Missing graph parameter value".into()))?;
            if result
                .insert(entry.key.to_string(), convert_parameter(value, depth)?)
                .is_some()
            {
                return Err(invalid(format!(
                    "Duplicate graph parameter key: {}",
                    entry.key
                )));
            }
        }
    }
    Ok(result)
}

fn convert_parameter(parameter: &Parameter, depth: usize) -> RedisResult<GraphParameter> {
    if depth > 128 {
        return Err(invalid("Graph parameters exceed maximum depth 128".into()));
    }
    Ok(match parameter.value.as_ref() {
        Some(parameter::Value::NullValue(_)) => GraphParameter::Null,
        Some(parameter::Value::BooleanValue(value)) => GraphParameter::Boolean(*value),
        Some(parameter::Value::IntegerValue(value)) => GraphParameter::Integer(*value),
        Some(parameter::Value::DoubleValue(value)) => GraphParameter::Double(*value),
        Some(parameter::Value::StringValue(value)) => GraphParameter::String(value.to_string()),
        Some(parameter::Value::ListValue(value)) => GraphParameter::List(
            value
                .values
                .iter()
                .map(|value| convert_parameter(value, depth + 1))
                .collect::<RedisResult<_>>()?,
        ),
        Some(parameter::Value::MapValue(value)) => {
            GraphParameter::Map(convert_map(Some(value), depth + 1)?)
        }
        None => return Err(invalid("Missing graph parameter type".into())),
    })
}

fn invalid(message: String) -> RedisError {
    RedisError::from((ErrorKind::ClientError, "Invalid graph input", message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_request::{ParameterEntry, ParameterList};
    use protobuf::MessageField;

    #[tokio::test]
    async fn original_submission_budget_includes_failed_native_adaptation() {
        use crate::client::{ConnectionRequest, NodeAddress};
        use std::time::Duration;

        let client = Client::new(
            ConnectionRequest {
                addresses: vec![NodeAddress {
                    host: "127.0.0.1".into(),
                    port: 1,
                    unix_socket_path: None,
                }],
                request_timeout: Some(1000),
                lazy_connect: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        for expired in [false, true] {
            let submitted_at = if expired {
                Instant::now() - Duration::from_secs(2)
            } else {
                Instant::now()
            };
            let adaptation: RedisResult<Vec<u8>> = Err(invalid("native adaptation failed".into()));
            let error = execute_request_at(client.clone(), adaptation, submitted_at)
                .await
                .unwrap_err();
            assert_eq!(error.is_timeout(), expired, "{error}");
            if !expired {
                assert!(error.to_string().contains("native adaptation failed"));
            }
            let graph = GraphContext::new(client.clone(), "local".into(), 0).unwrap();
            let adaptation: RedisResult<Vec<u8>> = Err(invalid("native adaptation failed".into()));
            let error = execute_request_with_context_at(graph, adaptation, submitted_at)
                .await
                .unwrap_err();
            assert_eq!(error.is_timeout(), expired, "{error}");
            if !expired {
                assert!(error.to_string().contains("native adaptation failed"));
            }
        }
    }

    #[test]
    fn bridge_and_serializer_agree_at_the_parameter_depth_boundary() {
        for map in [false, true] {
            for depth in [127, 128, 129] {
                let mut value = Parameter {
                    value: Some(parameter::Value::IntegerValue(42)),
                    ..Default::default()
                };
                for _ in 0..depth {
                    value = Parameter {
                        value: Some(if map {
                            parameter::Value::MapValue(ParameterMap {
                                entries: vec![ParameterEntry {
                                    key: "key".into(),
                                    value: MessageField::some(value),
                                    ..Default::default()
                                }],
                                ..Default::default()
                            })
                        } else {
                            parameter::Value::ListValue(ParameterList {
                                values: vec![value],
                                ..Default::default()
                            })
                        }),
                        ..Default::default()
                    };
                }
                let request = QueryRequest {
                    parameters: MessageField::some(ParameterMap {
                        entries: vec![ParameterEntry {
                            key: "p".into(),
                            value: MessageField::some(value),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let parsed = parse_request(&request.write_to_bytes().unwrap()).unwrap();
                let converted = convert_map(parsed.parameters.as_ref(), 0);
                assert_eq!(converted.is_ok(), depth <= 128, "map={map}, depth={depth}");
                if let Ok(parameters) = converted {
                    assert!(
                        super::super::params::serialize_query(
                            "RETURN $p",
                            &GraphQueryOptions {
                                parameters,
                                ..Default::default()
                            }
                        )
                        .is_ok()
                    );
                }
            }
        }
    }
}
