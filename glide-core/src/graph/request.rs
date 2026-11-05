// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

use super::{GraphContext, GraphParameter, GraphQueryOptions, bridge};
use crate::client::Client;
use crate::graph_request::{Parameter, ParameterMap, QueryRequest, parameter};
use protobuf::Message;
use redis::{ErrorKind, RedisError, RedisResult, Value};
use std::collections::BTreeMap;

/// Execute the graph-only byte bridge, preserving int64 values in protobuf.
pub async fn execute_request(client: Client, bytes: &[u8]) -> RedisResult<Value> {
    let deadline = tokio::time::Instant::now() + client.get_request_timeout();
    let request = parse_request(bytes)?;
    if client.graph_database_epoch() != request.database_epoch {
        return Err(invalid(
            "Graph handle's database changed; select a new graph handle".into(),
        ));
    }
    let graph = GraphContext::new(client, request.graph_name.to_string(), request.database)?;
    execute(graph, request, deadline).await
}

/// Execute a byte request using a previously selected, shared native context.
///
/// The request identity must match the immutable selection; it cannot redirect a
/// retained handle to another graph or database.
pub async fn execute_request_with_context(graph: GraphContext, bytes: &[u8]) -> RedisResult<Value> {
    let deadline = tokio::time::Instant::now() + graph.inner.client.get_request_timeout();
    let request = parse_request(bytes)?;
    if request.graph_name.as_bytes() != graph.name().as_bytes()
        || request.database != graph.inner.database
        || request.database_epoch != graph.inner.database_epoch
    {
        return Err(invalid(
            "Graph request does not match the selected graph context".into(),
        ));
    }
    execute(graph, request, deadline).await
}

fn parse_request(bytes: &[u8]) -> RedisResult<QueryRequest> {
    QueryRequest::parse_from_bytes(bytes)
        .map_err(|error| invalid(format!("Invalid graph request: {error}")))
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
                .insert(entry.key.to_string(), convert_parameter(value, depth + 1)?)
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
