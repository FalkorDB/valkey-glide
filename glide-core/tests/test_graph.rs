// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Graph execution safety tests without a module-enabled server.

mod constants;
mod utilities;

use bytes::BytesMut;
use glide_core::client::{Client, ConnectionRequest, NodeAddress};
use glide_core::graph::{GraphContext, GraphQueryOptions, GraphValue};
use redis::{ConnectionAddr, Value, ValueCodec};
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::Decoder;
use utilities::mocks::{Mock, ServerMock};

fn text(value: &str) -> Value {
    Value::BulkString(value.as_bytes().to_vec().into())
}

fn response(value: Value) -> String {
    let mut buffer = Vec::new();
    utilities::encode_value(&value, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

fn result(cell: Value) -> String {
    response(Value::Array(vec![
        Value::Array(vec![Value::Array(vec![Value::Int(1), text("result")])]),
        Value::Array(vec![Value::Array(vec![cell])]),
        Value::Array(vec![]),
    ]))
}

fn node() -> Value {
    Value::Array(vec![
        Value::Int(8),
        Value::Array(vec![
            Value::Int(0),
            Value::Array(vec![Value::Int(0)]),
            Value::Array(vec![]),
        ]),
    ])
}

async fn delayed_client() -> (Client, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    delayed_client_with_timeout(250).await
}

async fn delayed_client_with_timeout(
    request_timeout: u32,
) -> (Client, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let commands = Arc::new(AtomicUsize::new(0));
    let observed = commands.clone();
    let server = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (mut socket, _) = accepted.unwrap();
                    let observed = observed.clone();
                    connections.spawn(async move {
                        let mut buffer = BytesMut::new();
                        let mut codec = ValueCodec::default();
                        loop {
                            if socket.read_buf(&mut buffer).await.unwrap() == 0 {
                                return;
                            }
                            while let Some(request) = codec.decode(&mut buffer).unwrap() {
                                let Value::Array(args) = request.unwrap() else {
                                    panic!("array command expected")
                                };
                                let Value::BulkString(name) = &args[0] else {
                                    panic!("command name expected")
                                };
                                let reply = match name.as_ref() {
                                    b"HELLO" => response(Value::Map(vec![
                                        (text("proto"), Value::Int(3)),
                                        (text("role"), text("master")),
                                    ])),
                                    b"CLIENT" => response(Value::Okay),
                                    b"PING" => response(Value::SimpleString("PONG".into())),
                                    b"INFO" => response(text("role:master\r\n")),
                                    b"SELECT" | b"MULTI" | b"DISCARD" => response(Value::Okay),
                                    b"GRAPH.QUERY" | b"GRAPH.RO_QUERY" => {
                                        observed.fetch_add(1, Ordering::AcqRel);
                                        tokio::time::sleep(Duration::from_millis(150)).await;
                                        if name.as_ref() == b"GRAPH.RO_QUERY" {
                                            result(Value::Array(vec![Value::Int(2), text("Label")]))
                                        } else if args[2] == text("RETURN n") {
                                            result(node())
                                        } else {
                                            result(Value::Array(vec![Value::Int(3), Value::Int(42)]))
                                        }
                                    }
                                    other => panic!("unexpected command {other:?}"),
                                };
                                if socket.write_all(reply.as_bytes()).await.is_err() {
                                    return;
                                }
                            }
                        }
                    });
                }
                result = connections.join_next(), if !connections.is_empty() => {
                    result.unwrap().unwrap();
                }
            }
        }
    });
    let client = Client::new(
        ConnectionRequest {
            addresses: vec![NodeAddress {
                host: address.ip().to_string(),
                port: address.port(),
                unix_socket_path: None,
            }],
            request_timeout: Some(request_timeout),
            inflight_requests_limit: Some(1),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    (client, commands, server)
}

fn server(scripts: HashMap<String, Vec<String>>) -> ServerMock {
    ServerMock::new_with_command_scripts(
        HashMap::from([
            (
                "*1\r\n$4\r\nPING\r\n".into(),
                Value::SimpleString("PONG".into()),
            ),
            (
                "*2\r\n$4\r\nINFO\r\n$11\r\nREPLICATION\r\n".into(),
                text("role:master\r\n"),
            ),
        ]),
        scripts,
    )
}

async fn delayed_byte_bridge_poll(retained: bool) {
    use glide_core::graph::{execute_request, execute_request_with_context};
    use protobuf::Message;

    let (client, observed, server) = delayed_client().await;
    let request = glide_core::graph_request::QueryRequest {
        graph_name: "graph".into(),
        query: "CREATE (n) RETURN 42".into(),
        ..Default::default()
    };
    let bytes = request.write_to_bytes().unwrap();
    let operation: std::pin::Pin<
        Box<dyn std::future::Future<Output = redis::RedisResult<Value>> + '_>,
    > = if retained {
        let graph = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
        Box::pin(execute_request_with_context(graph, &bytes))
    } else {
        Box::pin(execute_request(client.clone(), &bytes))
    };
    tokio::time::sleep(Duration::from_millis(350)).await;
    let result = operation.await;
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    assert!(
        result.as_ref().is_err_and(|error| error.is_timeout()),
        "retained={retained}: {result:?}"
    );
    assert_eq!(observed.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn byte_bridge_deadline_includes_time_before_first_poll() {
    delayed_byte_bridge_poll(false).await;
}

#[tokio::test]
async fn retained_byte_bridge_deadline_includes_time_before_first_poll() {
    delayed_byte_bridge_poll(true).await;
}

async fn client(mock: &ServerMock) -> Client {
    let address = match &mock.get_addresses()[0] {
        ConnectionAddr::Tcp(host, port) => NodeAddress {
            host: host.clone(),
            port: *port,
            unix_socket_path: None,
        },
        other => panic!("unexpected mock address {other:?}"),
    };
    Client::new(
        ConnectionRequest {
            addresses: vec![address],
            request_timeout: Some(1000),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn scalar_queries_need_no_metadata_or_additional_connections() {
    let mock = server(HashMap::from([(
        "GRAPH.QUERY".into(),
        vec![result(Value::Array(vec![Value::Int(3), Value::Int(42)]))],
    )]));
    let client = client(&mock).await;
    let connections = mock.get_number_of_accepted_connections();
    let graph = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
    let clone = graph.clone();
    assert_eq!(mock.get_number_of_received_commands(), 0);
    for handle in [&graph, &clone] {
        assert_eq!(
            handle
                .query("RETURN 42", &GraphQueryOptions::default())
                .await
                .unwrap()
                .rows,
            vec![vec![GraphValue::Integer(42)]]
        );
    }
    assert_eq!(mock.get_number_of_received_commands(), 2);
    assert_eq!(mock.get_number_of_accepted_connections(), connections);
    drop(graph);
    drop(clone);
    drop(client);
    mock.close().await;
}

#[tokio::test]
async fn successful_write_is_not_replayed_when_metadata_is_denied() {
    let node = Value::Array(vec![
        Value::Int(8),
        Value::Array(vec![
            Value::Int(0),
            Value::Array(vec![Value::Int(0)]),
            Value::Array(vec![]),
        ]),
    ]);
    let mock = server(HashMap::from([
        ("GRAPH.QUERY".into(), vec![result(node)]),
        (
            "GRAPH.RO_QUERY".into(),
            vec!["-NOPERM metadata denied\r\n".into()],
        ),
    ]));
    let client = client(&mock).await;
    let graph = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
    let error = graph
        .query("CREATE (n:Label) RETURN n", &GraphQueryOptions::default())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("metadata denied"), "{error}");
    assert_eq!(
        mock.get_number_of_received_commands(),
        2,
        "one write and one denied metadata read, never a repeated write"
    );
    drop(graph);
    drop(client);
    mock.close().await;
}

#[tokio::test]
async fn malformed_success_is_not_replayed() {
    let mock = server(HashMap::from([(
        "GRAPH.QUERY".into(),
        vec![result(Value::Array(vec![Value::Int(99), Value::Nil]))],
    )]));
    let client = client(&mock).await;
    let graph = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
    assert!(
        graph
            .query("CREATE () RETURN 1", &GraphQueryOptions::default())
            .await
            .is_err()
    );
    assert_eq!(mock.get_number_of_received_commands(), 1);
    drop(graph);
    drop(client);
    mock.close().await;
}

#[tokio::test]
async fn select_invalidates_existing_handles_even_when_database_is_unchanged() {
    let mock = server(HashMap::from([
        ("SELECT".into(), vec!["+OK\r\n".into()]),
        (
            "GRAPH.QUERY".into(),
            vec![result(Value::Array(vec![Value::Int(3), Value::Int(1)]))],
        ),
    ]));
    let mut client = client(&mock).await;
    let graph = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
    client
        .send_command(redis::cmd("SELECT").arg(0), None)
        .await
        .unwrap();
    assert!(
        graph
            .query("RETURN 1", &GraphQueryOptions::default())
            .await
            .is_err()
    );
    let fresh = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
    assert!(
        fresh
            .query("RETURN 1", &GraphQueryOptions::default())
            .await
            .is_ok()
    );
    assert_eq!(mock.get_number_of_received_commands(), 2);
    drop(graph);
    drop(fresh);
    drop(client);
    mock.close().await;
}

#[tokio::test]
async fn unsettled_select_and_pooled_clients_cannot_create_graph_handles() {
    let mock = server(HashMap::from([(
        "SELECT".into(),
        vec!["-ERR invalid database\r\n".into(), "+OK\r\n".into()],
    )]));
    let mut client = client(&mock).await;
    assert!(
        client
            .send_command(redis::cmd("SELECT").arg(-1), None)
            .await
            .is_err()
    );
    assert!(GraphContext::new(client.clone(), "graph".into(), 0).is_err());
    client
        .send_command(redis::cmd("SELECT").arg(0), None)
        .await
        .unwrap();
    assert!(GraphContext::new(client.clone(), "graph".into(), 0).is_ok());
    client.mark_pool_managed();
    let error = GraphContext::new(client.clone(), "graph".into(), 0)
        .err()
        .expect("pool-managed graph selection must fail");
    assert!(error.to_string().contains("pooled client lease"), "{error}");
    drop(client);
    mock.close().await;
}

#[tokio::test]
async fn graph_queries_never_queue_inside_raw_multi() {
    let mock = server(HashMap::from([
        ("MULTI".into(), vec!["+OK\r\n".into()]),
        ("DISCARD".into(), vec!["+OK\r\n".into()]),
        (
            "GRAPH.QUERY".into(),
            vec![result(Value::Array(vec![Value::Int(3), Value::Int(42)]))],
        ),
    ]));
    let mut client = client(&mock).await;
    let graph = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
    client
        .send_command(&mut redis::cmd("MULTI"), None)
        .await
        .unwrap();
    let error = graph
        .query("CREATE () RETURN 42", &GraphQueryOptions::default())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("raw MULTI"), "{error}");
    assert_eq!(mock.get_number_of_received_commands(), 1);
    client
        .send_command(&mut redis::cmd("DISCARD"), None)
        .await
        .unwrap();
    assert_eq!(
        graph
            .query("RETURN 42", &GraphQueryOptions::default())
            .await
            .unwrap()
            .rows,
        vec![vec![GraphValue::Integer(42)]]
    );
    drop(graph);
    drop(client);
    mock.close().await;
}

#[tokio::test]
async fn queued_select_does_not_confirm_a_database_change() {
    let mock = server(HashMap::from([
        ("MULTI".into(), vec!["+OK\r\n".into()]),
        (
            "SELECT".into(),
            vec!["+QUEUED\r\n".into(), "+OK\r\n".into()],
        ),
        ("DISCARD".into(), vec!["+OK\r\n".into()]),
    ]));
    let mut client = client(&mock).await;
    client
        .send_command(&mut redis::cmd("MULTI"), None)
        .await
        .unwrap();
    client
        .send_command(redis::cmd("SELECT").arg(3), None)
        .await
        .unwrap();
    assert_eq!(client.current_database(), 0);
    client
        .send_command(&mut redis::cmd("DISCARD"), None)
        .await
        .unwrap();
    assert_eq!(client.current_database(), 0);
    assert!(GraphContext::new(client.clone(), "graph".into(), 0).is_err());
    client
        .send_command(redis::cmd("SELECT").arg(0), None)
        .await
        .unwrap();
    assert!(GraphContext::new(client.clone(), "graph".into(), 0).is_ok());
    drop(client);
    mock.close().await;
}

#[tokio::test]
async fn successful_batch_reset_confirms_database_zero() {
    let mock = server(HashMap::from([
        ("SELECT".into(), vec!["+OK\r\n".into()]),
        ("RESET".into(), vec!["+RESET\r\n".into()]),
    ]));
    let mut client = client(&mock).await;
    let mut pipeline = redis::pipe();
    pipeline.cmd("SELECT").arg(3).cmd("RESET");
    client
        .send_pipeline(&pipeline, None, true, None, Default::default())
        .await
        .unwrap();
    assert_eq!(client.current_database(), 0);
    assert_eq!(client.graph_database_epoch() & 1, 0);
    assert!(GraphContext::new(client.clone(), "graph".into(), 0).is_ok());
    drop(client);
    mock.close().await;
}

#[tokio::test]
async fn successful_batch_database_changes_survive_later_command_errors() {
    for atomic in [false, true] {
        for reset in [false, true] {
            if atomic && reset {
                continue;
            }
            for raise_on_error in [false, true] {
                let mut replies = HashMap::from([
                    (
                        "SELECT".into(),
                        vec![if atomic { "+QUEUED\r\n" } else { "+OK\r\n" }.into()],
                    ),
                    (
                        "RESET".into(),
                        vec![if atomic { "+QUEUED\r\n" } else { "+RESET\r\n" }.into()],
                    ),
                    (
                        "INCR".into(),
                        vec![
                            if atomic {
                                "+QUEUED\r\n"
                            } else {
                                "-ERR test integer failure\r\n"
                            }
                            .into(),
                        ],
                    ),
                ]);
                if atomic {
                    replies.insert("MULTI".into(), vec!["+OK\r\n".into()]);
                    replies.insert(
                        "EXEC".into(),
                        vec!["*2\r\n+OK\r\n-ERR test integer failure\r\n".into()],
                    );
                }
                let mock = server(replies);
                let mut client = client(&mock).await;
                let mut pipeline = redis::pipe();
                if atomic {
                    pipeline.atomic();
                }
                pipeline.cmd("SELECT").arg(3);
                if reset {
                    pipeline.cmd("RESET");
                }
                pipeline.cmd("INCR").arg("not-an-integer");
                let result = if atomic {
                    client
                        .send_transaction(&pipeline, None, None, raise_on_error)
                        .await
                } else {
                    client
                        .send_pipeline(&pipeline, None, raise_on_error, None, Default::default())
                        .await
                };
                assert_eq!(
                    result.is_err(),
                    raise_on_error,
                    "atomic={atomic}, reset={reset}: {result:?}"
                );
                let database = if reset { 0 } else { 3 };
                assert_eq!(client.current_database(), database);
                assert_eq!(client.graph_database_epoch() & 1, 0);
                assert!(GraphContext::new(client.clone(), "graph".into(), database).is_ok());
                drop(client);
                mock.close().await;
            }
        }
    }
}

#[tokio::test]
async fn reset_cancels_atomic_batch_and_leaves_database_unsettled_until_reconfirmed() {
    for raise_on_error in [false, true] {
        let mock = server(HashMap::from([
            ("SELECT".into(), vec!["+OK\r\n".into(), "+OK\r\n".into()]),
            ("MULTI".into(), vec!["+OK\r\n".into()]),
            ("RESET".into(), vec!["+RESET\r\n".into()]),
            ("EXEC".into(), vec!["-ERR EXEC without MULTI\r\n".into()]),
        ]));
        let mut client = client(&mock).await;
        client
            .send_command(redis::cmd("SELECT").arg(3), None)
            .await
            .unwrap();
        let old = GraphContext::new(client.clone(), "graph".into(), 3).unwrap();
        let mut pipeline = redis::pipe();
        pipeline.atomic().cmd("RESET");
        let error = client
            .send_transaction(&pipeline, None, None, raise_on_error)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("EXEC without MULTI"), "{error}");
        assert_eq!(client.graph_database_epoch() & 1, 1);
        assert!(GraphContext::new(client.clone(), "graph".into(), 0).is_err());
        assert!(
            old.query("RETURN 42", &GraphQueryOptions::default())
                .await
                .is_err()
        );
        client
            .send_command(redis::cmd("SELECT").arg(0), None)
            .await
            .unwrap();
        assert_eq!(client.current_database(), 0);
        assert_eq!(client.graph_database_epoch() & 1, 0);
        assert!(GraphContext::new(client.clone(), "graph".into(), 0).is_ok());
        drop(old);
        drop(client);
        mock.close().await;
    }
}

#[tokio::test]
async fn state_changes_wait_for_the_whole_graph_operation() {
    for (command, pipeline) in [
        ("SELECT", false),
        ("MULTI", false),
        ("SELECT", true),
        ("MULTI", true),
    ] {
        let (client, observed, server) = delayed_client_with_timeout(2000).await;
        let graph = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
        let operation =
            tokio::spawn(
                async move { graph.query("RETURN n", &GraphQueryOptions::default()).await },
            );
        tokio::time::timeout(Duration::from_secs(2), async {
            while observed.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut changing_client = client.clone();
        let change = tokio::spawn(async move {
            let mut cmd = redis::cmd(command);
            if command == "SELECT" {
                cmd.arg(0);
            }
            if pipeline {
                let mut batch = redis::pipe();
                batch.add_command(cmd);
                changing_client
                    .send_pipeline(&batch, None, true, None, Default::default())
                    .await
            } else {
                changing_client.send_command(&mut cmd, None).await
            }
        });
        let result = operation.await.unwrap().unwrap();
        assert!(matches!(&result.rows[0][0], GraphValue::Node(node) if node.labels == ["Label"]));
        change.await.unwrap().unwrap();
        assert_eq!(observed.load(Ordering::Acquire), 2);
        drop(client);
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn repeated_entities_fetch_each_dictionary_only_once() {
    let mock = server(HashMap::from([
        (
            "GRAPH.QUERY".into(),
            vec![result(Value::Array(vec![
                Value::Int(6),
                Value::Array(vec![node(); 256]),
            ]))],
        ),
        (
            "GRAPH.RO_QUERY".into(),
            vec![result(Value::Array(vec![Value::Int(2), text("Label")]))],
        ),
    ]));
    let client = client(&mock).await;
    let graph = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
    let result = graph
        .query("MATCH (n) RETURN collect(n)", &GraphQueryOptions::default())
        .await
        .unwrap();
    let GraphValue::List(nodes) = &result.rows[0][0] else {
        panic!("typed list expected")
    };
    assert_eq!(nodes.len(), 256);
    for node in nodes {
        let GraphValue::Node(node) = node else {
            panic!("typed node expected")
        };
        assert_eq!(node.labels, ["Label"]);
    }
    assert_eq!(mock.get_number_of_received_commands(), 2);
    drop(graph);
    drop(client);
    mock.close().await;
}

#[tokio::test]
async fn query_and_metadata_share_one_deadline_and_release_inflight_slots() {
    let (client, commands, server) = delayed_client().await;
    let graph = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
    let error = graph
        .query("RETURN n", &GraphQueryOptions::default())
        .await
        .unwrap_err();
    assert!(error.is_timeout(), "{error}");
    assert_eq!(commands.load(Ordering::Acquire), 2);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(client.available_inflight_count(), 1);
    assert_eq!(commands.load(Ordering::Acquire), 2, "no query replay");
    drop(graph);
    drop(client);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn cancelling_a_graph_operation_does_not_leak_or_replay() {
    let (client, commands, server) = delayed_client().await;
    let graph = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
    let cancelled = graph.clone();
    let query = tokio::spawn(async move {
        cancelled
            .query("RETURN 42", &GraphQueryOptions::default())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while commands.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    query.abort();
    assert!(query.await.unwrap_err().is_cancelled());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(commands.load(Ordering::Acquire), 1);
    assert_eq!(client.available_inflight_count(), 1);
    assert_eq!(
        graph
            .query("RETURN 42", &GraphQueryOptions::default())
            .await
            .unwrap()
            .rows,
        vec![vec![GraphValue::Integer(42)]]
    );
    assert_eq!(commands.load(Ordering::Acquire), 2);
    drop(graph);
    drop(client);
    server.abort();
    let _ = server.await;
}

#[cfg(feature = "proto")]
#[tokio::test]
async fn native_context_requests_cannot_change_selection() {
    use glide_core::graph::execute_request_with_context;
    use glide_core::graph_request::QueryRequest;
    use protobuf::Message;

    let mock = server(HashMap::from([(
        "GRAPH.QUERY".into(),
        vec![result(Value::Array(vec![Value::Int(3), Value::Int(42)]))],
    )]));
    let client = client(&mock).await;
    let graph = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
    let mut request = QueryRequest {
        graph_name: "other".into(),
        query: "RETURN 42".into(),
        ..Default::default()
    };
    assert!(
        execute_request_with_context(graph.clone(), &request.write_to_bytes().unwrap())
            .await
            .is_err()
    );
    request.graph_name = "graph".into();
    request.database = 1;
    assert!(
        execute_request_with_context(graph.clone(), &request.write_to_bytes().unwrap())
            .await
            .is_err()
    );
    request.database = 0;
    request.database_epoch = 2;
    assert!(
        execute_request_with_context(graph.clone(), &request.write_to_bytes().unwrap())
            .await
            .is_err()
    );
    assert_eq!(mock.get_number_of_received_commands(), 0);
    request.database_epoch = 0;
    assert!(
        execute_request_with_context(graph.clone(), &request.write_to_bytes().unwrap())
            .await
            .is_ok()
    );
    assert_eq!(mock.get_number_of_received_commands(), 1);
    drop(graph);
    drop(client);
    mock.close().await;
}

#[tokio::test]
async fn unsupported_module_error_is_preserved_without_metadata_or_replay() {
    let mock = server(HashMap::from([(
        "GRAPH.QUERY".into(),
        vec!["-ERR unknown command 'GRAPH.QUERY'\r\n".into()],
    )]));
    let client = client(&mock).await;
    let graph = GraphContext::new(client.clone(), "graph".into(), 0).unwrap();
    let error = graph
        .query("RETURN 1", &GraphQueryOptions::default())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("unknown command 'GRAPH.QUERY'"),
        "{error}"
    );
    assert_eq!(mock.get_number_of_received_commands(), 1);
    drop(graph);
    drop(client);
    mock.close().await;
}
