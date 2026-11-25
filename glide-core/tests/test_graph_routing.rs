// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Routing of raw FalkorDB `GRAPH.*` commands, against real FalkorDB servers.
//!
//! The tests are ignored by default: they need the servers that
//! `utils/start-falkordb-docker.sh` starts, a standalone primary with one replica
//! and a cluster of 3 primaries with 1 replica each:
//!
//! ```sh
//! IMAGE=falkordb/falkordb:6.2.1 utils/start-falkordb-docker.sh
//! source utils/.falkordb-endpoints.env
//! cd glide-core && cargo test --features proto --test test_graph_routing -- --ignored
//! ```
//!
//! They target FalkorDB 6.x on Valkey only. Whether a command reached its
//! node directly is read from `INFO commandstats`: a MOVED reply counts there as a
//! rejected call.

mod constants;
mod utilities;

#[cfg(test)]
mod graph_routing {
    use super::utilities::*;
    use glide_core::client::Client;
    use glide_core::connection_request::{ProtocolVersion, ReadFrom};
    use redis::cluster_topology::get_slot;
    use redis::{Cmd, ConnectionAddr, ErrorKind, RedisConnectionInfo, Value};
    use rstest::rstest;
    use std::time::Duration;

    const IGNORE_REASON: &str = "needs FalkorDB servers: run utils/start-falkordb-docker.sh, then source its endpoints file";
    const TEST_TIMEOUT: Duration = Duration::from_secs(60);

    /// The FalkorDB test server used by every test in this file.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Servers {
        /// A primary and its replica, in that order.
        Standalone,
        /// 3 primaries with 1 replica each.
        Cluster,
    }

    fn endpoints(servers: Servers) -> Vec<(String, u16)> {
        let variable = match servers {
            Servers::Standalone => "GRAPH_STANDALONE_ENDPOINTS",
            Servers::Cluster => "GRAPH_CLUSTER_ENDPOINTS",
        };
        let value = std::env::var(variable)
            .unwrap_or_else(|_| panic!("{variable} is not set; this test {IGNORE_REASON}"));
        value
            .split(',')
            .map(|endpoint| {
                let (host, port) = endpoint.rsplit_once(':').expect("host:port");
                (host.to_string(), port.parse().expect("port"))
            })
            .collect()
    }

    fn protocol_version(protocol: i64) -> ProtocolVersion {
        match protocol {
            2 => ProtocolVersion::RESP2,
            3 => ProtocolVersion::RESP3,
            _ => panic!("unknown protocol {protocol}"),
        }
    }

    async fn connect(
        servers: Servers,
        protocol: i64,
        read_from: Option<ReadFrom>,
        read_only: bool,
    ) -> Client {
        let addresses: Vec<ConnectionAddr> = endpoints(servers)
            .into_iter()
            .map(|(host, port)| ConnectionAddr::Tcp(host, port))
            .collect();
        let configuration = TestConfiguration {
            cluster_mode: if servers == Servers::Cluster {
                ClusterMode::Enabled
            } else {
                ClusterMode::Disabled
            },
            protocol: protocol_version(protocol),
            connection_info: Some(RedisConnectionInfo {
                protocol: match protocol {
                    2 => redis::ProtocolVersion::RESP2,
                    _ => redis::ProtocolVersion::RESP3,
                },
                ..Default::default()
            }),
            read_from,
            request_timeout: Some(5_000),
            ..Default::default()
        };
        let mut request = create_connection_request(&addresses, &configuration);
        if read_only {
            request.read_only = Some(true);
        }
        Client::new(request.into(), None)
            .await
            .expect("connect to the FalkorDB test servers")
    }

    fn graph(args: &[&str]) -> Cmd {
        let mut command = redis::cmd(args[0]);
        for arg in &args[1..] {
            command.arg(*arg);
        }
        command
    }

    async fn send(client: &mut Client, args: &[&str]) -> redis::RedisResult<Value> {
        client.send_command(&mut graph(args), None).await
    }

    /// Sends `args` straight to the node at `endpoint`, over a connection of its
    /// own: a standalone GLIDE client ignores by-address routing.
    fn send_to((host, port): &(String, u16), args: &[&str]) -> Value {
        let node = redis::Client::open(format!("redis://{host}:{port}/")).unwrap();
        let mut connection = node.get_connection(None).unwrap();
        graph(args)
            .query(&mut connection)
            .unwrap_or_else(|error| panic!("{args:?} on {host}:{port}: {error}"))
    }

    fn text(value: Value) -> String {
        match value {
            Value::BulkString(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Value::VerbatimString { text, .. } => text,
            Value::SimpleString(text) => text,
            other => panic!("expected a string, got {other:?}"),
        }
    }

    /// Whether the node at `endpoint` is a replica.
    fn is_replica(endpoint: &(String, u16)) -> bool {
        match send_to(endpoint, &["ROLE"]) {
            Value::Array(parts) => matches!(
                parts.into_iter().next().map(text).as_deref(),
                Some("slave" | "replica")
            ),
            other => panic!("unexpected ROLE reply {other:?}"),
        }
    }

    /// `(calls, rejected_calls)` for `command` on the node at `endpoint`.
    fn command_stats(endpoint: &(String, u16), command: &str) -> (u64, u64) {
        let info = text(send_to(endpoint, &["INFO", "commandstats"]));
        let prefix = format!("cmdstat_{}:", command.to_ascii_lowercase());
        let Some(line) = info
            .lines()
            .find(|line| line.to_ascii_lowercase().starts_with(&prefix))
        else {
            return (0, 0);
        };
        let field = |name: &str| -> u64 {
            line[prefix.len()..]
                .split(',')
                .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
                .map(|value| value.parse().unwrap())
                .unwrap_or(0)
        };
        (field("calls"), field("rejected_calls"))
    }

    fn reset_stats(servers: Servers) {
        for endpoint in endpoints(servers) {
            send_to(&endpoint, &["CONFIG", "RESETSTAT"]);
        }
    }

    /// The shard of the test cluster that owns `key`. `--cluster create` with 3
    /// primaries splits the slots as 0-5460, 5461-10922 and 10923-16383.
    fn shard_of(key: &str) -> usize {
        match get_slot(key.as_bytes()) {
            0..=5460 => 0,
            5461..=10922 => 1,
            _ => 2,
        }
    }

    /// One fresh key per shard of the 3-shard test cluster, each with `tag` in its name.
    fn key_per_shard(tag: &str) -> Vec<String> {
        let mut keys: Vec<Option<String>> = vec![None; 3];
        let mut i = 0;
        while keys.iter().any(Option::is_none) {
            let key = format!("graph-routing-{tag}-{i}");
            keys[shard_of(&key)].get_or_insert(key);
            i += 1;
        }
        keys.into_iter().map(Option::unwrap).collect()
    }

    #[rstest]
    #[ignore = "needs FalkorDB servers: run utils/start-falkordb-docker.sh"]
    #[serial_test::serial]
    #[timeout(TEST_TIMEOUT)]
    fn test_graph_constraint_and_memory_reach_the_key_owner(#[values(2, 3)] protocol: i64) {
        block_on_all(async {
            let mut client = connect(Servers::Cluster, protocol, None, false).await;
            let key = format!("graph-routing-constraint-{}", generate_random_string(8));
            send(
                &mut client,
                &["GRAPH.QUERY", &key, "CREATE INDEX FOR (p:P) ON (p.v)"],
            )
            .await
            .unwrap();
            reset_stats(Servers::Cluster);

            let constraint = [
                "GRAPH.CONSTRAINT",
                "CREATE",
                key.as_str(),
                "UNIQUE",
                "NODE",
                "P",
                "PROPERTIES",
                "1",
                "v",
            ];
            send(&mut client, &constraint).await.unwrap();
            send(&mut client, &["GRAPH.MEMORY", "USAGE", &key])
                .await
                .unwrap();

            for command in ["graph.CONSTRAINT", "graph.MEMORY"] {
                let (mut calls, mut rejected) = (0, 0);
                for endpoint in endpoints(Servers::Cluster) {
                    let (c, r) = command_stats(&endpoint, command);
                    calls += c;
                    rejected += r;
                }
                assert_eq!((calls, rejected), (1, 0), "{command}: (calls, MOVED)");
            }
            send(&mut client, &["GRAPH.DELETE", &key]).await.unwrap();
        });
    }

    #[rstest]
    #[ignore = "needs FalkorDB servers: run utils/start-falkordb-docker.sh"]
    #[serial_test::serial]
    #[timeout(TEST_TIMEOUT)]
    fn test_graph_list_merges_every_shard(#[values(2, 3)] protocol: i64) {
        block_on_all(async {
            let mut client = connect(Servers::Cluster, protocol, None, false).await;
            let keys = key_per_shard(&generate_random_string(8));
            for key in &keys {
                send(&mut client, &["GRAPH.QUERY", key, "CREATE ()"])
                    .await
                    .unwrap();
            }

            let listed: Vec<String> = match send(&mut client, &["GRAPH.LIST"]).await.unwrap() {
                Value::Array(names) => names.into_iter().map(text).collect(),
                other => panic!("expected an array, got {other:?}"),
            };
            for key in &keys {
                assert!(listed.contains(key), "{key} missing from {listed:?}");
            }
            for key in &keys {
                send(&mut client, &["GRAPH.DELETE", key]).await.unwrap();
            }
        });
    }

    #[rstest]
    #[ignore = "needs FalkorDB servers: run utils/start-falkordb-docker.sh"]
    #[serial_test::serial]
    #[timeout(TEST_TIMEOUT)]
    fn test_graph_udf_reaches_every_primary(#[values(2, 3)] protocol: i64) {
        block_on_all(async {
            let mut client = connect(Servers::Cluster, protocol, None, false).await;
            let library = format!("lib{}", generate_random_string(8));
            let code = "function inc(x) { return x + 1; } falkor.register('inc', inc);";
            let call = format!("RETURN {library}.inc(1)");
            let keys = key_per_shard(&library);

            send(&mut client, &["GRAPH.UDF", "LOAD", &library, code])
                .await
                .unwrap();
            for key in &keys {
                let reply = send(&mut client, &["GRAPH.QUERY", key, &call]).await;
                assert!(reply.is_ok(), "{key}: {reply:?}");
            }

            // FalkorDB 6 replicas reject GRAPH.UDF; LIST must still reach a primary.
            let mut replica_reader = connect(
                Servers::Cluster,
                protocol,
                Some(ReadFrom::PreferReplica),
                false,
            )
            .await;
            for _ in 0..20 {
                let listed = send(&mut replica_reader, &["GRAPH.UDF", "LIST"]).await;
                assert!(listed.is_ok(), "{listed:?}");
            }

            send(&mut client, &["GRAPH.UDF", "DELETE", &library])
                .await
                .unwrap();
            for key in &keys {
                let reply = send(&mut client, &["GRAPH.QUERY", key, &call]).await;
                assert!(reply.is_err(), "{key}: the library is still loaded");
                send(&mut client, &["GRAPH.DELETE", key]).await.unwrap();
            }
        });
    }

    #[rstest]
    #[ignore = "needs FalkorDB servers: run utils/start-falkordb-docker.sh"]
    #[serial_test::serial]
    #[timeout(TEST_TIMEOUT)]
    fn test_graph_config_set_reaches_every_node(#[values(2, 3)] protocol: i64) {
        block_on_all(async {
            let mut client = connect(Servers::Cluster, protocol, None, false).await;
            let get = ["GRAPH.CONFIG", "GET", "RESULTSET_SIZE"];
            // GRAPH.CONFIG GET <name> replies [name, value]; the value is an integer.
            let config_value = |reply: Value| -> i64 {
                match reply {
                    Value::Array(mut pair) if pair.len() == 2 => match pair.pop().unwrap() {
                        Value::Int(value) => value,
                        other => text(other).parse().unwrap(),
                    },
                    other => panic!("unexpected GRAPH.CONFIG GET reply {other:?}"),
                }
            };
            let original = config_value(send(&mut client, &get).await.unwrap()).to_string();
            let new_value = if original == "4321" { "4322" } else { "4321" };

            send(
                &mut client,
                &["GRAPH.CONFIG", "SET", "RESULTSET_SIZE", new_value],
            )
            .await
            .unwrap();
            for endpoint in endpoints(Servers::Cluster) {
                let value = config_value(send_to(&endpoint, &get));
                assert_eq!(value.to_string(), new_value, "{endpoint:?}");
            }

            send(
                &mut client,
                &["GRAPH.CONFIG", "SET", "RESULTSET_SIZE", &original],
            )
            .await
            .unwrap();
        });
    }

    #[rstest]
    #[ignore = "needs FalkorDB servers: run utils/start-falkordb-docker.sh"]
    #[serial_test::serial]
    #[timeout(TEST_TIMEOUT)]
    fn test_graph_ro_query_follows_read_from(
        #[values(2, 3)] protocol: i64,
        #[values(Servers::Standalone, Servers::Cluster)] servers: Servers,
    ) {
        block_on_all(async {
            let mut client = connect(servers, protocol, None, false).await;
            let key = format!("graph-routing-ro-{}", generate_random_string(8));
            send(&mut client, &["GRAPH.QUERY", &key, "CREATE (:A)"])
                .await
                .unwrap();
            send(&mut client, &["WAIT", "1", "1000"]).await.unwrap();
            let ro_query = [
                "GRAPH.RO_QUERY",
                key.as_str(),
                "MATCH (a:A) RETURN count(a)",
            ];

            for (read_from, from_replicas) in [(None, false), (Some(ReadFrom::PreferReplica), true)]
            {
                let mut reader = connect(servers, protocol, read_from, false).await;
                reset_stats(servers);
                for _ in 0..10 {
                    send(&mut reader, &ro_query).await.unwrap();
                }
                let (mut primary_calls, mut replica_calls) = (0, 0);
                for endpoint in endpoints(servers) {
                    let (calls, _) = command_stats(&endpoint, "graph.RO_QUERY");
                    if is_replica(&endpoint) {
                        replica_calls += calls;
                    } else {
                        primary_calls += calls;
                    }
                }
                let expected = if from_replicas { (0, 10) } else { (10, 0) };
                assert_eq!(
                    (primary_calls, replica_calls),
                    expected,
                    "read_from {read_from:?}: (primary, replica) calls"
                );
            }
            send(&mut client, &["GRAPH.DELETE", &key]).await.unwrap();
        });
    }

    #[rstest]
    #[ignore = "needs FalkorDB servers: run utils/start-falkordb-docker.sh"]
    #[serial_test::serial]
    #[timeout(TEST_TIMEOUT)]
    fn test_graph_copy_needs_both_keys_in_one_slot(#[values(2, 3)] protocol: i64) {
        block_on_all(async {
            let mut client = connect(Servers::Cluster, protocol, None, false).await;
            let tag = generate_random_string(8);
            let source = format!("{{{tag}}}source");
            let copy = format!("{{{tag}}}copy");
            send(&mut client, &["GRAPH.QUERY", &source, "CREATE ()"])
                .await
                .unwrap();

            send(&mut client, &["GRAPH.COPY", &source, &copy])
                .await
                .unwrap();
            let other_slot = format!("{{{tag}-other}}copy");
            let error = send(&mut client, &["GRAPH.COPY", &source, &other_slot])
                .await
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::CrossSlot, "{error}");

            for key in [&source, &copy] {
                send(&mut client, &["GRAPH.DELETE", key]).await.unwrap();
            }
        });
    }

    #[rstest]
    #[ignore = "needs FalkorDB servers: run utils/start-falkordb-docker.sh"]
    #[serial_test::serial]
    #[timeout(TEST_TIMEOUT)]
    fn test_graph_read_only_standalone_client(#[values(2, 3)] protocol: i64) {
        block_on_all(async {
            let mut writer = connect(Servers::Standalone, protocol, None, false).await;
            let key = format!("graph-routing-read-only-{}", generate_random_string(8));
            send(&mut writer, &["GRAPH.QUERY", &key, "CREATE (:A)"])
                .await
                .unwrap();
            send(&mut writer, &["WAIT", "1", "1000"]).await.unwrap();

            let mut reader = connect(Servers::Standalone, protocol, None, true).await;
            for read in [
                vec![
                    "GRAPH.RO_QUERY",
                    key.as_str(),
                    "MATCH (a:A) RETURN count(a)",
                ],
                vec!["GRAPH.LIST"],
                vec!["GRAPH.CONFIG", "GET", "RESULTSET_SIZE"],
            ] {
                let reply = send(&mut reader, &read).await;
                assert!(reply.is_ok(), "{read:?}: {reply:?}");
            }

            // Rejected by the client before it is sent.
            reset_stats(Servers::Standalone);
            let error = send(&mut reader, &["GRAPH.QUERY", &key, "CREATE (:A)"])
                .await
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ReadOnly, "{error}");
            for endpoint in endpoints(Servers::Standalone) {
                let (calls, rejected) = command_stats(&endpoint, "graph.QUERY");
                assert_eq!((calls, rejected), (0, 0), "{endpoint:?}");
            }
            send(&mut writer, &["GRAPH.DELETE", &key]).await.unwrap();
        });
    }

    #[test]
    fn test_key_per_shard_covers_three_slot_ranges() {
        let keys = key_per_shard("check");
        let shards: Vec<usize> = keys.iter().map(|key| shard_of(key)).collect();
        assert_eq!(shards, vec![0, 1, 2]);
    }
}
