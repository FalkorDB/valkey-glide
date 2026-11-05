// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Opt-in tests of GraphContext against the dedicated FalkorDB harness.
//!
//! Run with FALKORDB_STANDALONE_ENDPOINT, FALKORDB_REPLICA_ENDPOINT and
//! FALKORDB_CLUSTER_ENDPOINT exported:
//! cargo test --test test_graph_topology -- --ignored --test-threads=1
//!
//! These tests never flush databases or restart servers. Cluster tests reserve
//! an empty random slot, migrate only their reservation key, and restore its
//! original owner. The replica test briefly pauses replication writes and
//! unpauses them even if an assertion fails; run this target separately from
//! other tests that require the same replica to be current. Actual primary
//! failover additionally requires FALKORDB_TEST_FAILOVER=1 and an exclusive
//! harness; it promotes one replica and restores the original primary.

use futures::FutureExt;
use glide_core::client::{
    AuthenticationInfo, Client, ConnectionRequest, MonitorClient, MonitorLine, NodeAddress,
    PeriodicCheck, ReadFrom, TlsMode,
};
use glide_core::graph::{GraphContext, GraphQueryOptions, GraphQueryResult, GraphValue};
use redis::aio::MultiplexedConnection;
use redis::{ErrorKind, GlideConnectionOptions, ProtocolVersion, RedisConnectionInfo, Value};
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout};
use uuid::Uuid;

const CREATE_PATH: &str =
    "CREATE p=(a:Source {node_key:1})-[r:EDGE {edge_key:2}]->(b:Destination) RETURN p";
const PROTOCOLS: [ProtocolVersion; 2] = [ProtocolVersion::RESP2, ProtocolVersion::RESP3];

fn endpoint(variable: &str) -> NodeAddress {
    let value = std::env::var(variable).unwrap_or_else(|_| {
        panic!("{variable} is required; start utils/falkordb_harness.py first")
    });
    let (host, port) = value.rsplit_once(':').expect("endpoint must be host:port");
    NodeAddress {
        host: host.trim_start_matches('[').trim_end_matches(']').into(),
        port: port.parse().expect("endpoint port must be numeric"),
        unix_socket_path: None,
    }
}

fn unique_name(prefix: &str) -> String {
    format!("glide-graph-topology:{prefix}:{{{}}}", Uuid::new_v4())
}

async fn admin(address: &NodeAddress) -> MultiplexedConnection {
    redis::Client::open((address.host.as_str(), address.port))
        .unwrap()
        .get_multiplexed_async_connection(GlideConnectionOptions::default())
        .await
        .expect("dedicated FalkorDB endpoint must be reachable")
}

async fn attempt(
    connection: &mut MultiplexedConnection,
    arguments: &[&str],
) -> redis::RedisResult<Value> {
    let mut command = redis::cmd(arguments[0]);
    command.arg(&arguments[1..]);
    timeout(Duration::from_secs(10), command.query_async(connection))
        .await
        .expect("admin command exceeded its deadline")
}

async fn run(connection: &mut MultiplexedConnection, arguments: &[&str]) -> Value {
    attempt(connection, arguments)
        .await
        .unwrap_or_else(|error| panic!("{}", admin_failure_message(&error)))
}

fn admin_failure_message(error: &redis::RedisError) -> String {
    format!("admin command failed: {:?}", error.kind())
}

#[test]
fn admin_failures_do_not_log_server_credentials() {
    let error = redis::RedisError::from((
        ErrorKind::ResponseError,
        "ACL failure",
        "private-test-username private-test-password".to_owned(),
    ));
    assert!(error.to_string().contains("private-test-password"));
    assert_eq!(
        admin_failure_message(&error),
        "admin command failed: ResponseError"
    );
}

fn text(value: &Value) -> String {
    redis::from_redis_value(value).expect("expected text")
}

fn elements(value: &Value) -> &[Value] {
    match value {
        Value::Array(values) => values,
        other => panic!("expected array, got {other:?}"),
    }
}

async fn core_client(
    addresses: Vec<NodeAddress>,
    cluster: bool,
    protocol: ProtocolVersion,
    read_from: ReadFrom,
    name: String,
) -> Client {
    Client::new(
        ConnectionRequest {
            addresses,
            cluster_mode_enabled: cluster,
            protocol: Some(protocol),
            read_from: Some(read_from),
            client_name: Some(name),
            periodic_checks: Some(PeriodicCheck::Disabled),
            request_timeout: Some(5000),
            connection_timeout: Some(5000),
            inflight_requests_limit: Some(1),
            ..Default::default()
        },
        None,
    )
    .await
    .expect("core client must connect to dedicated FalkorDB")
}

fn assert_path(result: &GraphQueryResult) {
    assert_eq!(result.columns, ["p"]);
    assert_eq!(result.rows.len(), 1);
    let GraphValue::Path(path) = &result.rows[0][0] else {
        panic!("expected resolved GraphValue::Path: {result:?}");
    };
    assert_eq!(path.nodes.len(), 2);
    assert_eq!(path.relationships.len(), 1);
    assert_eq!(path.nodes[0].labels, ["Source"]);
    assert_eq!(path.nodes[1].labels, ["Destination"]);
    assert_eq!(
        path.nodes[0].properties,
        [("node_key".into(), GraphValue::Integer(1))]
    );
    assert_eq!(path.relationships[0].r#type, "EDGE");
    assert_eq!(
        path.relationships[0].properties,
        [("edge_key".into(), GraphValue::Integer(2))]
    );
    assert_eq!(path.relationships[0].source_id, path.nodes[0].id);
    assert_eq!(path.relationships[0].destination_id, path.nodes[1].id);
}

#[derive(Clone, Debug)]
struct ObservedCommand {
    client_addr: String,
    command: String,
    args: Vec<String>,
}

impl From<MonitorLine> for ObservedCommand {
    fn from(line: MonitorLine) -> Self {
        Self {
            client_addr: line.client_addr,
            command: line.command,
            args: line.args,
        }
    }
}

struct Observation {
    monitor: MonitorClient,
    lines: Arc<Mutex<Vec<ObservedCommand>>>,
}

impl Observation {
    async fn start(address: &NodeAddress) -> Self {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&lines);
        let monitor = MonitorClient::new(
            address,
            RedisConnectionInfo {
                protocol: ProtocolVersion::RESP2,
                ..Default::default()
            },
            TlsMode::NoTls,
            Arc::new(move |line| captured.lock().unwrap().push(line.into())),
        )
        .await
        .expect("MONITOR must be available on the dedicated harness");
        Self { monitor, lines }
    }

    fn snapshot(&self) -> Vec<ObservedCommand> {
        self.lines.lock().unwrap().clone()
    }

    async fn assert_graph_affinity(&self, key: &str, command: &str, query: &str, asking: bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let lines = loop {
            let lines = self.snapshot();
            let count = lines
                .iter()
                .filter(|line| {
                    line.command.to_ascii_uppercase().starts_with("GRAPH.")
                        && line.args.first().is_some_and(|argument| argument == key)
                })
                .count();
            if count >= 4 {
                break lines;
            }
            assert!(
                Instant::now() < deadline,
                "missing graph MONITOR lines: {lines:?}"
            );
            sleep(Duration::from_millis(10)).await;
        };
        let graph_lines: Vec<_> = lines
            .iter()
            .filter(|line| {
                line.command.to_ascii_uppercase().starts_with("GRAPH.")
                    && line.args.first().is_some_and(|argument| argument == key)
            })
            .collect();
        assert_eq!(graph_lines.len(), 4, "query must execute exactly once");
        let address = &graph_lines[0].client_addr;
        assert!(graph_lines.iter().all(|line| &line.client_addr == address));
        assert_eq!(graph_lines[0].command.to_ascii_uppercase(), command);
        assert_eq!(graph_lines[0].args[1], query);
        assert!(
            graph_lines[1..]
                .iter()
                .all(|line| line.command.eq_ignore_ascii_case("GRAPH.RO_QUERY")),
            "dictionary reads must not require GRAPH.QUERY permission"
        );
        assert_eq!(
            graph_lines[1..]
                .iter()
                .map(|line| line.args[1].as_str())
                .collect::<Vec<_>>(),
            [
                "CALL db.labels()",
                "CALL db.relationshipTypes()",
                "CALL db.propertyKeys()"
            ]
        );
        if asking {
            let connection_lines: Vec<_> = lines
                .iter()
                .filter(|line| &line.client_addr == address)
                .collect();
            for (index, line) in connection_lines.iter().enumerate() {
                if line.command.to_ascii_uppercase().starts_with("GRAPH.") {
                    assert!(index > 0, "graph command had no preceding ASKING");
                    assert_eq!(
                        connection_lines[index - 1].command.to_ascii_uppercase(),
                        "ASKING",
                        "ASKING and each query/metadata command must stay adjacent"
                    );
                }
            }
        }
    }
}

#[derive(Clone)]
struct Master {
    address: NodeAddress,
    id: String,
    connection: MultiplexedConnection,
}

struct SlotLease {
    key: String,
    reservation: String,
    slot: String,
    masters: Vec<Master>,
    source: usize,
    target: usize,
}

impl SlotLease {
    async fn reserve(seed: &NodeAddress) -> Self {
        Self::reserve_excluding(seed, &HashSet::new()).await
    }

    async fn reserve_excluding(seed: &NodeAddress, excluded_owners: &HashSet<String>) -> Self {
        let mut seed_connection = admin(seed).await;
        let slots = run(&mut seed_connection, &["CLUSTER", "SLOTS"]).await;
        let mut masters = Vec::new();
        let mut seen = HashSet::new();
        for entry in elements(&slots) {
            let node = elements(&elements(entry)[2]);
            let id = text(&node[2]);
            if seen.insert(id.clone()) {
                let address = NodeAddress {
                    host: text(&node[0]),
                    port: redis::from_redis_value(&node[1]).unwrap(),
                    unix_socket_path: None,
                };
                masters.push(Master {
                    connection: admin(&address).await,
                    address,
                    id,
                });
            }
        }
        assert!(
            masters.len() >= 2,
            "cluster requires at least two primaries"
        );
        for _ in 0..128 {
            let key = unique_name("slot");
            let reservation = format!("{key}:reservation");
            let slot_value = run(&mut seed_connection, &["CLUSTER", "KEYSLOT", &key]).await;
            let slot: u16 = redis::from_redis_value(&slot_value).unwrap();
            let owner = elements(&slots)
                .iter()
                .find(|entry| {
                    let entry = elements(entry);
                    let first: u16 = redis::from_redis_value(&entry[0]).unwrap();
                    let last: u16 = redis::from_redis_value(&entry[1]).unwrap();
                    first <= slot && slot <= last
                })
                .unwrap();
            let owner_id = text(&elements(&elements(owner)[2])[2]);
            if excluded_owners.contains(&owner_id) {
                continue;
            }
            let source = masters
                .iter()
                .position(|master| master.id == owner_id)
                .unwrap();
            let target = (source + 1) % masters.len();
            let slot = slot.to_string();
            let connection = &mut masters[source].connection;
            if run(connection, &["CLUSTER", "COUNTKEYSINSLOT", &slot]).await != Value::Int(0) {
                continue;
            }
            let reserved = run(
                connection,
                &["SET", &reservation, "reserved", "NX", "PX", "180000"],
            )
            .await;
            assert_eq!(text(&reserved), "OK");
            if run(connection, &["CLUSTER", "COUNTKEYSINSLOT", &slot]).await != Value::Int(1) {
                run(connection, &["DEL", &reservation]).await;
                continue;
            }
            return Self {
                key,
                reservation,
                slot,
                masters,
                source,
                target,
            };
        }
        panic!("could not reserve an unused random cluster slot");
    }

    async fn begin_import(&mut self) {
        let source_id = self.masters[self.source].id.clone();
        let target_id = self.masters[self.target].id.clone();
        assert_eq!(
            text(
                &run(
                    &mut self.masters[self.target].connection,
                    &["CLUSTER", "SETSLOT", &self.slot, "IMPORTING", &source_id],
                )
                .await
            ),
            "OK"
        );
        assert_eq!(
            text(
                &run(
                    &mut self.masters[self.source].connection,
                    &["CLUSTER", "SETSLOT", &self.slot, "MIGRATING", &target_id],
                )
                .await
            ),
            "OK"
        );
    }

    async fn move_ownership(&mut self) {
        self.begin_import().await;
        let target = self.masters[self.target].address.clone();
        let port = target.port.to_string();
        assert_eq!(
            text(
                &run(
                    &mut self.masters[self.source].connection,
                    &[
                        "MIGRATE",
                        &target.host,
                        &port,
                        &self.reservation,
                        "0",
                        "5000"
                    ],
                )
                .await
            ),
            "OK"
        );
        let owner = self.masters[self.target].id.clone();
        for master in &mut self.masters {
            assert_eq!(
                text(
                    &run(
                        &mut master.connection,
                        &["CLUSTER", "SETSLOT", &self.slot, "NODE", &owner],
                    )
                    .await
                ),
                "OK"
            );
        }
    }

    async fn cleanup(&mut self) {
        // ASKING also works on stable owners, so cleanup covers partial transitions.
        for index in [self.target, self.source] {
            let connection = &mut self.masters[index].connection;
            for key in [&self.key, &self.reservation] {
                let _: redis::RedisResult<Value> = redis::pipe()
                    .cmd("ASKING")
                    .ignore()
                    .cmd("DEL")
                    .arg(key)
                    .query_async(connection)
                    .await;
            }
        }
        let owner = self.masters[self.source].id.clone();
        let target = self.masters[self.target].id.clone();
        for index in [self.source, self.target] {
            run(
                &mut self.masters[index].connection,
                &["CLUSTER", "SETSLOT", &self.slot, "STABLE"],
            )
            .await;
        }
        let slots = run(
            &mut self.masters[self.source].connection,
            &["CLUSTER", "SLOTS"],
        )
        .await;
        let current_owner =
            text(&elements(&slot_description(&slots, self.slot.parse().unwrap())[2])[2]);
        if current_owner != owner {
            assert_eq!(current_owner, target, "only this lease's owner may change");
            run(
                &mut self.masters[self.target].connection,
                &["CLUSTER", "SETSLOT", &self.slot, "MIGRATING", &owner],
            )
            .await;
            run(
                &mut self.masters[self.source].connection,
                &["CLUSTER", "SETSLOT", &self.slot, "IMPORTING", &target],
            )
            .await;
            // IMPORTING -> NODE gives the restored owner a newer config epoch.
            // Merely assigning NODE can lose to the previous owner's gossip.
            run(
                &mut self.masters[self.source].connection,
                &["CLUSTER", "SETSLOT", &self.slot, "NODE", &owner],
            )
            .await;
        }
        for master in &mut self.masters {
            assert_eq!(
                text(
                    &run(
                        &mut master.connection,
                        &["CLUSTER", "SETSLOT", &self.slot, "NODE", &owner],
                    )
                    .await
                ),
                "OK",
                "restore only this test's slot"
            );
        }
        // NODE restores ownership but leaves IMPORTING on a non-owning target.
        for index in [self.source, self.target] {
            let connection = &mut self.masters[index].connection;
            assert_eq!(
                text(&run(connection, &["CLUSTER", "SETSLOT", &self.slot, "STABLE"]).await),
                "OK"
            );
            let nodes = text(&run(connection, &["CLUSTER", "NODES"]).await);
            assert!(
                !nodes.contains(&format!("[{}-", self.slot)),
                "owned slot must not retain an IMPORTING/MIGRATING marker: {nodes}"
            );
        }
        self.await_restored_owner().await;
    }

    async fn await_restored_owner(&mut self) {
        let owner = self.masters[self.source].id.clone();
        let slots = run(
            &mut self.masters[self.source].connection,
            &["CLUSTER", "SLOTS"],
        )
        .await;
        let mut seen = HashSet::new();
        let mut observers = Vec::new();
        for shard in elements(&slots) {
            for node in &elements(shard)[2..] {
                let node = elements(node);
                if seen.insert(text(&node[2])) {
                    observers.push(
                        admin(&NodeAddress {
                            host: text(&node[0]),
                            port: redis::from_redis_value(&node[1]).unwrap(),
                            unix_socket_path: None,
                        })
                        .await,
                    );
                }
            }
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let mut converged = true;
            for observer in &mut observers {
                let slots = run(observer, &["CLUSTER", "SLOTS"]).await;
                let current_owner =
                    text(&elements(&slot_description(&slots, self.slot.parse().unwrap())[2])[2]);
                converged &= current_owner == owner;
            }
            if converged {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "all primary and replica views must converge before a later failover"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn redirected_graph(protocol: ProtocolVersion, asking: bool) {
    let seed = endpoint("FALKORDB_CLUSTER_ENDPOINT");
    let mut lease = SlotLease::reserve(&seed).await;
    let outcome = AssertUnwindSafe(async {
        // Disable periodic refresh so this existing client's slot map stays stale.
        let client = core_client(
            vec![seed],
            true,
            protocol,
            ReadFrom::Primary,
            unique_name("redirect-client"),
        )
        .await;
        let graph = GraphContext::new(client.clone(), lease.key.clone(), 0).unwrap();
        if asking {
            lease.begin_import().await;
        } else {
            lease.move_ownership().await;
        }
        let rejection = attempt(
            &mut lease.masters[lease.source].connection,
            &["GRAPH.QUERY", &lease.key, CREATE_PATH, "--compact"],
        )
        .await
        .unwrap_err();
        assert_eq!(
            rejection.kind(),
            if asking {
                ErrorKind::Ask
            } else {
                ErrorKind::Moved
            }
        );
        let observation = Observation::start(&lease.masters[lease.target].address).await;
        let result = graph
            .query(CREATE_PATH, &GraphQueryOptions::default())
            .await
            .unwrap();
        assert_path(&result);
        assert_eq!(client.available_inflight_count(), 1);
        observation
            .assert_graph_affinity(&lease.key, "GRAPH.QUERY", CREATE_PATH, asking)
            .await;
        observation.monitor.stop_async().await;
        assert_eq!(result.statistics.nodes_created, Some(2));
        assert_eq!(result.statistics.relationships_created, Some(1));
    })
    .catch_unwind()
    .await;
    lease.cleanup().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[ignore = "requires dedicated FalkorDB cluster; run --ignored --test-threads=1"]
async fn graph_context_follows_moved_and_pins_all_metadata() {
    for protocol in PROTOCOLS {
        redirected_graph(protocol, false).await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated FalkorDB cluster; run --ignored --test-threads=1"]
async fn graph_context_follows_ask_for_query_and_each_dictionary() {
    for protocol in PROTOCOLS {
        redirected_graph(protocol, true).await;
    }
}

fn assert_named_node(result: &GraphQueryResult, label: &str, property: &str, value: i64) {
    assert_eq!(result.rows.len(), 1);
    let GraphValue::Node(node) = &result.rows[0][0] else {
        panic!("expected resolved node: {result:?}");
    };
    assert_eq!(node.labels, [label]);
    assert_eq!(
        node.properties,
        [(property.into(), GraphValue::Integer(value))]
    );
}

#[tokio::test]
#[ignore = "requires dedicated FalkorDB primary/replica; briefly pauses replica writes"]
async fn graph_context_replica_reads_resolve_divergent_dictionaries_locally() {
    let primary = endpoint("FALKORDB_STANDALONE_ENDPOINT");
    let replica = endpoint("FALKORDB_REPLICA_ENDPOINT");
    for protocol in PROTOCOLS {
        let key = unique_name("replica");
        let mut writer = admin(&primary).await;
        let mut reader = admin(&replica).await;
        let outcome = AssertUnwindSafe(async {
            run(
                &mut writer,
                &["GRAPH.QUERY", &key, "CREATE (:Before {old:1})", "--compact"],
            )
            .await;
            assert_eq!(
                run(&mut writer, &["WAIT", "1", "10000"]).await,
                Value::Int(1)
            );
            let client = core_client(
                vec![primary.clone(), replica.clone()],
                false,
                protocol,
                ReadFrom::PreferReplica,
                unique_name("replica-client"),
            )
            .await;
            let graph = GraphContext::new(client, key.clone(), 0).unwrap();
            let options = GraphQueryOptions::default();
            assert_named_node(
                &graph.ro_query("MATCH(n) RETURN n", &options).await.unwrap(),
                "Before",
                "old",
                1,
            );
            assert_eq!(
                text(&run(&mut reader, &["CLIENT", "PAUSE", "30000", "WRITE"]).await),
                "OK"
            );
            run(&mut writer, &["GRAPH.DELETE", &key]).await;
            run(
                &mut writer,
                &["GRAPH.QUERY", &key, "CREATE (:After {new:2})", "--compact"],
            )
            .await;
            let old = graph.ro_query("MATCH(n) RETURN n", &options).await.unwrap();
            assert_named_node(&old, "Before", "old", 1);
            let current = graph.query("MATCH(n) RETURN n", &options).await.unwrap();
            assert_named_node(&current, "After", "new", 2);
            assert_eq!(
                old.statistics.graph_version,
                current.statistics.graph_version
            );
            assert_eq!(text(&run(&mut reader, &["CLIENT", "UNPAUSE"]).await), "OK");
            assert_eq!(
                run(&mut writer, &["WAIT", "1", "10000"]).await,
                Value::Int(1)
            );
            assert_named_node(
                &graph.ro_query("MATCH(n) RETURN n", &options).await.unwrap(),
                "After",
                "new",
                2,
            );
        })
        .catch_unwind()
        .await;
        run(&mut reader, &["CLIENT", "UNPAUSE"]).await;
        run(&mut writer, &["DEL", &key]).await;
        run(&mut writer, &["WAIT", "1", "10000"]).await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
}

async fn named_connections(connection: &mut MultiplexedConnection, name: &str) -> Vec<String> {
    text(&run(connection, &["CLIENT", "LIST"]).await)
        .lines()
        .filter_map(|line| {
            let fields: HashMap<_, _> = line
                .split_whitespace()
                .filter_map(|field| field.split_once('='))
                .collect();
            (fields.get("name") == Some(&name)).then(|| fields["id"].to_string())
        })
        .collect()
}

#[tokio::test]
#[ignore = "requires dedicated FalkorDB; disconnects only this test's named client IDs"]
async fn graph_context_survives_exact_connection_reconnect_without_replaying_writes() {
    let primary = endpoint("FALKORDB_STANDALONE_ENDPOINT");
    for protocol in PROTOCOLS {
        let key = unique_name("reconnect");
        let name = unique_name("reconnect-client");
        let mut controller = admin(&primary).await;
        let outcome = AssertUnwindSafe(async {
            let client = core_client(
                vec![primary.clone()],
                false,
                protocol,
                ReadFrom::Primary,
                name.clone(),
            )
            .await;
            let graph = GraphContext::new(client, key.clone(), 0).unwrap();
            let options = GraphQueryOptions::default();
            let created = graph
                .query("CREATE (n:Reconnect {marker:1}) RETURN n", &options)
                .await
                .unwrap();
            assert_named_node(&created, "Reconnect", "marker", 1);
            let original = named_connections(&mut controller, &name).await;
            assert!(
                !original.is_empty(),
                "test client must have identifiable connections"
            );
            for id in &original {
                assert_eq!(
                    run(&mut controller, &["CLIENT", "KILL", "ID", id]).await,
                    Value::Int(1)
                );
            }
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                // Each retry is a new application read, not a replay of the write.
                match graph.ro_query("MATCH(n) RETURN n", &options).await {
                    Ok(result) => {
                        assert_named_node(&result, "Reconnect", "marker", 1);
                        break;
                    }
                    Err(error) => {
                        assert!(
                            Instant::now() < deadline,
                            "client did not reconnect: {error}"
                        );
                        sleep(Duration::from_millis(50)).await;
                    }
                }
            }
            let replacements = named_connections(&mut controller, &name).await;
            assert!(!replacements.is_empty());
            assert!(replacements.iter().all(|id| !original.contains(id)));
            let counted = graph
                .ro_query("MATCH(n) RETURN count(n)", &options)
                .await
                .unwrap();
            assert_eq!(
                counted.rows,
                [vec![GraphValue::Integer(1)]],
                "the successful write must not be replayed"
            );
        })
        .catch_unwind()
        .await;
        run(&mut controller, &["DEL", &key]).await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
}

#[tokio::test]
#[ignore = "requires dedicated FalkorDB; creates and deletes only one ephemeral ACL user"]
async fn graph_context_readonly_acl_decodes_entities_and_rejects_writes() {
    let primary = endpoint("FALKORDB_STANDALONE_ENDPOINT");
    for protocol in PROTOCOLS {
        let key = unique_name("readonly-acl");
        let username = format!("glide_graph_readonly_{}", Uuid::new_v4().simple());
        let password = Uuid::new_v4().to_string();
        let mut controller = admin(&primary).await;
        let outcome = AssertUnwindSafe(async {
            run(
                &mut controller,
                &["GRAPH.QUERY", &key, CREATE_PATH, "--compact"],
            )
            .await;
            // Do not use the argument-printing admin helper for credentials.
            let mut configure_user = redis::cmd("ACL");
            configure_user
                .arg("SETUSER")
                .arg(&username)
                .arg("reset")
                .arg("on")
                .arg(format!(">{password}"))
                .arg(format!("~{key}"))
                .arg(&[
                    "+graph.ro_query",
                    "+hello",
                    "+ping",
                    "+info",
                    "+select",
                    "+client|setname",
                    "+client|setinfo",
                ]);
            assert_eq!(
                text(
                    &timeout(
                        Duration::from_secs(10),
                        configure_user.query_async::<_, Value>(&mut controller),
                    )
                    .await
                    .expect("ACL setup exceeded its deadline")
                    .expect("ephemeral read-only ACL user must be configurable")
                ),
                "OK"
            );
            let client = Client::new(
                ConnectionRequest {
                    addresses: vec![primary.clone()],
                    protocol: Some(protocol),
                    client_name: Some(unique_name("readonly-acl-client")),
                    periodic_checks: Some(PeriodicCheck::Disabled),
                    request_timeout: Some(5000),
                    connection_timeout: Some(5000),
                    authentication_info: Some(AuthenticationInfo {
                        username: Some(username.clone()),
                        password: Some(password),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("normal core client must authenticate with only the scoped read ACL");
            let graph = GraphContext::new(client.clone(), key.clone(), 0).unwrap();
            let options = GraphQueryOptions::default();
            let query = "MATCH p=(:Source)-[:EDGE]->(:Destination) RETURN p";
            let observation = Observation::start(&primary).await;
            assert_path(&graph.ro_query(query, &options).await.unwrap());
            observation
                .assert_graph_affinity(&key, "GRAPH.RO_QUERY", query, false)
                .await;
            observation.monitor.stop_async().await;

            let denied = graph
                .query("CREATE (:Forbidden)", &options)
                .await
                .expect_err("GRAPH.QUERY must be denied by the server ACL");
            assert_eq!(denied.kind(), ErrorKind::PermissionDenied);
            assert!(
                graph
                    .ro_query("CREATE (:Forbidden)", &options)
                    .await
                    .is_err(),
                "GRAPH.RO_QUERY must not allow a mutating Cypher statement"
            );
            let outside_scope =
                GraphContext::new(client, unique_name("outside-readonly-acl"), 0).unwrap();
            assert_eq!(
                outside_scope
                    .ro_query("RETURN 1", &options)
                    .await
                    .expect_err("ACL must restrict graph keys, not only command names")
                    .kind(),
                ErrorKind::PermissionDenied
            );
            assert_eq!(
                graph
                    .ro_query("MATCH(n) RETURN count(n)", &options)
                    .await
                    .unwrap()
                    .rows,
                [vec![GraphValue::Integer(2)]],
                "neither rejected write may mutate the graph"
            );
        })
        .catch_unwind()
        .await;
        run(&mut controller, &["ACL", "DELUSER", &username]).await;
        run(&mut controller, &["DEL", &key]).await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
}

fn slot_description(slots: &Value, slot: u16) -> &[Value] {
    elements(slots)
        .iter()
        .map(elements)
        .find(|entry| {
            let first: u16 = redis::from_redis_value(&entry[0]).unwrap();
            let last: u16 = redis::from_redis_value(&entry[1]).unwrap();
            first <= slot && slot <= last
        })
        .expect("cluster must advertise this slot")
}

async fn owning_replica(lease: &mut SlotLease) -> Master {
    let slots = run(
        &mut lease.masters[lease.source].connection,
        &["CLUSTER", "SLOTS"],
    )
    .await;
    let shard = slot_description(&slots, lease.slot.parse().unwrap());
    assert_eq!(
        text(&elements(&shard[2])[2]),
        lease.masters[lease.source].id
    );
    assert_eq!(
        shard.len(),
        4,
        "exclusive harness must have exactly one replica for this primary"
    );
    let node = elements(&shard[3]);
    let address = NodeAddress {
        host: text(&node[0]),
        port: redis::from_redis_value(&node[1]).unwrap(),
        unix_socket_path: None,
    };
    Master {
        connection: admin(&address).await,
        address,
        id: text(&node[2]),
    }
}

fn info_fields(value: &Value) -> HashMap<String, String> {
    text(value)
        .lines()
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

async fn await_failover_roles(
    lease: &mut SlotLease,
    replica: &mut Master,
    promoted: bool,
) -> redis::RedisResult<()> {
    let original_id = lease.masters[lease.source].id.clone();
    let expected_owner = if promoted {
        replica.id.clone()
    } else {
        original_id
    };
    let slot = lease.slot.parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let state: redis::RedisResult<bool> = async {
            for connection in lease
                .masters
                .iter_mut()
                .map(|node| &mut node.connection)
                .chain(std::iter::once(&mut replica.connection))
            {
                let slots = attempt(connection, &["CLUSTER", "SLOTS"]).await?;
                let owner = text(&elements(&slot_description(&slots, slot)[2])[2]);
                let info = info_fields(&attempt(connection, &["CLUSTER", "INFO"]).await?);
                if owner != expected_owner
                    || info.get("cluster_state").map(String::as_str) != Some("ok")
                {
                    return Ok(false);
                }
            }
            let original = info_fields(
                &attempt(
                    &mut lease.masters[lease.source].connection,
                    &["INFO", "REPLICATION"],
                )
                .await?,
            );
            let other =
                info_fields(&attempt(&mut replica.connection, &["INFO", "REPLICATION"]).await?);
            let (primary, follower, primary_address) = if promoted {
                (&other, &original, &replica.address)
            } else {
                (&original, &other, &lease.masters[lease.source].address)
            };
            Ok(primary.get("role").map(String::as_str) == Some("master")
                && follower.get("role").map(String::as_str) == Some("slave")
                && follower.get("master_link_status").map(String::as_str) == Some("up")
                && follower.get("master_sync_in_progress").map(String::as_str) == Some("0")
                && follower.get("master_host") == Some(&primary_address.host)
                && follower.get("master_port") == Some(&primary_address.port.to_string()))
        }
        .await;
        if matches!(state, Ok(true)) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(redis::RedisError::from((
                ErrorKind::IoError,
                "cluster failover did not converge",
                format!("expected owner {expected_owner}, promoted={promoted}, last={state:?}"),
            )));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn restore_failover_primary(
    lease: &mut SlotLease,
    replica: &mut Master,
) -> redis::RedisResult<()> {
    if let Err(promotion_error) = await_failover_roles(lease, replica, true).await {
        // A rejected/incomplete promotion may have left the original pair intact.
        return await_failover_roles(lease, replica, false)
            .await
            .map_err(|_| promotion_error);
    }
    let reply = attempt(
        &mut lease.masters[lease.source].connection,
        &["CLUSTER", "FAILOVER"],
    )
    .await?;
    assert_eq!(text(&reply), "OK");
    await_failover_roles(lease, replica, false).await
}

#[tokio::test]
#[ignore = "requires an EXCLUSIVE harness and FALKORDB_TEST_FAILOVER=1; promotes then restores a shard"]
async fn graph_context_survives_actual_primary_failover_and_restores_topology() {
    if std::env::var("FALKORDB_TEST_FAILOVER").as_deref() != Ok("1") {
        eprintln!(
            "not running actual failover: set FALKORDB_TEST_FAILOVER=1 on an exclusive harness"
        );
        return;
    }
    let seed = endpoint("FALKORDB_CLUSTER_ENDPOINT");
    let mut exercised_primaries = HashSet::new();
    for protocol in PROTOCOLS {
        // Separate shards prevent back-to-back reuse of the server's previous
        // manual-election state after promoting the original primary back.
        let mut lease = SlotLease::reserve_excluding(&seed, &exercised_primaries).await;
        exercised_primaries.insert(lease.masters[lease.source].id.clone());
        let mut replica = owning_replica(&mut lease).await;
        let mut promotion_attempted = false;
        let outcome = AssertUnwindSafe(async {
            let client = core_client(
                vec![seed.clone()],
                true,
                protocol,
                ReadFrom::Primary,
                unique_name("failover-client"),
            )
            .await;
            let graph = GraphContext::new(client, lease.key.clone(), 0).unwrap();
            let options = GraphQueryOptions::default();
            assert_path(&graph.query(CREATE_PATH, &options).await.unwrap());
            // An ordered marker on this admin connection makes WAIT cover the
            // graph write performed earlier on the normal core connection.
            run(
                &mut lease.masters[lease.source].connection,
                &["SET", &lease.reservation, "before-failover"],
            )
            .await;
            assert_eq!(
                run(
                    &mut lease.masters[lease.source].connection,
                    &["WAIT", "1", "10000"],
                )
                .await,
                Value::Int(1)
            );
            await_failover_roles(&mut lease, &mut replica, false)
                .await
                .unwrap();
            eprintln!(
                "actual-failover start: protocol={protocol:?} graph={} slot={} original={} replica={}",
                lease.key, lease.slot, lease.masters[lease.source].id, replica.id
            );
            promotion_attempted = true;
            assert_eq!(
                text(&run(&mut replica.connection, &["CLUSTER", "FAILOVER"]).await),
                "OK"
            );
            await_failover_roles(&mut lease, &mut replica, true)
                .await
                .unwrap();
            eprintln!(
                "actual-failover promotion verified: protocol={protocol:?} new_primary={}",
                replica.id
            );

            let query = "MATCH p=(:Source)-[:EDGE]->(:Destination) RETURN p";
            // The retained context has the old slot map. Only new application
            // reads may retry while that existing client discovers the new owner.
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                match graph.ro_query(query, &options).await {
                    Ok(result) => {
                        assert_path(&result);
                        break;
                    }
                    Err(error) => {
                        assert!(
                            Instant::now() < deadline,
                            "retained graph context did not recover after real promotion: {error}"
                        );
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            let observation = Observation::start(&replica.address).await;
            assert_path(&graph.ro_query(query, &options).await.unwrap());
            observation
                .assert_graph_affinity(&lease.key, "GRAPH.RO_QUERY", query, false)
                .await;
            observation.monitor.stop_async().await;
            let created = graph
                .query("CREATE (n:PostFailover {marker:7}) RETURN n", &options)
                .await
                .unwrap();
            assert_named_node(&created, "PostFailover", "marker", 7);
            assert_eq!(created.statistics.nodes_created, Some(1));
            assert_eq!(
                graph
                    .ro_query("MATCH(n) RETURN count(n)", &options)
                    .await
                    .unwrap()
                    .rows,
                [vec![GraphValue::Integer(3)]],
                "a successful write must execute once on the promoted primary"
            );
            run(
                &mut replica.connection,
                &["SET", &lease.reservation, "after-failover"],
            )
            .await;
            assert_eq!(
                run(&mut replica.connection, &["WAIT", "1", "10000"]).await,
                Value::Int(1)
            );
        })
        .catch_unwind()
        .await;
        let restored = AssertUnwindSafe(async {
            if promotion_attempted {
                restore_failover_primary(&mut lease, &mut replica).await
            } else {
                Ok(())
            }
        })
        .catch_unwind()
        .await;
        // Even a failed restoration must attempt exact-key cleanup on both
        // possible owners. Never reset a node or change unrelated slot mappings.
        let mut deleted = false;
        for connection in [
            &mut lease.masters[lease.source].connection,
            &mut replica.connection,
        ] {
            if attempt(connection, &["DEL", &lease.key, &lease.reservation])
                .await
                .is_ok()
            {
                deleted = true;
                assert_eq!(
                    run(connection, &["WAIT", "1", "10000"]).await,
                    Value::Int(1),
                    "owned-key cleanup must replicate"
                );
            }
        }
        assert!(
            deleted,
            "could not delete owned failover keys on either owner"
        );
        match restored {
            Ok(result) => {
                result.expect("must restore original primary/replica roles and slot ownership")
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
        eprintln!(
            "actual-failover restored: protocol={protocol:?} primary={} replica={} owned_keys_deleted={deleted}",
            lease.masters[lease.source].id, replica.id
        );
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
}
