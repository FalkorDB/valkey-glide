// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Dedicated pinned-FalkorDB tests, excluded from the plain-server `it_*` target.

use glide::graph::{GraphParameter, GraphPoint, GraphQueryOptions, GraphQueryResult, GraphValue};
use glide::{
    CustomCommand, GlideClient, GlideClientConfiguration, GlideClusterClient,
    GlideClusterClientConfiguration, ProtocolVersion,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

fn endpoint(name: &str) -> (String, u16) {
    let value = std::env::var(name).expect("run with the pinned FalkorDB harness");
    let (host, port) = value.rsplit_once(':').expect("host:port endpoint");
    (host.to_string(), port.parse().expect("endpoint port"))
}

fn graph_name() -> String {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    format!(
        "{{glide-graph}}:{}:{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    )
}

const ALL_TYPES: &str = "\
CREATE p=(a:Person:Researcher {name:'Ada',age:42})-[r:KNOWS {since:2020}]->(b:Person {name:'Bob'})
RETURN null AS null_value,
'text' AS string_value,
-9223372036854775808 AS integer_value,
true AS boolean_value,
1.5 AS double_value,
[1,null,{nested:[false,'x']}] AS list_value,
r AS relationship_value,
a AS node_value,
p AS path_value,
{first:1,second:'two'} AS map_value,
point({latitude:1.5,longitude:2.5}) AS point_value,
vecf32([1,2.5,-3]) AS vector_value,
localdatetime('2020-01-02T03:04:05') AS datetime_value,
date('2020-01-02') AS date_value,
localtime('03:04:05') AS time_value,
duration('P1Y2M3DT4H5M6S') AS duration_value";

fn assert_all_types(result: GraphQueryResult) {
    assert_eq!(
        result.columns,
        [
            "null_value",
            "string_value",
            "integer_value",
            "boolean_value",
            "double_value",
            "list_value",
            "relationship_value",
            "node_value",
            "path_value",
            "map_value",
            "point_value",
            "vector_value",
            "datetime_value",
            "date_value",
            "time_value",
            "duration_value",
        ]
    );
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert_eq!(row[0], GraphValue::Null);
    assert_eq!(row[1], GraphValue::String("text".into()));
    assert_eq!(row[2], GraphValue::Integer(i64::MIN));
    assert_eq!(row[3], GraphValue::Boolean(true));
    assert_eq!(row[4], GraphValue::Double(1.5));
    assert_eq!(
        row[5],
        GraphValue::List(vec![
            GraphValue::Integer(1),
            GraphValue::Null,
            GraphValue::Map(vec![(
                "nested".into(),
                GraphValue::List(vec![
                    GraphValue::Boolean(false),
                    GraphValue::String("x".into())
                ])
            )]),
        ])
    );
    let GraphValue::Relationship(relationship) = &row[6] else {
        panic!("typed relationship expected")
    };
    let GraphValue::Node(node) = &row[7] else {
        panic!("typed node expected")
    };
    let GraphValue::Path(path) = &row[8] else {
        panic!("typed path expected")
    };
    assert_eq!(relationship.r#type, "KNOWS");
    assert_eq!(relationship.source_id, node.id);
    assert_eq!(
        relationship.properties,
        vec![("since".into(), GraphValue::Integer(2020))]
    );
    assert_eq!(node.labels, ["Person", "Researcher"]);
    assert_eq!(
        node.properties,
        vec![
            ("name".into(), GraphValue::String("Ada".into())),
            ("age".into(), GraphValue::Integer(42)),
        ]
    );
    assert_eq!(path.nodes.len(), 2);
    assert_eq!(path.nodes[0], *node);
    assert_eq!(path.nodes[1].id, relationship.destination_id);
    assert_eq!(
        path.relationships.as_slice(),
        std::slice::from_ref(relationship)
    );
    assert_eq!(
        row[9],
        GraphValue::Map(vec![
            ("first".into(), GraphValue::Integer(1)),
            ("second".into(), GraphValue::String("two".into())),
        ])
    );
    assert_eq!(
        row[10],
        GraphValue::Point(GraphPoint {
            latitude: 1.5,
            longitude: 2.5,
        })
    );
    assert_eq!(row[11], GraphValue::Vector(vec![1.0, 2.5, -3.0]));
    assert_eq!(row[12], GraphValue::DateTime(1_577_934_245));
    assert_eq!(row[13], GraphValue::Date(1_577_923_200));
    assert_eq!(row[14], GraphValue::Time(11_045));
    assert_eq!(row[15], GraphValue::Duration(36_907_506));
}

#[tokio::test]
async fn selection_is_local_and_rejects_embedded_nul() {
    let client = GlideClient::connect(
        GlideClientConfiguration::with_address("127.0.0.1", 1).lazy_connect(true),
    )
    .await
    .unwrap();
    assert!(client.select_graph("local").is_ok());
    assert!(client.select_graph("bad\0name").is_err());
}

async fn check_graph(graph: glide::Graph) {
    let options = GraphQueryOptions {
        parameters: BTreeMap::from([
            ("min".into(), GraphParameter::Integer(i64::MIN)),
            ("max".into(), GraphParameter::Integer(i64::MAX)),
            (
                "text".into(),
                GraphParameter::String("quote'\"\\\n\t\u{263a}".into()),
            ),
        ]),
        timeout_ms: Some(0),
    };
    let result = graph
        .query(
            "RETURN $min AS minimum, $max AS maximum, $text AS text",
            &options,
        )
        .await
        .unwrap();
    assert_eq!(result.columns, ["minimum", "maximum", "text"]);
    assert_eq!(result.rows[0][0], GraphValue::Integer(i64::MIN));
    assert_eq!(result.rows[0][1], GraphValue::Integer(i64::MAX));
    assert_eq!(
        result.rows[0][2],
        GraphValue::String("quote'\"\\\n\t\u{263a}".into())
    );
    let result = graph
        .query(
            "CREATE p=(a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'}) RETURN a,b,p",
            &GraphQueryOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.columns, ["a", "b", "p"]);
    let GraphValue::Node(node) = &result.rows[0][0] else {
        panic!("typed node expected")
    };
    assert_eq!(node.labels, ["Person"]);
    assert_eq!(
        node.properties,
        vec![("name".into(), GraphValue::String("Alice".into()))]
    );
    let GraphValue::Path(path) = &result.rows[0][2] else {
        panic!("typed path expected")
    };
    assert_eq!(path.nodes.len(), 2);
    assert_eq!(path.relationships.len(), 1);
    let result = graph
        .ro_query(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name",
            &GraphQueryOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert!(
        graph
            .ro_query("CREATE ()", &GraphQueryOptions::default())
            .await
            .is_err()
    );
    assert!(
        graph
            .query("RETURN '\0'", &GraphQueryOptions::default())
            .await
            .is_err()
    );
    assert_all_types(
        graph
            .query(ALL_TYPES, &GraphQueryOptions::default())
            .await
            .unwrap(),
    );
}

#[tokio::test]
#[ignore = "requires the dedicated pinned FalkorDB harness"]
async fn standalone_resp2_and_resp3() {
    let (host, port) = endpoint("FALKORDB_STANDALONE_ENDPOINT");
    for protocol in [ProtocolVersion::RESP2, ProtocolVersion::RESP3] {
        let client = GlideClient::connect(
            GlideClientConfiguration::with_address(&host, port)
                .protocol(protocol)
                .request_timeout(Duration::from_secs(10)),
        )
        .await
        .unwrap();
        let name = graph_name();
        check_graph(client.select_graph(&name).unwrap()).await;
        client
            .custom_command(&["GRAPH.DELETE", &name])
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires the dedicated pinned FalkorDB harness"]
async fn cluster_resp2_and_resp3() {
    let (host, port) = endpoint("FALKORDB_CLUSTER_ENDPOINT");
    for protocol in [ProtocolVersion::RESP2, ProtocolVersion::RESP3] {
        let client = GlideClusterClient::connect(
            GlideClusterClientConfiguration::with_address(&host, port)
                .protocol(protocol)
                .request_timeout(Duration::from_secs(10)),
        )
        .await
        .unwrap();
        let name = graph_name();
        check_graph(client.select_graph(&name).unwrap()).await;
        client
            .custom_command(&["GRAPH.DELETE", &name])
            .await
            .unwrap();
    }
}

#[cfg(feature = "sync")]
#[test]
#[ignore = "requires the dedicated pinned FalkorDB harness"]
fn blocking_standalone_and_cluster() {
    for protocol in [ProtocolVersion::RESP2, ProtocolVersion::RESP3] {
        let (host, port) = endpoint("FALKORDB_STANDALONE_ENDPOINT");
        let client = glide::sync::SyncGlideClient::connect(
            GlideClientConfiguration::with_address(host, port)
                .protocol(protocol)
                .request_timeout(Duration::from_secs(10)),
        )
        .unwrap();
        let name = graph_name();
        let graph = client.select_graph(&name).unwrap();
        assert_all_types(
            graph
                .query(ALL_TYPES, &GraphQueryOptions::default())
                .unwrap(),
        );
        assert_eq!(
            graph
                .ro_query("RETURN 42", &GraphQueryOptions::default())
                .unwrap()
                .rows,
            vec![vec![GraphValue::Integer(42)]]
        );
        client.custom_command(&["GRAPH.DELETE", &name]).unwrap();
        let (host, port) = endpoint("FALKORDB_CLUSTER_ENDPOINT");
        let client = glide::sync::SyncGlideClusterClient::connect(
            GlideClusterClientConfiguration::with_address(host, port)
                .protocol(protocol)
                .request_timeout(Duration::from_secs(10)),
        )
        .unwrap();
        let name = graph_name();
        let graph = client.select_graph(&name).unwrap();
        assert_all_types(
            graph
                .query(ALL_TYPES, &GraphQueryOptions::default())
                .unwrap(),
        );
        assert_eq!(
            graph
                .ro_query("RETURN 42", &GraphQueryOptions::default())
                .unwrap()
                .rows,
            vec![vec![GraphValue::Integer(42)]]
        );
        client.custom_command(&["GRAPH.DELETE", &name]).unwrap();
    }
}
