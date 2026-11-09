// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! CPU-only graph codec benchmarks derived from the recorded FalkorDB v6.0.3
//! RESP2/RESP3 fixtures. No server, runtime, network or new dependency is needed.
//!
//! Workloads select real scalar, entity, nested and vector cells, then repeat
//! their recorded row 1, 16 or 256 times. Parameter workloads represent those
//! same decoded rows as literal records: entities are maps, not graph-valued
//! parameters. Fixture loading, dictionary decoding and input cloning occur
//! outside both timing and allocation measurements.
//!
//! Each `graph_allocations` line reports allocation/reallocation requests and
//! requested bytes per codec call, averaged over 16 calls. These are cumulative
//! requested bytes, not retained/peak memory or allocator-resident bytes.
//! Counting is disabled during Criterion timing, but the allocator's disabled
//! atomic check remains; compare scaling within this benchmark, not timings
//! against binaries using a different allocator. Counting is process-wide;
//! this benchmark starts no background work.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeMap,
    hint::black_box,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use glide_core::graph::{
    GraphNode, GraphParameter, GraphQueryOptions, GraphQueryResult, GraphRelationship, GraphSchema,
    GraphValue,
    decode::{decode_dictionary, decode_result},
    params::serialize_query,
};
use redis::Value;
use serde_json::Value as Json;

struct CountingAllocator;

static RECORDING: AtomicBool = AtomicBool::new(false);
static ALLOCATION_REQUESTS: AtomicU64 = AtomicU64::new(0);
static REQUESTED_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn record_allocation(pointer: *mut u8, size: usize) {
    if !pointer.is_null() && RECORDING.load(Ordering::Relaxed) {
        ALLOCATION_REQUESTS.fetch_add(1, Ordering::Relaxed);
        REQUESTED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
    }
}

// SAFETY: every allocation operation forwards its original pointer/layout to
// System unchanged. Instrumentation uses only atomics: it neither allocates nor
// dereferences pointers, and cannot recurse into this allocator.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid allocation layout.
        let pointer = unsafe { System.alloc(layout) };
        record_allocation(pointer, layout.size());
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid allocation layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        record_allocation(pointer, layout.size());
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller supplies the original System pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: the caller satisfies GlobalAlloc::realloc's original-layout
        // and nonzero-new-size requirements; System retains pointer ownership.
        let pointer = unsafe { System.realloc(pointer, layout, size) };
        record_allocation(pointer, size);
        pointer
    }
}

struct Recording;

impl Drop for Recording {
    fn drop(&mut self) {
        RECORDING.store(false, Ordering::Relaxed);
    }
}

fn allocations(mut run: impl FnMut()) -> (f64, f64) {
    const REQUESTS: u64 = 16;
    ALLOCATION_REQUESTS.store(0, Ordering::Relaxed);
    REQUESTED_BYTES.store(0, Ordering::Relaxed);
    RECORDING.store(true, Ordering::Relaxed);
    let recording = Recording;
    for _ in 0..REQUESTS {
        run();
    }
    drop(recording);
    (
        ALLOCATION_REQUESTS.load(Ordering::Relaxed) as f64 / REQUESTS as f64,
        REQUESTED_BYTES.load(Ordering::Relaxed) as f64 / REQUESTS as f64,
    )
}

fn recorded(value: &Json) -> Value {
    let payload = &value["value"];
    match value["type"].as_str().expect("Recorded RESP type") {
        "array" => Value::Array(
            payload
                .as_array()
                .expect("Recorded array")
                .iter()
                .map(recorded)
                .collect(),
        ),
        "bulk" => {
            let text = payload.as_str().expect("Recorded string");
            let text = if text == "Query internal execution time: <milliseconds> milliseconds" {
                "Query internal execution time: 0.000000 milliseconds"
            } else {
                text
            };
            Value::BulkString(text.to_owned().into())
        }
        "integer" => Value::Int(payload.as_i64().expect("Exact recorded i64")),
        "double" => Value::Double(
            payload
                .as_str()
                .expect("Recorded double spelling")
                .parse()
                .expect("Valid recorded double"),
        ),
        "null" => Value::Nil,
        other => panic!("Unexpected recorded compact response type: {other}"),
    }
}

struct Fixture {
    protocol: &'static str,
    wire: Value,
    schema: GraphSchema,
    decoded: GraphQueryResult,
}

impl Fixture {
    fn load(protocol: &'static str, source: &str) -> Self {
        let source: Json = serde_json::from_str(source).expect("Valid recorded fixture JSON");
        let steps = source["cases"]
            .as_array()
            .expect("Recorded cases")
            .iter()
            .find(|case| case["name"] == "all_tags")
            .expect("Recorded all_tags case")["steps"]
            .as_array()
            .expect("Recorded steps");
        let wire = recorded(&steps[0]["response"]);
        let schema = GraphSchema {
            labels: decode_dictionary(&recorded(&steps[1]["response"]))
                .expect("Recorded label dictionary"),
            relationship_types: decode_dictionary(&recorded(&steps[2]["response"]))
                .expect("Recorded relationship dictionary"),
            property_keys: decode_dictionary(&recorded(&steps[3]["response"]))
                .expect("Recorded property dictionary"),
        };
        let decoded = decode_result(&wire, &schema).expect("Recorded all_tags result decodes");
        assert_eq!(decoded.columns.len(), 16);
        assert_eq!(decoded.rows.len(), 1);
        assert_eq!(decoded.rows[0][2], GraphValue::Integer(i64::MIN));
        assert_eq!(decoded.rows[0][12], GraphValue::DateTime(1_577_934_245));
        assert_eq!(decoded.rows[0][13], GraphValue::Date(1_577_923_200));
        assert_eq!(decoded.rows[0][14], GraphValue::Time(11_045));
        assert_eq!(decoded.rows[0][15], GraphValue::Duration(36_907_506));
        Self {
            protocol,
            wire,
            schema,
            decoded,
        }
    }

    fn workload(&self, columns: &[usize], rows: usize) -> Value {
        let Value::Array(envelope) = &self.wire else {
            panic!("Recorded compact envelope");
        };
        let Value::Array(header) = &envelope[0] else {
            panic!("Recorded compact header");
        };
        let Value::Array(recorded_rows) = &envelope[1] else {
            panic!("Recorded compact rows");
        };
        let Value::Array(row) = &recorded_rows[0] else {
            panic!("Recorded compact row");
        };
        let row: Vec<_> = columns.iter().map(|column| row[*column].clone()).collect();
        let wire = Value::Array(vec![
            Value::Array(
                columns
                    .iter()
                    .map(|column| header[*column].clone())
                    .collect(),
            ),
            Value::Array((0..rows).map(|_| Value::Array(row.clone())).collect()),
            envelope[2].clone(),
        ]);
        let decoded = decode_result(&wire, &self.schema).expect("Derived fixture decodes");
        assert_eq!(decoded.rows.len(), rows);
        for row in &decoded.rows {
            assert_eq!(row.len(), columns.len());
            for (value, column) in row.iter().zip(columns) {
                assert_eq!(value, &self.decoded.rows[0][*column]);
            }
        }
        wire
    }
}

fn parameter_map(values: &[(String, GraphValue)]) -> GraphParameter {
    GraphParameter::Map(
        values
            .iter()
            .map(|(key, value)| (key.clone(), parameter(value)))
            .collect(),
    )
}

fn node_parameter(node: &GraphNode) -> GraphParameter {
    GraphParameter::Map(BTreeMap::from([
        ("id".into(), GraphParameter::Integer(node.id)),
        (
            "labels".into(),
            GraphParameter::List(
                node.labels
                    .iter()
                    .cloned()
                    .map(GraphParameter::String)
                    .collect(),
            ),
        ),
        ("properties".into(), parameter_map(&node.properties)),
    ]))
}

fn relationship_parameter(relationship: &GraphRelationship) -> GraphParameter {
    GraphParameter::Map(BTreeMap::from([
        ("id".into(), GraphParameter::Integer(relationship.id)),
        (
            "type".into(),
            GraphParameter::String(relationship.r#type.clone()),
        ),
        (
            "source_id".into(),
            GraphParameter::Integer(relationship.source_id),
        ),
        (
            "destination_id".into(),
            GraphParameter::Integer(relationship.destination_id),
        ),
        ("properties".into(), parameter_map(&relationship.properties)),
    ]))
}

fn parameter(value: &GraphValue) -> GraphParameter {
    match value {
        GraphValue::Null => GraphParameter::Null,
        GraphValue::String(value) => GraphParameter::String(value.clone()),
        GraphValue::Integer(value)
        | GraphValue::DateTime(value)
        | GraphValue::Date(value)
        | GraphValue::Time(value)
        | GraphValue::Duration(value) => GraphParameter::Integer(*value),
        GraphValue::Boolean(value) => GraphParameter::Boolean(*value),
        GraphValue::Double(value) => GraphParameter::Double(*value),
        GraphValue::List(values) => GraphParameter::List(values.iter().map(parameter).collect()),
        GraphValue::Map(values) => parameter_map(values),
        GraphValue::Node(value) => node_parameter(value),
        GraphValue::Relationship(value) => relationship_parameter(value),
        GraphValue::Path(value) => GraphParameter::Map(BTreeMap::from([
            (
                "nodes".into(),
                GraphParameter::List(value.nodes.iter().map(node_parameter).collect()),
            ),
            (
                "relationships".into(),
                GraphParameter::List(
                    value
                        .relationships
                        .iter()
                        .map(relationship_parameter)
                        .collect(),
                ),
            ),
        ])),
        GraphValue::Point(value) => GraphParameter::Map(BTreeMap::from([
            ("latitude".into(), GraphParameter::Double(value.latitude)),
            ("longitude".into(), GraphParameter::Double(value.longitude)),
        ])),
        GraphValue::Vector(values) => GraphParameter::List(
            values
                .iter()
                .map(|value| GraphParameter::Double(f64::from(*value)))
                .collect(),
        ),
    }
}

const WORKLOADS: [(&str, &[usize]); 4] = [
    ("scalar", &[0, 1, 2, 3, 4, 12, 13, 14, 15]),
    ("entity", &[6, 7, 8]),
    ("nested", &[5, 9]),
    ("vector", &[11]),
];
const ROW_COUNTS: [usize; 3] = [1, 16, 256];
const PARAMETER_QUERY: &str = "UNWIND $rows AS row RETURN row";

fn benchmark_graph(c: &mut Criterion) {
    let fixtures = [
        Fixture::load("resp2", include_str!("../tests/fixtures/graph/resp2.json")),
        Fixture::load("resp3", include_str!("../tests/fixtures/graph/resp3.json")),
    ];
    assert_eq!(fixtures[0].decoded, fixtures[1].decoded);

    for fixture in &fixtures {
        let mut group = c.benchmark_group(format!("graph_decode/{}", fixture.protocol));
        for (name, columns) in WORKLOADS {
            for rows in ROW_COUNTS {
                let wire = fixture.workload(columns, rows);
                let (allocations, bytes) = allocations(|| {
                    black_box(decode_result(black_box(&wire), black_box(&fixture.schema)).unwrap());
                });
                eprintln!(
                    "graph_allocations operation=decode protocol={} shape={name} rows={rows} \
                     allocations_per_request={allocations:.2} requested_bytes_per_request={bytes:.2}",
                    fixture.protocol
                );
                group.throughput(Throughput::Elements((rows * columns.len()) as u64));
                group.bench_with_input(BenchmarkId::new(name, rows), &wire, |b, wire| {
                    b.iter(|| {
                        black_box(
                            decode_result(black_box(wire), black_box(&fixture.schema)).unwrap(),
                        )
                    });
                });
            }
        }
        group.finish();
    }

    let fixture = &fixtures[0];
    let mut group = c.benchmark_group("graph_serialize");
    for (name, columns) in WORKLOADS {
        let row = GraphParameter::Map(
            columns
                .iter()
                .map(|column| {
                    (
                        fixture.decoded.columns[*column].clone(),
                        parameter(&fixture.decoded.rows[0][*column]),
                    )
                })
                .collect(),
        );
        for rows in ROW_COUNTS {
            let options = GraphQueryOptions {
                parameters: BTreeMap::from([(
                    "rows".into(),
                    GraphParameter::List(vec![row.clone(); rows]),
                )]),
                ..Default::default()
            };
            let serialized =
                serialize_query(PARAMETER_QUERY, &options).expect("Derived parameters serialize");
            assert!(serialized.starts_with("CYPHER `rows`=["));
            assert!(serialized.ends_with(PARAMETER_QUERY));
            let (allocations, bytes) = allocations(|| {
                black_box(
                    serialize_query(black_box(PARAMETER_QUERY), black_box(&options)).unwrap(),
                );
            });
            eprintln!(
                "graph_allocations operation=serialize shape={name} rows={rows} \
                 output_bytes={} allocations_per_request={allocations:.2} \
                 requested_bytes_per_request={bytes:.2}",
                serialized.len()
            );
            group.throughput(Throughput::Bytes(serialized.len() as u64));
            group.bench_with_input(BenchmarkId::new(name, rows), &options, |b, options| {
                b.iter(|| {
                    black_box(
                        serialize_query(black_box(PARAMETER_QUERY), black_box(options)).unwrap(),
                    )
                });
            });
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(300))
        .without_plots();
    targets = benchmark_graph
}
criterion_main!(benches);
