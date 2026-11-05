# FalkorDB Graph API

The query-first Graph API uses an existing GLIDE client and a shared Rust
implementation of the FalkorDB compact protocol. Its initial compatibility
baseline is **FalkorDB v6.0.3**, tested separately from Valkey Search and JSON.
An ordinary Valkey or Redis server does not provide `GRAPH.*` commands.

## Select a graph

Selection is local: it does not connect, execute a query, or create a graph.
The first successful `GRAPH.QUERY` creates a missing graph according to the
server's rules. `GRAPH.RO_QUERY` does not create a missing graph.

| Client | Selection | Query | Read-only query |
| --- | --- | --- | --- |
| Python async | `client.select_graph(name)` | `await graph.query(query, params=..., timeout_ms=...)` | `await graph.ro_query(...)` |
| Python sync | `client.select_graph(name)` | `graph.query(query, params=..., timeout_ms=...)` | `graph.ro_query(...)` |
| Node.js/TypeScript | `client.selectGraph(name)` | `graph.query(query, options)` | `graph.roQuery(...)` |
| Java | `client.selectGraph(name)` | `graph.query(query, options)` | `graph.roQuery(...)` |
| Go | `client.SelectGraph(name)` | `graph.Query(ctx, query, options)` | `graph.ROQuery(...)` |
| Rust async | `client.select_graph(name)` | `graph.query(query, &options).await` | `graph.ro_query(...).await` |
| Rust blocking | `client.select_graph(name)` | `graph.query(query, &options)` | `graph.ro_query(...)` |

Refer to the [Python](../python/README.md), [Node.js](../node/README.md),
[Java](../java/README.md), [Go](../go/README.md), and
[Rust](../rust/README.md) guides for concrete parameter and result types.
Java operations return `CompletableFuture`; Node operations return `Promise`.

A graph handle owns a native Rust context and retains its parent client, not a
second connection. Clones share that context. Close the parent normally; later graph operations
must not use a closed client. The Rust client has drop-based ownership rather
than a separate close operation.

Database selection is captured with a generation counter. A `SELECT` attempt
invalidates existing graph handles even if it selects the same database. If a
`SELECT` fails or times out, explicitly select a known database successfully
before selecting another graph. Graph operations hold a shared connection-state lease through query and
metadata resolution. `SELECT`, `RESET` and raw transaction-state changes take an
exclusive lease, so they cannot move an in-flight graph operation to another
database or queue it inside `MULTI`; independent graph queries remain concurrent.
A confirmed successful `RESET`, including in a non-atomic batch, selects database zero for
fresh graph handles. Confirmed batch state changes are recorded before reporting
an unrelated per-command error. An ambiguous batch state change still requires a
successful standalone `SELECT` before selecting a new handle.
`RESET` immediately cancels `MULTI`; it is not a queued command in `EXEC`.
An atomic batch containing `RESET` consequently fails with `EXEC without MULTI`
and conservatively invalidates graph handles until a standalone `SELECT` succeeds.
Graph handles cannot retain pooled-client leases or isolated
scopes; no graph-specific pooling API is introduced.

## Parameters and deadlines

Use parameters instead of interpolating user input:

```python
graph = client.select_graph("people")
result = await graph.query(
    "CREATE (p:Person {name: $name}) RETURN p",
    params={"name": "Alice"},
)
```

The shared core serializes a `CYPHER` parameter prefix once and leaves the query
body unchanged. Accepted inputs are null, booleans, signed 64-bit integers,
finite doubles, Unicode strings, lists, and nested string-keyed maps. Bindings
reject cycles, unsupported objects, integer overflow and nonfinite numbers.
Parameter errors identify the offending path. Keys use validated quoted
identifiers, not general-purpose object stringification.

Shared parameter and result processing permits up to 128 logical nesting levels;
bindings may impose a stricter input limit. Compact graph values add RESP
envelopes around those levels. The asynchronous wire decoder therefore uses a
bounded 512-aggregate iterative builder, without recursive parser stack growth or
changes to ordinary command response shapes.

Nested Rust values still have recursive ordinary destruction. End-to-end deep
processing requires **at least 2 MiB of native thread stack**, including result
destruction and error/cancellation cleanup. GLIDE-owned runtime workers enforce
this minimum and preserve larger `RUST_MIN_STACK` settings. Node's existing
pinned-worker pool cannot override thread stacks, so creating it with an explicit
smaller setting fails instead of starting unsupported workers. Configure
`RUST_MIN_STACK` before any Rust threads start; changing it later is unsupported
because Rust caches that setting.

Externally owned/custom Tokio runtimes and synchronous foreign-caller threads
are not resized: their owners must provide the same minimum. This also applies
to the calling thread of Rust's blocking client. The 64 KiB wire-parser test
checks iterative **construction only**, manually draining the result afterward;
it is not a small-stack destruction guarantee. Separate 2 MiB regressions cover
full graph decoding and ordinary destruction at the 128-level logical boundary,
512-level mixed wire aggregates, malformed/partial frames, timeout and
cancellation cleanup. Arbitrarily small stacks are not supported.

Pass date/time/point/vector inputs as supported scalars or lists into Cypher
constructors. Arbitrary native objects, graph entities, and native temporal
objects are not implicitly serialized. Graph names and query text cannot contain
embedded NUL because v6.0.3 truncates some command inputs at that byte.

TypeScript graph integer results and entity IDs are `bigint`, including values
outside JavaScript's safe-number range. Use `bigint` for exact int64 parameters;
unsafe integer `number` inputs are rejected. This does **not** change integer
results from ordinary GLIDE commands.

The optional graph server timeout is a nonnegative millisecond count within the
server's signed 64-bit range. Zero keeps its server-defined meaning. It is
also subject to the server's separately configured `TIMEOUT_MAX` limit. It is
separate from the configured GLIDE client request timeout. One client deadline
covers native adaptation and worker queueing, core serialization, query execution, metadata reads, permitted redirects,
compact decoding and canonical result encoding. Metadata does not get a fresh
end-to-end budget. Native bridges capture the submission time before copying or
parsing the request, rather than when a worker first polls it. An already-expired
queued operation does not dispatch its graph command. Synchronous decoding cannot be preempted; if it finishes after
the deadline, the operation returns the normal client timeout rather than a late
success.
Binding-side validation and protobuf request encoding occur before native
submission and are outside this native request budget.

## Results

Graph query results contain ordered columns, ordered rows and statistics. Rows are
not dictionaries indexed by column name: duplicate column names in a valid
payload are preserved. The pinned server itself rejects some queries that
produce duplicate aliases.

The shared decoder supports every compact tag emitted by v6.0.3:

| Kind | Representation |
| --- | --- |
| Null, string, integer, boolean, double | Native scalar, with exact graph int64 values |
| List and map                            | Recursive values using each binding's map representation          |
| Node | ID, label names and named properties |
| Relationship | ID, type name, source/destination IDs and named properties |
| Path | Ordered nodes and relationships, with validated connectivity |
| Point | Latitude and longitude |
| Float32 vector | Float32 values, without changing generic array results |
| Datetime, date, time, duration | Distinct lossless temporal wrappers |

Temporal payloads preserve the released server's integer encoding. In particular,
duration uses an offset encoding, not an ordinary elapsed-duration count. Do not
silently convert these wrappers into a language's local datetime or duration.

Known statistics are optional; absent counts are not fabricated as zero. Raw and
unknown statistics remain available. Statistics-only writes and empty result
sets are valid. Invalid UTF-8, unknown tags, malformed lengths, unresolved schema
IDs, invalid paths and embedded server errors fail the whole operation rather
than returning partially decoded success.

## Routing, schema consistency and retries

`GRAPH.QUERY` always targets the graph key's primary. `GRAPH.RO_QUERY` honors the
client's configured replica-read strategy; the driver does not infer read-only
behavior from Cypher. Graph key hash tags work like other Redis/Valkey keys.
Raw graph custom commands receive the same corrected routing without changing
their response shapes.

For entities, the core resolves only the required dictionaries through read-only
`CALL db.labels()`, `CALL db.relationshipTypes()` and `CALL db.propertyKeys()`
queries. Query and metadata use the **same existing multiplexed connection**,
including on replicas and after redirects. Under `ASK`, each metadata operation
also sends its own atomic `ASKING`/query transport unit.

Scalar-only queries issue no metadata commands. Each required dictionary is
fetched at most once per operation, not once per entity or row. Dictionaries are
operation-scoped and released with the operation. There is no process-global
graph registry and no unbounded per-graph cache.

This is a deliberate correctness tradeoff: v6.0.3 does not expose a documented
graph-incarnation identifier. Its `Graph version` statistic is a **write**
version, not the query's schema token. External delete/recreate or restore can
reuse schema counters, so cross-query cache reuse would risk stale names.
Warm entity queries therefore still fetch required metadata; concurrent
operations do not assume that another operation's dictionary belongs to their
graph incarnation.

Concurrent external replacement of a graph between the query and its metadata
reads cannot be detected reliably by this server contract. Applications must
coordinate graph deletion, restoration and replacement with readers. Do not
treat a matching schema counter as proof of incarnation identity.

Only explicit `MOVED`/`ASK` replies, which reject execution, permit bounded
rerouting. Completed attempts release their inflight slot before redirect or
metadata admission, including when the client has a one-request limit.
The graph path does not add transport replay after timeout,
disconnect, runtime error, malformed result or failed metadata read. A successful
write is **never rerun** to recover its result. An error after execution may mean
that a write already committed; application retries must account for that.

## Scope and verification

The initial API covers individual query and read-only-query operations.
Management wrappers, OGM, Bolt, bulk loading, streaming, typed graph pipelines,
transactions and graph-specific pools are not included. Existing raw custom
command batches remain available and return raw responses. Typed graph queries
reject an open raw `MULTI` transaction before queuing any command.

The dedicated harness and graph workflow pin the server artifact and exercise
RESP2/RESP3, standalone, replicas, cluster routing and protocol fixtures. Each
binding has a separate graph target rather than silently relying on a plain
Valkey job. See [the harness guide](../utils/falkordb_README.md) and the binding
developer guides for commands. Cleanup must use that run's state file and stop
only its containers.

The shared Criterion target measures scalar-, entity-, nested- and vector-heavy
serialization/decoding at 1, 16 and 256 rows, including allocation counts and
cumulative requested bytes:

```bash
cargo bench --manifest-path glide-core/Cargo.toml --features proto --bench graph_benchmark
```

For a bounded development check, add
`--profile dev -- --warm-up-time 0.1 --measurement-time 0.2 --sample-size 10 --noplot`.
Development-profile timings are not production throughput claims. Scalar-only
queries require zero dictionary requests; a 256-entity result sharing one label
dictionary requires one dictionary request, not 256. Warm queries deliberately
repeat required dictionaries because cross-query incarnation validation is not
available.

Published GLIDE documentation is maintained in `valkey-io/valkey-glide-docs`.
Publishing this guide there is a separate follow-up; this change does not modify
that repository or the external C#/PHP/Ruby clients.
