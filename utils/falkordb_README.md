# FalkorDB graph compatibility harness

This is a dedicated **FalkorDB v6.0.3** gate, separate from ordinary Valkey jobs.
It uses Python's standard library and Docker; it does not build Rust or bindings.
Only synthetic, local test data is used.

## Run

```bash
# Fast recorder/fixture safety tests, without Docker:
python3 -m unittest discover -s utils -p falkordb_tests.py -v

# Replay exact fixtures and exercise standalone + replica + six-node cluster:
python3 utils/falkordb_tests.py --topology all

# Standalone-only replay:
python3 utils/falkordb_tests.py --topology standalone
```

Docker access is checked before starting. The immutable image is
`falkordb/falkordb@sha256:0fb85036662ce3d1206530a38fdf7df5a92b1fb31ad23087f8d4e89038ce0812`.
The index contains amd64 and arm64 images; fixtures were verified on Linux amd64.
The image is reused when present and is **never removed or pruned** by the harness.
Its `graph` module must report version `60003`.

The full run creates eight containers (one standalone, its replica, and three
cluster primary/replica pairs), with UUID names, a private per-run bridge,
random loopback-only published ports, and per-container in-memory data. Servers
run as uid 999 with a read-only root filesystem and `TEMP_FOLDER /data`; the image
browser is not started. Readiness requires `PING`, the exact module version,
replication links and cluster state. Replication assertions use `WAIT`, not sleeps.

## Share endpoints with dedicated binding tests

```bash
# Choose a new state file per independently running harness:
python3 utils/falkordb_harness.py start \
    --state .build/my-graph-run.json --topology all --format env

# The command prints exports for:
# FALKORDB_STANDALONE_ENDPOINT=127.0.0.1:<port>
# FALKORDB_REPLICA_ENDPOINT=127.0.0.1:<port>
# FALKORDB_CLUSTER_ENDPOINT=127.0.0.1:<seed-port>
#
# Export those values before running the binding's dedicated graph tests.
# Cluster tests must follow the topology, not assume the seed owns every key.

python3 utils/falkordb_harness.py stop --state .build/my-graph-run.json
```

Default JSON output and the persisted state also contain an `environment` map.
Starting refuses to overwrite an existing state. The manifest is saved before
resource creation so interrupted starts can be recovered. Creation failures and
the test runner clean up in `finally`; persistent `start` requires explicit `stop`.
Cleanup inspects **only exact recorded resource IDs/names**, checks both ownership
labels, removes their anonymous volumes, and refuses mismatched ownership.
Missing resources are harmless; other failures keep the manifest for recovery.
There is no global `FLUSH`, process-name kill, system prune, host sysctl change,
or deletion of unrelated files. Use unique graph keys in binding tests too.

## Shared replica MONITOR evidence helper

Every language's replica conformance test can use the same standard-library
subprocess instead of reimplementing MONITOR wire parsing:

```bash
python3 utils/falkordb_monitor.py \
    --endpoint "$FALKORDB_REPLICA_ENDPOINT" \
    --graph 'language-graph-test:{01234567-89ab-cdef-0123-456789abcdef}' \
    --timeout 30
```

Launch with stdin/stdout/stderr pipes, retaining stdin until the read finishes.
The graph key must contain only ASCII letters/digits, `_`, `:`, `{`, `}`, `.`,
or `-`, and be at most 512 characters. Setup must create the unique graph on its
primary and synchronize with `WAIT` **before** starting the helper.

The line-oriented contract is:

1. The helper connects only to the supplied endpoint, verifies that `ROLE`
   reports a replica (`slave`), and enters MONITOR. Only after receiving `+OK`
   does it print and flush the exact stdout line `READY`.
2. Run **one** normal binding `RO_QUERY` operation returning nodes, a relationship
   or path, and properties. Configure the binding's primary and replica addresses
   with its replica-read strategy; do not send a raw query directly to the replica
   as a substitute for testing that strategy.
3. After the operation succeeds, send `STOP\n` and flush, or close stdin for a
   clean EOF. The helper issues a unique `PING` on its own second connection and
   consumes the corresponding MONITOR event before reporting, so there is no
   timing-based drain or race with the final dictionary fetch.
4. Read one final JSON stdout line and wait for exit code zero:

```json
{
  "endpoint": "127.0.0.1:32791",
  "graph": "language-graph-test:{01234567-89ab-cdef-0123-456789abcdef}",
  "role": "replica",
  "counts": {
    "query": 1,
    "labels": 1,
    "relationship_types": 1,
    "property_keys": 1,
    "write": 0
  },
  "commands": [
    {
      "command": "GRAPH.RO_QUERY",
      "query": "MATCH p=()-->() RETURN p",
      "client": "172.25.0.1:12345",
      "db": 0,
      "kind": "query"
    }
  ],
  "complete": true,
  "same_connection": true
}
```

The example omits the three dictionary entries from `commands` for brevity;
actual reports retain every matching command in observation order. Metadata
classifications are `labels` for `CALL db.labels()`, `relationship_types` for
`CALL db.relationshipTypes()`, and `property_keys` for `CALL db.propertyKeys()`.
Other `GRAPH.RO_QUERY` statements are `query`; matching `GRAPH.QUERY` is `write`.
Only the **exact** graph key is recorded, never other tests' graph commands.
Each entry includes the server-observed physical client address and database.

Callers **must assert** `complete == true`, `same_connection == true`, and
`counts.write == 0`, preferably each of the four read counts exactly one for a
single entity operation. Exit code zero alone means observation succeeded; an
empty capture returns `complete: false`, not a false-positive proof.
`same_connection` considers every recorded read's client address **and** database.
The helper's transport is RESP2 MONITOR, independently of the binding query's
RESP2/RESP3 setting; run the binding test in both protocols.

Operational errors, a non-replica endpoint, unexpected stream closure, malformed
MONITOR data or timeout print `ERROR ...` to stderr and exit nonzero. Readiness is
never printed on a failed role check or MONITOR acknowledgment. The deadline
defaults to 30 seconds (configurable above zero through 600), including stop-fence
draining. Both owned sockets close on success or error; there are no files,
background daemon, server configuration changes, global cleanup or graph-result
decoding. A caller that aborts early should terminate and wait for only its own
child process.

## Captured protocol contract

`glide-core/tests/fixtures/graph/manifest.json` pins the source commit, inspected
source hashes, image digests, platform and verified/unproven scope. `resp2.json`
and `resp3.json` each record 46 commands and their actual recursive RESP frames:

```json
{"type": "array", "value": [{"type": "integer", "value": 3}]}
```

Each case has a fresh graph. Commands use `{graph}` as a replaceable key.
Execution durations alone are normalized to `<milliseconds>`. Every numeric
tag, cached flag, version, statistic and wire type is retained. The recorder
independently asserts semantics **before** writing goldens; normal runs compare
against the committed goldens. To intentionally refresh after review:

```bash
python3 utils/falkordb_tests.py --topology all --record
```

Compact query results are `[columns, rows, stats]`; statements without `RETURN`
produce `[stats]`. Columns are `[1,name]`. Every row value is `[tag,payload]`:

| Tag | Payload |
| --- | --- |
| 1–5 | null, string, i64, bulk `true`/`false`, bulk double text |
| 6 | array of tagged values |
| 7 | `[id,type_id,source_id,destination_id,properties]` |
| 8 | `[id,label_ids,properties]` |
| 9 | `[[6,tagged_nodes],[6,tagged_relationships]]` |
| 10 | alternating `[key,tagged_value,...]` |
| 11 | `[latitude,longitude]`, both bulk decimal strings |
| 12 | array of RESP2 bulk decimal strings / RESP3 native doubles |
| 13–16 | raw i64 datetime/date/time seconds and offset-encoded duration |

Entity properties are `[property_id,tag,payload]` triples. Temporal constructors
in the fixtures are `localdatetime`, `date`, `localtime`, and `duration`. The
duration encoding anchors year/month components at 1970-01-01 and adds day/time
components; it must not be presented as an ordinary elapsed-duration value.

The exact pre-execution schema rejection is
`[RESP error "ERR invalid graph version", current_schema_version]`.
It does not execute writes or create a missing graph. `Graph version:` in stats
is a **write version**, not the schema token.

Typed parameters use `CYPHER ` followed by backtick-quoted identifier assignments,
not positional command arguments. Nested map keys are Cypher identifiers, not
JSON double-quoted strings. The supported key subset excludes empty keys,
backticks and controls. Captures verify escaped apostrophe, backslash, newline,
tab, carriage return, backspace, form feed and `uXXXX`. `TIMEOUT` is a canonical
decimal integer from zero through i64 max, further bounded by positive
`TIMEOUT_MAX`; the schema `version` flag is zero through u32 max.

## Affinity and replay limits

`topology.json` records assertion outcomes for both RESP versions. The live
tests temporarily pause **only the owned replica's replication writes**, recreate
a graph on its primary, and show that the replica's old node IDs require its old
dictionaries. The primary's dictionaries would silently rename the same IDs.
Recreation also reuses schema versions, ruling out cross-query cache guarantees.

The cluster test proves `MOVED` and `ASK` rejection occurs before execution and
that query and dictionary fetches use the exact owning/importing connection.
`ASKING` permits **one** command: each subsequent dictionary query needs its own
`ASKING`+query pipeline. The fixture reader is a wire recorder, not a GLIDE client;
these tests do not claim client routing, ambiguous-failure replay, failover,
authentication or TLS coverage. Those require the dedicated binding/core tests.

## Dedicated API workflow

`.github/workflows/graph-compatibility.yml` first replays the wire fixtures, then
uses separate Linux-hosted matrix jobs for Rust/core, Go, Node, Java and Python.
Each API job builds its own native binding with the existing shared dependency
actions and binding build commands. No ordinary Valkey workflow is modified.
The Python job uses the existing `build-python-wrapper` action to build both
sync and async clients; Java uses its normal `graphTest` JNI build dependencies.

Every job creates its own UUID-named topology and distinct state file containing
the binding, workflow run ID and attempt. All three `FALKORDB_*_ENDPOINT` values
are exported through `GITHUB_ENV`. API tests run directly on the Linux runner:
loopback-published seeds and the cluster's advertised Docker bridge addresses
are reachable from the native clients. Jobs do not run inside another container.
An `always()` step removes only that job's recorded resources after success,
test/build failure or ordinary cancellation. Hard runner loss still relies on
the hosted runner's disposal; no machine-wide Docker cleanup is used.

The explicit test gates are:

| Job | Dedicated commands |
| --- | --- |
| Rust/core | `cargo test --manifest-path glide-core/Cargo.toml --features proto,socket-layer,standalone_heartbeat --lib graph::`; `cargo test --manifest-path glide-core/Cargo.toml --features proto,socket-layer,standalone_heartbeat --test test_graph`; `cargo test --manifest-path glide-core/Cargo.toml --features proto,socket-layer,standalone_heartbeat --test test_graph_topology -- --ignored --test-threads=1`; `cargo test --manifest-path rust/Cargo.toml --test graph -- --include-ignored --test-threads=1` |
| Go | `make -C go graph-test` |
| Node | `npm run test:graph-unit`; `npm run test:graph-native`; `npm run test:graph` (from `node/`) |
| Java | `GLIDE_DOCKER_SERVERS=true ./gradlew --build-cache :client:graphTest --max-workers=2` (from `java/`) |
| Python | `.env/bin/python -m pytest -o addopts= graph_tests` (from `python/`, after both clients are built) |

The core topology target exercises real `GraphContext` MOVED/ASK routing,
same-transport dictionary fetches, divergent replica schemas and exact-client-ID
reconnects under both RESP versions. A scoped ephemeral ACL user can decode paths
and all three metadata dictionaries using only `GRAPH.RO_QUERY` plus required
handshake/read commands; graph writes and reads outside its owned graph key must
be rejected. The user and key are deleted after the test, including on assertion
failure.

The target reserves an empty random slot, restores its original owner, and never
resets the cluster or flushes a database. Run it separately from other tests
needing the same replica because it briefly pauses replication writes.
Slot cleanup clears both nodes' importing/migrating state and uses a zero-key
reverse handoff when ownership changed, so the restored owner has a newer cluster
epoch. It waits for every primary and replica's slot view to converge before a
later test can promote a replica with stale ownership.

Actual cluster primary failover is additionally gated by
`FALKORDB_TEST_FAILOVER=1`. Without that exact value the failover case prints a
not-run notice and returns without changing roles. Enable it **only on an
exclusive harness**, never one still serving other binding tests:

```bash
FALKORDB_TEST_FAILOVER=1 cargo test --manifest-path glide-core/Cargo.toml \
    --features proto,socket-layer,standalone_heartbeat \
    --test test_graph_topology \
    graph_context_survives_actual_primary_failover_and_restores_topology \
    -- --ignored --test-threads=1
```

The case uses coordinated `CLUSTER FAILOVER` on the owning replica, proves
changed roles and slot ownership, exercises the retained core context and its
metadata on the new primary, then promotes the original primary back. Ordered
markers plus `WAIT` cover graph replication before transitions; cleanup deletes
only the test's graph/reservation and verifies restored roles, replication and
slot ownership. The RESP2 and RESP3 cases use distinct shards rather than
immediately reusing a replica's previous manual-election state after restoration.
Only the dedicated Rust/core CI step sets the opt-in flag; its job owns the
complete topology. No `FORCE`, `TAKEOVER`, global reset or unrelated key cleanup is
used.

Static workflow validation and local API runs are not evidence of a hosted CI
run: the workflow must still execute successfully on GitHub after publication.
