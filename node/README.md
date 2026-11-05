# Welcome to Valkey GLIDE!

Valkey General Language Independent Driver for the Enterprise (GLIDE) is the official open-source Valkey client library, proudly part of the Valkey organization. Our mission is to make your experience with Valkey and Redis OSS seamless and enjoyable. Whether you're a seasoned developer or just starting out, Valkey GLIDE is here to support you every step of the way.

# Why Choose Valkey GLIDE?

- **Community and Open Source**: Join our vibrant community and contribute to the project. We are always here to respond, and the client is for the community.
- **Reliability**: Built with best practices learned from over a decade of operating Redis OSS-compatible services.
- **Performance**: Optimized for high performance and low latency.
- **High Availability**: Designed to ensure your applications are always up and running.
- **Cross-Language Support**: Implemented using a core driver framework written in Rust, with language-specific extensions to ensure consistency and reduce complexity.
- **Stability and Fault Tolerance**: We brought our years of experience to create a bulletproof client.
- **Backed and Supported by AWS and GCP**: Ensuring robust support and continuous improvement of the project.

## Documentation

See GLIDE's Node.js [documentation site](https://glide.valkey.io/languages/nodejs).

## Supported Engine Versions

Refer to the [Supported Engine Versions table](https://github.com/valkey-io/valkey-glide/blob/main/README.md#supported-engine-versions) for details.

## Typed FalkorDB graphs

The additive graph API targets FalkorDB **v6.0.3**. It is separate from ordinary
Valkey commands and requires a FalkorDB server; compile-only checks do not establish
compatibility with other FalkorDB versions.

```typescript
import { GlideClient, GraphNode } from "@valkey/valkey-glide";

const client = await GlideClient.createClient({
    addresses: [{ host: "localhost", port: 6379 }],
});

try {
    // Local selection: no network request and no additional connection.
    const graph = client.selectGraph("people");
    const result = await graph.query(
        "CREATE (n:Person {name:$name,externalId:$id}) RETURN n",
        { params: { name: "Ada", id: 9007199254740993n }, timeoutMs: 1000 },
    );
    const node = result.rows[0][0];

    if (node instanceof GraphNode) {
        console.log(node.id, node.properties.get("name"));
    }

    await graph.roQuery("MATCH (n:Person) RETURN n.name");
} finally {
    client.close();
}
```

`query` and `roQuery` return `GraphQueryResult` with ordered `columns`, ordered
`rows`, and `statistics`. Duplicate column names are preserved. Graph integers,
entity IDs and statistic counters are always `bigint`; ordinary command number
conversion is unchanged. `GraphMap.entries` preserves map ordering and duplicate
keys, and `.get(key)` returns the first matching value.

The sixteen graph value kinds include native scalar values and lists,
`GraphMap`, `GraphNode`, `GraphRelationship`, `GraphPath`, `GraphPoint`,
`GraphVector`, `GraphDateTime`, `GraphDate`, `GraphTime`, and `GraphDuration`.
Temporal wrappers retain the exact engine `bigint` in `.value`, without
JavaScript `Date`, timezone, or millisecond conversion. Vectors preserve the
engine's float32 elements widened to JavaScript numbers. Known statistics are
optional; `statistics.raw` retains unrecognized server statistics.

Parameters accept null, booleans, well-formed Unicode strings, finite numbers,
signed-64-bit `bigint`, dense arrays and plain nested objects. Use `bigint` for
integers beyond JavaScript's safe integer range. Cycles, accessors, symbols,
native objects (including `Date`, `Map`, buffers), graph entities and nonfinite
numbers are rejected before dispatch. Query parameters remain separate from
query text; the shared Rust core performs Cypher literal serialization.
`timeoutMs` is an optional nonnegative safe integer server-side timeout, separate
from the client's overall request timeout; explicit zero is preserved.
Server-reported graph timeout expiration is a `RequestError`, distinct from a
client-side request deadline reported as `TimeoutError`.

A graph handle retains its parent client and selected database generation.
Its opaque native handle owns the Rust graph context; in-flight requests clone
that context before dispatch, and garbage collection releases handle ownership.
Closing the parent or attempting `SELECT` invalidates existing graph handles,
even when selecting away and back. Select a fresh graph after a successful
database change. Borrowed pool clients and isolated scopes do not expose usable
graph handles.

Graph-specific validation is isolated from plain-Valkey jobs:

```bash
cd node
npm run build-protobuf
npm run test:graph-unit
# After building the native addon:
npm run test:graph-native
# After building the native addon, point at the pinned harness endpoints:
FALKORDB_STANDALONE_ENDPOINT=127.0.0.1:6379 \
FALKORDB_REPLICA_ENDPOINT=127.0.0.1:6380 \
FALKORDB_CLUSTER_ENDPOINT=127.0.0.1:7000 npm run test:graph
```

The dedicated conformance target covers the shared recorded dataset under
RESP2/RESP3 in standalone/cluster mode, exact integers, schema recreation,
concurrency, close and pool/database lifetimes. Configured-replica read tests
observe the entity query and dictionary queries on the replica's MONITOR stream
under both protocols. See
`../utils/falkordb_harness.py` for digest-pinned endpoint provisioning.

# Getting Started - Node Wrapper

## System Requirements

The release of Valkey GLIDE was tested on the following platforms:

### Linux GNU

Linux with **glibc 2.17** or higher.

### MacOS (Darwin)

MacOS Apple Silicon/aarch_64 and x86_64/amd64.

- Full tests are running on MacOS 15.0 arm64/aarch64
- Minimal tests are running on: MacOS 13.5 x86*64/amd64*(We do not recommend using MacOS Intel for production, It is supported for development purposes)\_

### Alpine

All alpine versions that are using _musl libc_ 1.2.3 (All Alpine non deprecated alpine versions) or higher should be supported.
Tests are running on:

- node:alpine (x86_64/amd64 and arm64/aarch64)

## NodeJS supported version

Node.js 16 or higher.
**For npm users on linux it is recommended to use npm >=11 since it support optional download base on libc, yarn users should not be concerned**

- Note: The library is dependent on the [protobufjs library](https://protobufjs.github.io/protobuf.js/#installation), which add a size to the package. The package is using the protobufjs/minimal version, hence, if size matter, bundlers should be able to strip the unused code. It should reduce the size of the dependency from 19kb gzipped to 6.5kb gzipped.

### Building & Testing

Development instructions for local building & testing the package are in the [DEVELOPER.md](https://github.com/valkey-io/valkey-glide/blob/main/node/DEVELOPER.md#build-from-source) file.

# Quick Start

## Installation

```bash
npm i @valkey/valkey-glide
```

## Basic Examples

#### Standalone Mode:

```typescript
import { GlideClient, Logger } from "@valkey/valkey-glide";

Logger.setLoggerConfig("info");

const addresses = [
    {
        host: "localhost",
        port: 6379,
    },
];

// Check `GlideClientConfiguration` for additional options.
const client = await GlideClient.createClient({
    addresses,
    // Enable TLS if required by the server.
    // useTLS: true,
    // Set a timeout appropriate for your use case.
    requestTimeout: 500,
    clientName: "test_standalone_client",
});

try {
    // Pass the command and its arguments as an array.
    const pong = await client.customCommand(["PING"]);
    Logger.log("info", "app", `PING response: ${pong}`);

    const setResponse = await client.set("foo", "bar");
    Logger.log("info", "app", `Set response is: ${setResponse}`);

    const getResponse = await client.get("foo");
    Logger.log("info", "app", `Get response is: ${getResponse}`);
} finally {
    client.close();
}
```

#### Cluster Mode:

```typescript
import { GlideClusterClient, Logger } from "@valkey/valkey-glide";

Logger.setLoggerConfig("info");

// Add the address of any cluster node; the client discovers the remaining nodes.
const addresses = [
    {
        host: "localhost",
        port: 6379,
    },
];

// Check `GlideClusterClientConfiguration` for additional options.
const client = await GlideClusterClient.createClient({
    addresses,
    // Enable TLS if required by the cluster nodes.
    // useTLS: true,
    // Set a timeout appropriate for your use case.
    requestTimeout: 500,
    clientName: "test_cluster_client",
});

try {
    // Pass the command and its arguments as an array.
    const pong = await client.customCommand(["PING"], {
        route: "randomNode",
    });
    Logger.log("info", "app", `PING response: ${pong}`);

    const setResponse = await client.set("foo", "bar");
    Logger.log("info", "app", `Set response is: ${setResponse}`);

    const getResponse = await client.get("foo");
    Logger.log("info", "app", `Get response is: ${getResponse}`);
} finally {
    client.close();
}
```

### Supported platforms

Currently, the package is tested on:

| Operation systems | C lib                | Architecture      |
| ----------------- | -------------------- | ----------------- |
| `Linux`           | `glibc`, `musl libc` | `x86_64`, `arm64` |
| `macOS`           | `Darwin`             | `x86_64`, `arm64` |

## Community and Feedback

We encourage you to join our community to support, share feedback, and ask questions. You can approach us for anything on our Valkey Slack: [Join Valkey Slack](https://join.slack.com/t/valkey-oss-developer/shared_invite/zt-2nxs51chx-EB9hu9Qdch3GMfRcztTSkQ).
