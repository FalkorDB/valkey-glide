/**
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 *
 * Dedicated FalkorDB v6.0.3 tests: never included in the plain-Valkey suites.
 */

import { afterAll, beforeAll, describe, expect, it } from "@jest/globals";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { createInterface } from "node:readline";
import {
    ClientPool,
    ClosingError,
    Decoder,
    GlideClient,
    GlideClusterClient,
    GraphDate,
    GraphDateTime,
    GraphDuration,
    GraphMap,
    GraphNode,
    GraphPath,
    GraphPoint,
    GraphQueryResult,
    GraphRelationship,
    GraphTime,
    GraphVector,
    ProtocolVersion,
    RequestError,
    TimeoutError,
} from "../build-ts";

interface Fixture {
    cases: { name: string; steps: { command: string[] }[] }[];
}

interface ReplicaObservation {
    endpoint: string;
    graph: string;
    role: string;
    counts: {
        query: number;
        labels: number;
        relationship_types: number;
        property_keys: number;
        write: number;
    };
    commands: {
        command: string;
        query: string;
        client: string;
        db: number;
        kind: string;
    }[];
    complete: boolean;
    same_connection: boolean;
}

const fixture = JSON.parse(
    readFileSync(
        resolve(__dirname, "../../glide-core/tests/fixtures/graph/resp2.json"),
        "utf8",
    ),
) as Fixture;
const allTypesQuery = fixture.cases.find(({ name }) => name === "all_tags")!
    .steps[0].command[2];

function address(endpoint: string | undefined, variable: string) {
    if (!endpoint)
        throw new Error(
            `${variable}=host:port is required; run utils/falkordb_harness.py (pinned v6.0.3)`,
        );
    const url = new URL(`redis://${endpoint}`);
    return { host: url.hostname, port: Number(url.port) };
}

async function observeReplicaGraph(
    endpoint: string,
    graph: string,
    query: () => Promise<GraphQueryResult>,
): Promise<{ result: GraphQueryResult; observation: ReplicaObservation }> {
    const monitor = spawn("python3", [
        resolve(__dirname, "../../utils/falkordb_monitor.py"),
        "--endpoint",
        endpoint,
        "--graph",
        graph,
        "--timeout",
        "30",
    ]);
    let diagnostic = "";
    monitor.stderr.setEncoding("utf8");
    monitor.stderr.on("data", (chunk: string) => {
        diagnostic += chunk;
    });
    monitor.stdin.on("error", (error) => {
        diagnostic += error.message;
    });
    const completion = new Promise<number | null>((resolve) => {
        monitor.once("error", (error) => {
            diagnostic += error.message;
            resolve(null);
        });
        monitor.once("close", resolve);
    });
    const lines = createInterface({ input: monitor.stdout });
    const output = lines[Symbol.asyncIterator]();

    const readLine = async () => {
        const line = await output.next();

        if (line.done) {
            const code = await completion;

            throw new Error(
                `Replica MONITOR ended with code ${code}: ${diagnostic}`,
            );
        }

        return line.value;
    };

    try {
        expect(await readLine()).toBe("READY");
        const result = await query();
        monitor.stdin.end("STOP\n");
        const observation = JSON.parse(await readLine()) as ReplicaObservation;
        const code = await completion;
        expect({ code, diagnostic }).toEqual({ code: 0, diagnostic: "" });
        return { result, observation };
    } finally {
        monitor.stdin.end();

        if (monitor.exitCode === null && monitor.signalCode === null)
            monitor.kill("SIGTERM");
        await completion;
        lines.close();
    }
}

describe.each([false, true])(
    "FalkorDB pinned conformance cluster=%s",
    (cluster) => {
        describe.each([ProtocolVersion.RESP2, ProtocolVersion.RESP3])(
            "protocol=%s",
            (protocol) => {
                let client: GlideClient | GlideClusterClient;
                const graphs: string[] = [];
                const endpointVariable = cluster
                    ? "FALKORDB_CLUSTER_ENDPOINT"
                    : "FALKORDB_STANDALONE_ENDPOINT";
                const config = () => ({
                    addresses: [
                        address(
                            process.env[endpointVariable],
                            endpointVariable,
                        ),
                    ],
                    protocol,
                    requestTimeout: 10_000,
                    defaultDecoder: Decoder.Bytes,
                });

                const name = () => {
                    const value = `glide-node-graph:{${randomUUID()}}`;
                    graphs.push(value);
                    return value;
                };

                const create = () =>
                    cluster
                        ? GlideClusterClient.createClient(config())
                        : GlideClient.createClient(config());

                beforeAll(async () => {
                    client = await create();
                });
                afterAll(async () => {
                    if (client) {
                        for (const graph of graphs) await client.del([graph]);
                        client.close();
                    }
                });

                it("materializes all sixteen recorded kinds, IDs, properties and temporal scalars", async () => {
                    const result: GraphQueryResult = await client
                        .selectGraph(name())
                        .query(allTypesQuery);
                    const row = result.rows[0];
                    expect(result.columns).toHaveLength(16);
                    expect(row.slice(0, 5)).toEqual([
                        null,
                        "text",
                        -(1n << 63n),
                        true,
                        1.5,
                    ]);
                    expect(row[5]).toEqual([
                        1n,
                        null,
                        new GraphMap([["nested", [false, "x"]]]),
                    ]);
                    expect(row[6]).toBeInstanceOf(GraphRelationship);
                    expect(row[7]).toBeInstanceOf(GraphNode);
                    const node = row[7] as GraphNode;
                    const edge = row[6] as GraphRelationship;
                    expect(node.labels).toEqual(["Person", "Researcher"]);
                    expect(node.properties.get("name")).toBe("Ada");
                    expect(node.properties.get("age")).toBe(42n);
                    expect(typeof node.id).toBe("bigint");
                    expect(edge.type).toBe("KNOWS");
                    expect(edge.sourceId).toBe(node.id);
                    expect(edge.properties.get("since")).toBe(2020n);
                    expect(row[8]).toBeInstanceOf(GraphPath);
                    expect((row[8] as GraphPath).nodes).toHaveLength(2);
                    expect(row[9]).toBeInstanceOf(GraphMap);
                    expect(row[10]).toEqual(new GraphPoint(1.5, 2.5));
                    expect(row[11]).toEqual(new GraphVector([1, 2.5, -3]));
                    expect(row[12]).toEqual(new GraphDateTime(1577934245n));
                    expect(row[13]).toEqual(new GraphDate(1577923200n));
                    expect(row[14]).toEqual(new GraphTime(11045n));
                    expect(row[15]).toEqual(new GraphDuration(36907506n));
                    expect(result.statistics.nodesCreated).toBe(2n);
                    expect(result.statistics.raw.length).toBeGreaterThan(0);
                });

                it("resolves entity dictionaries with inflightRequestsLimit=1", async () => {
                    const limitedConfig = {
                        ...config(),
                        inflightRequestsLimit: 1,
                    };
                    const limited = cluster
                        ? await GlideClusterClient.createClient(limitedConfig)
                        : await GlideClient.createClient(limitedConfig);

                    try {
                        const graph = limited.selectGraph(name());
                        const result = await graph.query(
                            "CREATE (a:LimitOne {name:'a'})-[r:LIMIT_ONE {weight:1}]->(b:LimitOne {name:'b'}) RETURN a,r",
                        );
                        const node = result.rows[0][0] as GraphNode;
                        const relationship = result
                            .rows[0][1] as GraphRelationship;
                        expect(node).toBeInstanceOf(GraphNode);
                        expect(node.labels).toEqual(["LimitOne"]);
                        expect(node.properties.get("name")).toBe("a");
                        expect(relationship).toBeInstanceOf(GraphRelationship);
                        expect(relationship.type).toBe("LIMIT_ONE");
                        expect(relationship.properties.get("weight")).toBe(1n);
                        const readonly = await graph.roQuery(
                            "MATCH (a)-[r:LIMIT_ONE]->() RETURN a,r",
                        );
                        expect(readonly.rows[0][0]).toBeInstanceOf(GraphNode);
                        expect(readonly.rows[0][1]).toBeInstanceOf(
                            GraphRelationship,
                        );
                    } finally {
                        limited.close();
                    }
                });

                it("preserves exact parameter integers, nested literals and unicode", async () => {
                    const graph = client.selectGraph(name());
                    const unicode = "'\\\n\r\t\0\u0001界😀 // RETURN 999";
                    const result = await graph.query(
                        "RETURN $min AS min_value, $max AS max_value, $text AS text, $list AS nested",
                        {
                            params: {
                                min: -(1n << 63n),
                                max: (1n << 63n) - 1n,
                                text: unicode,
                                list: [true, null, { x: 1n }],
                            },
                            timeoutMs: 0,
                        },
                    );
                    expect(result.columns).toEqual([
                        "min_value",
                        "max_value",
                        "text",
                        "nested",
                    ]);
                    expect(result.rows).toEqual([
                        [
                            -(1n << 63n),
                            (1n << 63n) - 1n,
                            unicode,
                            [true, null, new GraphMap([["x", 1n]])],
                        ],
                    ]);
                    // The graph path must not alter ordinary NAPI integer or decoder behavior.
                    const counter = name();
                    expect(await client.incr(counter)).toBe(1);
                });

                it("expires a positive server timeout as RequestError and recovers with timeoutMs=0", async () => {
                    const graph = client.selectGraph(name());
                    await graph.query("CREATE (:ServerTimeout)");
                    const expiration = graph.roQuery(
                        "UNWIND range(1,10000) AS x UNWIND range(1,10000) AS y RETURN sum(x*y)",
                        { timeoutMs: 1 },
                    );
                    await expect(expiration).rejects.toBeInstanceOf(
                        RequestError,
                    );
                    await expect(expiration).rejects.not.toBeInstanceOf(
                        TimeoutError,
                    );
                    await expect(expiration).rejects.toHaveProperty(
                        "name",
                        "RequestError",
                    );
                    await expect(expiration).rejects.toThrow(/timed out/i);
                    expect(
                        (
                            await graph.roQuery("RETURN 42", {
                                timeoutMs: 0,
                            })
                        ).rows,
                    ).toEqual([[42n]]);
                });

                it("keeps local selection lazy and read-only routing rejects writes", async () => {
                    const graphName = name();
                    const graph = client.selectGraph(graphName);
                    expect(graph.name).toBe(graphName);
                    expect(await client.exists([graphName])).toBe(0);
                    await graph.query("CREATE (:ReadOnlyTest)");
                    const readonly = await graph.roQuery("MATCH (n) RETURN n");
                    expect(readonly.rows[0][0]).toBeInstanceOf(GraphNode);
                    await expect(
                        graph.roQuery("CREATE (:Forbidden)"),
                    ).rejects.toThrow();
                });

                it("selects a graph locally even when a lazy parent's endpoint is unreachable", async () => {
                    const lazyConfig = {
                        ...config(),
                        addresses: [{ host: "127.0.0.1", port: 1 }],
                        lazyConnect: true,
                    };
                    const parent = cluster
                        ? await GlideClusterClient.createClient(lazyConfig)
                        : await GlideClient.createClient(lazyConfig);

                    try {
                        const graph = parent.selectGraph("local-only");
                        expect(graph.name).toBe("local-only");
                        expect(graph.database).toBe(0);
                    } finally {
                        parent.close();
                    }
                });

                it("recreates dictionaries without retaining stale schema, and preserves empty results", async () => {
                    const graphName = name();
                    const graph = client.selectGraph(graphName);
                    const before = await graph.query(
                        "CREATE (n:Before {old:1}) RETURN n",
                    );
                    expect((before.rows[0][0] as GraphNode).labels).toEqual([
                        "Before",
                    ]);
                    await client.customCommand(["GRAPH.DELETE", graphName]);
                    const after = await graph.query(
                        "CREATE (n:After {new:2}) RETURN n",
                    );
                    expect((after.rows[0][0] as GraphNode).labels).toEqual([
                        "After",
                    ]);
                    expect(
                        (after.rows[0][0] as GraphNode).properties.get("new"),
                    ).toBe(2n);
                    expect(
                        (await graph.roQuery("MATCH (n:Missing) RETURN n"))
                            .rows,
                    ).toEqual([]);
                });

                it("concurrent handles stay isolated and failed query decoding does not poison callbacks", async () => {
                    const handles = Array.from({ length: 4 }, () =>
                        client.selectGraph(name()),
                    );
                    const values = await Promise.all(
                        handles.map((graph, index) =>
                            graph.query(
                                "CREATE (n:Concurrent {value:$value}) RETURN n",
                                { params: { value: index } },
                            ),
                        ),
                    );
                    values.forEach((value, index) =>
                        expect(
                            (value.rows[0][0] as GraphNode).properties.get(
                                "value",
                            ),
                        ).toBe(BigInt(index)),
                    );
                    await expect(
                        handles[0].query("NOT A CYPHER QUERY"),
                    ).rejects.toThrow();
                    expect((await handles[0].roQuery("RETURN 1")).rows).toEqual(
                        [[1n]],
                    );
                });

                it("invalidates borrowed handles when the parent closes", async () => {
                    const parent = await create();
                    const graph = parent.selectGraph(name());
                    const inflight = graph.query("RETURN 1");
                    parent.close();
                    await expect(inflight).rejects.toBeInstanceOf(ClosingError);
                    await expect(
                        graph.query("RETURN 1"),
                    ).rejects.toBeInstanceOf(ClosingError);
                    expect(() => parent.selectGraph("closed")).toThrow(
                        ClosingError,
                    );
                });

                it("rejects graph handles on pool-borrowed clients before they can escape the lease", async () => {
                    const pool = await ClientPool.create(config(), {
                        clusterMode: cluster,
                        minIdle: 1,
                        maxSize: 1,
                    });

                    try {
                        const borrowed = await pool.acquire();
                        expect(() => borrowed.selectGraph(name())).toThrow(
                            /pool/i,
                        );
                        const scope = await borrowed.scopedConnection(name());

                        try {
                            expect("selectGraph" in scope).toBe(false);
                        } finally {
                            scope.release();
                        }

                        expect("selectGraph" in scope).toBe(false);
                        await pool.release(borrowed);
                        expect(() => borrowed.selectGraph(name())).toThrow();
                    } finally {
                        await pool.close();
                    }
                });

                it("does not expose graph selection through isolated scopes before or after release", async () => {
                    const scope = await client.scopedConnection(name());

                    try {
                        expect("selectGraph" in scope).toBe(false);
                        expect("clientHandle" in scope).toBe(false);
                    } finally {
                        scope.release();
                    }

                    expect(scope.isReleased).toBe(true);
                    expect("selectGraph" in scope).toBe(false);
                });

                if (!cluster) {
                    it("uses configured replica for RO_QUERY entities and every metadata dictionary", async () => {
                        const replicaEndpoint =
                            process.env.FALKORDB_REPLICA_ENDPOINT;
                        const replicaAddress = address(
                            replicaEndpoint,
                            "FALKORDB_REPLICA_ENDPOINT",
                        );
                        const graphName = name();
                        await client
                            .selectGraph(graphName)
                            .query(
                                "CREATE (a:ReplicaPerson {name:'Ada'})-[r:REPLICA_LINK {weight:7}]->(b:ReplicaPerson {name:'Grace'}) RETURN a,r,b",
                            );
                        expect(
                            await client.customCommand(["WAIT", "1", "10000"]),
                        ).toBeGreaterThanOrEqual(1);
                        const reader = await GlideClient.createClient({
                            ...config(),
                            addresses: [...config().addresses, replicaAddress],
                            readFrom: "preferReplica",
                        });
                        const query =
                            "MATCH (a:ReplicaPerson {name:'Ada'})-[r:REPLICA_LINK]->(b) RETURN a,r,b";

                        try {
                            const { result, observation } =
                                await observeReplicaGraph(
                                    replicaEndpoint!,
                                    graphName,
                                    () =>
                                        reader
                                            .selectGraph(graphName)
                                            .roQuery(query),
                                );
                            expect(result.rows).toHaveLength(1);
                            const [source, relationship, destination] = result
                                .rows[0] as [
                                GraphNode,
                                GraphRelationship,
                                GraphNode,
                            ];
                            expect(source).toBeInstanceOf(GraphNode);
                            expect(source.labels).toEqual(["ReplicaPerson"]);
                            expect(source.properties.get("name")).toBe("Ada");
                            expect(relationship).toBeInstanceOf(
                                GraphRelationship,
                            );
                            expect(relationship.type).toBe("REPLICA_LINK");
                            expect(relationship.properties.get("weight")).toBe(
                                7n,
                            );
                            expect(destination).toBeInstanceOf(GraphNode);
                            expect(destination.properties.get("name")).toBe(
                                "Grace",
                            );
                            expect(relationship.sourceId).toBe(source.id);
                            expect(relationship.destinationId).toBe(
                                destination.id,
                            );
                            expect(observation).toMatchObject({
                                endpoint: replicaEndpoint,
                                graph: graphName,
                                role: "replica",
                                complete: true,
                                same_connection: true,
                                counts: {
                                    query: 1,
                                    labels: 1,
                                    relationship_types: 1,
                                    property_keys: 1,
                                    write: 0,
                                },
                            });
                            expect(
                                observation.commands.map(
                                    ({ command }) => command,
                                ),
                            ).toEqual(Array(4).fill("GRAPH.RO_QUERY"));
                            expect(
                                observation.commands.map(({ kind }) => kind),
                            ).toEqual([
                                "query",
                                "labels",
                                "relationship_types",
                                "property_keys",
                            ]);
                            expect(observation.commands[0].query).toBe(query);
                            expect(
                                observation.commands
                                    .slice(1)
                                    .map(({ query }) => query),
                            ).toEqual([
                                "CALL db.labels()",
                                "CALL db.relationshipTypes()",
                                "CALL db.propertyKeys()",
                            ]);
                            expect(
                                new Set(
                                    observation.commands.map(
                                        ({ client }) => client,
                                    ),
                                ).size,
                            ).toBe(1);
                            expect(
                                observation.commands.every(
                                    ({ db }) => db === 0,
                                ),
                            ).toBe(true);
                            console.info(
                                JSON.stringify({
                                    protocol:
                                        protocol === ProtocolVersion.RESP2
                                            ? "RESP2"
                                            : "RESP3",
                                    ...observation,
                                }),
                            );
                        } finally {
                            reader.close();
                        }
                    });

                    it("rejects typed graph queries inside raw MULTI without queuing a write", async () => {
                        const graph = client.selectGraph(name());
                        await graph.query("CREATE (:BeforeTransaction)");
                        await client.customCommand(["MULTI"]);

                        try {
                            await expect(
                                graph.query("CREATE (:ForbiddenTransaction)"),
                            ).rejects.toThrow(/MULTI/);
                            await expect(
                                graph.roQuery("RETURN 1"),
                            ).rejects.toThrow(/MULTI/);
                        } finally {
                            await client.customCommand(["EXEC"]);
                        }

                        expect(
                            (
                                await graph.roQuery(
                                    "MATCH (n:ForbiddenTransaction) RETURN count(n)",
                                )
                            ).rows,
                        ).toEqual([[0n]]);
                    });

                    it("invalidates old graph snapshots after SELECT away and back", async () => {
                        const graph = client.selectGraph(name());
                        await client.select(1);
                        await expect(graph.query("RETURN 1")).rejects.toThrow();
                        await client.select(0);
                        await expect(graph.query("RETURN 1")).rejects.toThrow();
                        expect(
                            (await client.selectGraph(name()).query("RETURN 1"))
                                .rows,
                        ).toEqual([[1n]]);
                    });

                    it("rejects selection after a failed SELECT until a successful reset", async () => {
                        await expect(client.select(999999)).rejects.toThrow();
                        expect(() => client.selectGraph(name())).toThrow(
                            /unsettled/,
                        );
                        await client.select(0);
                        expect(client.selectGraph(name()).database).toBe(0);
                    });
                }
            },
        );
    },
);
