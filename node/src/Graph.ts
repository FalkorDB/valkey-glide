/**
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 */

/** Literal query parameters. Integers outside JavaScript's safe range require bigint. */
export type GraphParameter =
    | null
    | boolean
    | string
    | number
    | bigint
    | readonly GraphParameter[]
    | { readonly [key: string]: GraphParameter };

/** Options for GRAPH.QUERY and GRAPH.RO_QUERY. No routing/decoder overrides apply. */
export interface QueryOptions {
    params?: Readonly<Record<string, GraphParameter>>;
    /** Nonnegative, safe integer server timeout in milliseconds; zero is preserved. */
    timeoutMs?: number;
}

export type GraphQueryOptions = QueryOptions;

/** Ordered map entries, preserving duplicate keys rather than silently overwriting. */
export class GraphMap {
    readonly kind = "map";
    constructor(
        public readonly entries: readonly (readonly [string, GraphValue])[],
    ) {}

    /** Return the first matching value; inspect entries to retain duplicate keys. */
    get(key: string): GraphValue | undefined {
        return this.entries.find(([name]) => name === key)?.[1];
    }
}

export class GraphNode {
    readonly kind = "node";
    constructor(
        public readonly id: bigint,
        public readonly labels: readonly string[],
        public readonly properties: GraphMap,
    ) {}
}

export class GraphRelationship {
    readonly kind = "relationship";
    constructor(
        public readonly id: bigint,
        public readonly type: string,
        public readonly sourceId: bigint,
        public readonly destinationId: bigint,
        public readonly properties: GraphMap,
    ) {}
}

export class GraphPath {
    readonly kind = "path";
    constructor(
        public readonly nodes: readonly GraphNode[],
        public readonly relationships: readonly GraphRelationship[],
    ) {}
}

export class GraphPoint {
    readonly kind = "point";
    constructor(
        public readonly latitude: number,
        public readonly longitude: number,
    ) {}
}

export class GraphVector {
    readonly kind = "vector";
    /** Values are exact JavaScript-number widenings of the server's float32 elements. */
    constructor(public readonly values: readonly number[]) {}
}

/** Raw FalkorDB datetime epoch seconds, without lossy JavaScript Date conversion. */
export class GraphDateTime {
    readonly kind = "datetime";
    constructor(public readonly value: bigint) {}
}

/** Raw FalkorDB date epoch seconds, without timezone or calendar reinterpretation. */
export class GraphDate {
    readonly kind = "date";
    constructor(public readonly value: bigint) {}
}

/** Raw FalkorDB time offset, retained exactly as returned by the engine. */
export class GraphTime {
    readonly kind = "time";
    constructor(public readonly value: bigint) {}
}

/** Raw FalkorDB duration, retained exactly rather than converted to milliseconds. */
export class GraphDuration {
    readonly kind = "duration";
    constructor(public readonly value: bigint) {}
}

/** All sixteen FalkorDB value kinds. Every integer and entity ID is bigint. */
export type GraphValue =
    | null
    | string
    | bigint
    | boolean
    | number
    | GraphValue[]
    | GraphMap
    | GraphNode
    | GraphRelationship
    | GraphPath
    | GraphPoint
    | GraphVector
    | GraphDateTime
    | GraphDate
    | GraphTime
    | GraphDuration;

/** Known statistics are optional; raw preserves all server statistics verbatim. */
export interface GraphStatistics {
    raw: readonly string[];
    labelsAdded?: bigint;
    labelsRemoved?: bigint;
    nodesCreated?: bigint;
    nodesDeleted?: bigint;
    propertiesSet?: bigint;
    propertiesRemoved?: bigint;
    relationshipsCreated?: bigint;
    relationshipsDeleted?: bigint;
    indicesCreated?: bigint;
    indicesDeleted?: bigint;
    cachedExecution?: boolean;
    executionTimeMs?: number;
    graphVersion?: bigint;
}

export interface GraphQueryResult {
    /** Ordered column names; duplicates are intentionally preserved. */
    columns: readonly string[];
    /** Ordered rows, never converted into objects keyed by column names. */
    rows: readonly (readonly GraphValue[])[];
    statistics: GraphStatistics;
}

/**
 * Local view of a graph on an existing client and its selected database.
 * Selecting a graph creates no connection or server-side state. Closing the
 * parent invalidates this handle. Borrowed pool clients do not support graphs.
 * The native handle retains its Rust graph context; in-flight queries clone that
 * context, and garbage collection releases it automatically.
 */
export class Graph {
    /** @internal Created by BaseClient.selectGraph. */
    constructor(
        public readonly name: string,
        public readonly database: number,
        private readonly execute: (
            query: string,
            options: QueryOptions | undefined,
            readonly: boolean,
        ) => Promise<GraphQueryResult>,
    ) {}

    /** Execute a graph query, with literal parameters serialized by the Rust core. */
    query(query: string, options?: QueryOptions): Promise<GraphQueryResult> {
        return this.execute(query, options, false);
    }

    /** Execute GRAPH.RO_QUERY; the server rejects mutations. */
    roQuery(query: string, options?: QueryOptions): Promise<GraphQueryResult> {
        return this.execute(query, options, true);
    }
}
