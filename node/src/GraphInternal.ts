/**
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 *
 * Graph-only transport and canonical-result materialization, not wire parsing.
 */

import Long from "long";
import { types } from "node:util";
import { graph_request } from "../build-ts/ProtobufMessage";
import {
    GraphDate,
    GraphDateTime,
    GraphDuration,
    GraphMap,
    GraphNode,
    GraphPath,
    GraphPoint,
    GraphQueryResult,
    GraphRelationship,
    GraphStatistics,
    GraphTime,
    GraphValue,
    GraphVector,
    QueryOptions,
} from "./Graph";

const CONTROL = /\p{Cc}/u;

function text(value: unknown): string {
    if (typeof value !== "string" || !value.isWellFormed()) {
        throw new TypeError(
            "Graph strings must be well-formed Unicode strings",
        );
    }

    return value;
}

export function validateGraphName(name: string): void {
    if (!text(name).length || CONTROL.test(name)) {
        throw new TypeError(
            "Graph name must be nonempty and contain no control characters",
        );
    }
}

function key(name: string): void {
    if (
        !text(name).length ||
        CONTROL.test(name) ||
        name.includes("`") ||
        Buffer.byteLength(name, "utf8") > 512
    ) {
        throw new TypeError(
            "Graph parameter keys must be nonempty, at most 512 UTF-8 bytes, and contain no backticks or control characters",
        );
    }
}

function entries(value: unknown): [string, unknown][] {
    if (
        typeof value !== "object" ||
        value === null ||
        types.isProxy(value) ||
        ![Object.prototype, null].includes(Object.getPrototypeOf(value))
    ) {
        throw new TypeError(
            "Graph parameters must contain only plain objects and arrays",
        );
    }

    return Reflect.ownKeys(value).map((name) => {
        if (typeof name !== "string") {
            throw new TypeError("Symbol graph parameter keys are unsupported");
        }

        key(name);
        const descriptor = Object.getOwnPropertyDescriptor(value, name)!;

        if (!("value" in descriptor)) {
            throw new TypeError("Graph parameter accessors are unsupported");
        }

        return [name, descriptor.value as unknown];
    });
}

function encodeParameter(
    value: unknown,
    ancestors: Set<object>,
    depth: number,
): graph_request.IParameter {
    if (depth > 128) {
        throw new TypeError("Graph parameter nesting exceeds 128 levels");
    }

    if (value === null) return { nullValue: true };
    if (typeof value === "boolean") return { booleanValue: value };
    if (typeof value === "string") return { stringValue: text(value) };

    if (typeof value === "bigint") {
        if (value < -(1n << 63n) || value > (1n << 63n) - 1n) {
            throw new RangeError(
                "Graph integer parameters must fit signed 64 bits",
            );
        }

        return { integerValue: Long.fromString(value.toString(), false) };
    }

    if (typeof value === "number") {
        if (!Number.isFinite(value)) {
            throw new TypeError("Graph numeric parameters must be finite");
        }

        if (Number.isInteger(value) && !Object.is(value, -0)) {
            if (!Number.isSafeInteger(value)) {
                throw new RangeError("Unsafe graph integer number; use bigint");
            }

            return { integerValue: Long.fromNumber(value, false) };
        }

        return { doubleValue: value };
    }

    if (typeof value !== "object") {
        throw new TypeError("Unsupported graph parameter type");
    }

    if (types.isProxy(value)) {
        throw new TypeError("Proxy graph parameters are unsupported");
    }

    if (ancestors.has(value)) {
        throw new TypeError("Cyclic graph parameters are unsupported");
    }

    ancestors.add(value);

    try {
        if (Array.isArray(value)) {
            if (Object.getPrototypeOf(value) !== Array.prototype) {
                throw new TypeError("Graph array subclasses are unsupported");
            }

            if (Reflect.ownKeys(value).length !== value.length + 1) {
                throw new TypeError(
                    "Graph arrays must be dense and have no custom properties",
                );
            }

            const values = Array.from({ length: value.length }, (_, index) => {
                const descriptor = Object.getOwnPropertyDescriptor(
                    value,
                    String(index),
                );

                if (!descriptor || !("value" in descriptor)) {
                    throw new TypeError(
                        "Sparse graph arrays and accessors are unsupported",
                    );
                }

                return encodeParameter(descriptor.value, ancestors, depth + 1);
            });
            return { listValue: { values } };
        }

        return { mapValue: encodeMap(value, ancestors, depth + 1) };
    } finally {
        ancestors.delete(value);
    }
}

function encodeMap(
    value: unknown,
    ancestors: Set<object>,
    depth: number,
): graph_request.IParameterMap {
    return {
        entries: entries(value).map(([name, value]) => ({
            key: name,
            value: encodeParameter(value, ancestors, depth),
        })),
    };
}

export function encodeGraphRequest(
    name: string,
    database: number,
    query: string,
    options: QueryOptions | undefined,
    readonly: boolean,
    databaseEpoch: bigint,
): Uint8Array {
    validateGraphName(name);

    if (text(query).includes("\0")) {
        throw new TypeError("Graph query text must not contain NUL");
    }

    const timeout = options?.timeoutMs;

    if (
        timeout !== undefined &&
        (!Number.isSafeInteger(timeout) || timeout < 0)
    ) {
        throw new RangeError(
            "Graph timeoutMs must be a nonnegative safe integer",
        );
    }

    return graph_request.QueryRequest.encode({
        graphName: name,
        database,
        query,
        readonly,
        databaseEpoch: Long.fromString(databaseEpoch.toString(), true),
        parameters:
            options?.params === undefined
                ? undefined
                : encodeMap(options.params, new Set([options.params]), 0),
        timeoutMs:
            timeout === undefined ? undefined : Long.fromNumber(timeout, true),
    }).finish();
}

function array(value: unknown): unknown[] {
    if (!Array.isArray(value))
        throw new TypeError("Invalid canonical graph array");
    return value;
}

function record(value: unknown): Map<string, unknown> {
    const result = new Map<string, unknown>();

    for (const entry of array(value)) {
        if (
            typeof entry !== "object" ||
            entry === null ||
            !("key" in entry) ||
            !("value" in entry)
        ) {
            throw new TypeError("Invalid canonical graph record");
        }

        const name = text(entry.key);
        if (result.has(name))
            throw new TypeError("Duplicate canonical graph field");
        result.set(name, entry.value);
    }

    return result;
}

function integer(value: unknown): bigint {
    if (typeof value !== "bigint")
        throw new TypeError("Invalid canonical graph integer");
    return value;
}

function number(value: unknown): number {
    if (typeof value !== "number")
        throw new TypeError("Invalid canonical graph double");
    return value;
}

function boolean(value: unknown): boolean {
    if (typeof value !== "boolean")
        throw new TypeError("Invalid canonical graph boolean");
    return value;
}

function map(value: unknown): GraphMap {
    return new GraphMap(
        array(value).map((entry) => {
            const pair = array(entry);
            if (pair.length !== 2)
                throw new TypeError("Invalid canonical graph map entry");
            return [text(pair[0]), materializeGraphValue(pair[1])] as const;
        }),
    );
}

/** Materialize only the canonical core envelope, never the FalkorDB RESP schema. */
export function materializeGraphValue(value: unknown): GraphValue {
    const envelope = record(value);
    const kind = text(envelope.get("kind"));
    const payload = envelope.get("value");

    switch (kind) {
        case "null":
            if (payload !== null)
                throw new TypeError("Invalid canonical graph null");
            return null;
        case "string":
            return text(payload);
        case "integer":
            return integer(payload);
        case "boolean":
            return boolean(payload);
        case "double":
            return number(payload);
        case "list":
            return array(payload).map(materializeGraphValue);
        case "map":
            return map(payload);

        case "node": {
            const fields = record(payload);
            return new GraphNode(
                integer(fields.get("id")),
                array(fields.get("labels")).map(text),
                map(fields.get("properties")),
            );
        }

        case "relationship": {
            const fields = record(payload);
            return new GraphRelationship(
                integer(fields.get("id")),
                text(fields.get("type")),
                integer(fields.get("source_id")),
                integer(fields.get("destination_id")),
                map(fields.get("properties")),
            );
        }

        case "path": {
            const fields = record(payload);
            const nodes = array(fields.get("nodes")).map(materializeGraphValue);
            const relationships = array(fields.get("relationships")).map(
                materializeGraphValue,
            );

            if (
                !nodes.every(
                    (node): node is GraphNode => node instanceof GraphNode,
                ) ||
                !relationships.every(
                    (relationship): relationship is GraphRelationship =>
                        relationship instanceof GraphRelationship,
                )
            ) {
                throw new TypeError("Invalid canonical graph path");
            }

            return new GraphPath(nodes, relationships);
        }

        case "point": {
            const fields = record(payload);
            return new GraphPoint(
                number(fields.get("latitude")),
                number(fields.get("longitude")),
            );
        }

        case "vector":
            return new GraphVector(array(payload).map(number));
        case "datetime":
            return new GraphDateTime(integer(payload));
        case "date":
            return new GraphDate(integer(payload));
        case "time":
            return new GraphTime(integer(payload));
        case "duration":
            return new GraphDuration(integer(payload));
        default:
            throw new TypeError(`Unknown canonical graph value kind: ${kind}`);
    }
}

export function materializeGraphResult(value: unknown): GraphQueryResult {
    const result = record(value);
    const columns = array(result.get("columns")).map(text);
    const rows = array(result.get("rows")).map((row) => {
        const values = array(row).map(materializeGraphValue);
        if (values.length !== columns.length)
            throw new TypeError("Invalid canonical graph row width");
        return values;
    });
    const rawStatistics = record(result.get("statistics"));
    const statistics: GraphStatistics = {
        raw: array(rawStatistics.get("raw")).map(text),
    };
    const counters = {
        labels_added: "labelsAdded",
        labels_removed: "labelsRemoved",
        nodes_created: "nodesCreated",
        nodes_deleted: "nodesDeleted",
        properties_set: "propertiesSet",
        properties_removed: "propertiesRemoved",
        relationships_created: "relationshipsCreated",
        relationships_deleted: "relationshipsDeleted",
        indices_created: "indicesCreated",
        indices_deleted: "indicesDeleted",
        graph_version: "graphVersion",
    } as const;

    for (const [source, target] of Object.entries(counters)) {
        if (rawStatistics.has(source))
            statistics[target] = integer(rawStatistics.get(source));
    }

    if (rawStatistics.has("cached_execution"))
        statistics.cachedExecution = boolean(
            rawStatistics.get("cached_execution"),
        );
    if (rawStatistics.has("execution_time_ms"))
        statistics.executionTimeMs = number(
            rawStatistics.get("execution_time_ms"),
        );
    return { columns, rows, statistics };
}
