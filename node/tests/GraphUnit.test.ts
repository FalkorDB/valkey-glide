/**
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 */

import { describe, expect, it } from "@jest/globals";
import {
    Graph,
    GraphDate,
    GraphDateTime,
    GraphDuration,
    GraphMap,
    GraphNode,
    GraphParameter,
    GraphPath,
    GraphPoint,
    GraphRelationship,
    GraphTime,
    GraphVector,
} from "../src/Graph";
import {
    encodeGraphRequest,
    materializeGraphResult,
    materializeGraphValue,
    validateGraphName,
} from "../src/GraphInternal";
import { graph_request } from "../build-ts/ProtobufMessage";

const record = (value: Record<string, unknown>) =>
    Object.entries(value).map(([key, value]) => ({ key, value }));
const envelope = (kind: string, value: unknown) => record({ kind, value });

function request(params?: Record<string, GraphParameter>, timeoutMs?: number) {
    return graph_request.QueryRequest.decode(
        encodeGraphRequest(
            "graph:{界}",
            4,
            "RETURN $p",
            { params, timeoutMs },
            true,
            12n,
        ),
    );
}

describe("typed graph request transport", () => {
    it("encodes exact signed i64 extrema, optional timeout and database generation", () => {
        const message = request(
            {
                min: -(1n << 63n),
                max: (1n << 63n) - 1n,
                safe: Number.MAX_SAFE_INTEGER,
                unicode: "'\\\n\0界😀",
                list: [null, true, 1.25, { nested: -0 }],
            },
            0,
        );
        const params = new Map(
            message.parameters!.entries.map(({ key, value }) => [key, value!]),
        );
        expect(params.get("min")!.integerValue!.toString()).toBe(
            "-9223372036854775808",
        );
        expect(params.get("max")!.integerValue!.toString()).toBe(
            "9223372036854775807",
        );
        expect(params.get("safe")!.integerValue!.toString()).toBe(
            "9007199254740991",
        );
        expect(params.get("unicode")!.stringValue).toBe("'\\\n\0界😀");
        expect(params.get("list")!.listValue!.values[0].nullValue).toBe(true);
        expect(
            Object.is(
                params.get("list")!.listValue!.values[3].mapValue!.entries[0]
                    .value!.doubleValue,
                -0,
            ),
        ).toBe(true);
        expect(message.graphName).toBe("graph:{界}");
        expect(message.database).toBe(4);
        expect(message.databaseEpoch.toString()).toBe("12");
        expect(message.readonly).toBe(true);
        expect(Object.hasOwn(message, "timeoutMs")).toBe(true);
        expect(message.timeoutMs!.toString()).toBe("0");
        expect(Object.hasOwn(request(), "timeoutMs")).toBe(false);
    });

    it.each([
        Number.MAX_SAFE_INTEGER + 1,
        -(Number.MAX_SAFE_INTEGER + 1),
        1n << 63n,
        -(1n << 63n) - 1n,
        NaN,
        Infinity,
        -Infinity,
        undefined,
        Symbol("x"),
        () => 1,
        new Date(),
        new Map(),
        new Set(),
        new Uint8Array([1]),
        Buffer.from("x"),
        new GraphNode(1n, [], new GraphMap([])),
        new GraphDate(1n),
        Object.create({ inherited: 1 }),
        new Proxy({}, {}),
        Array(3),
        Object.assign([1], { extra: 2 }),
    ])("rejects unsupported or lossy parameter %# before dispatch", (value) => {
        expect(() => request({ p: value as GraphParameter })).toThrow();
    });

    it("rejects cycles and accessors without evaluating them; accepts shared noncyclic objects", () => {
        const cyclic: Record<string, GraphParameter> = {};
        cyclic.self = cyclic;
        expect(() => request({ p: cyclic })).toThrow("Cyclic");
        let evaluated = false;
        const accessor = Object.defineProperty({}, "x", {
            get() {
                evaluated = true;
                return 1;
            },
        });
        expect(() => request({ p: accessor })).toThrow("accessors");
        expect(evaluated).toBe(false);
        const shared = { x: 1 };
        expect(() => request({ p: [shared, shared] })).not.toThrow();
        const nullPrototype = Object.create(null);
        Object.defineProperty(nullPrototype, "__proto__", { value: "literal" });
        expect(() => request({ p: nullPrototype })).not.toThrow();
    });

    it.each(["", "a`b", "x\0", "x\n", "x\u007f"])(
        "rejects invalid key %j at every depth",
        (key) => {
            expect(() => request({ [key]: 1 })).toThrow();
            expect(() => request({ p: { [key]: 1 } })).toThrow();
        },
    );

    it.each([-1, 0.1, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])(
        "rejects timeout %s",
        (timeout) => {
            expect(() => request({}, timeout)).toThrow();
        },
    );

    it("rejects NUL query/graph and invalid Unicode, but allows unicode graph keys", () => {
        for (const name of ["", "a\0", "a\n", "\ud800"])
            expect(() => validateGraphName(name)).toThrow();
        expect(() => validateGraphName("界😀:{key}")).not.toThrow();
        expect(() =>
            encodeGraphRequest("g", 0, "RETURN '\0'", {}, false, 0n),
        ).toThrow();
        expect(() => request({ p: "\udfff" })).toThrow();
    });

    it("bounds recursion", () => {
        let value: GraphParameter = 1;
        for (let i = 0; i < 130; i++) value = [value];
        expect(() => request({ p: value })).toThrow("128");
    });

    it("bounds parameter identifiers by UTF-8 byte length, not UTF-16 length", () => {
        expect(() => request({ ["a".repeat(512)]: 1 })).not.toThrow();
        expect(() => request({ ["a".repeat(513)]: 1 })).toThrow("512");
        expect(() => request({ ["😀".repeat(128)]: 1 })).not.toThrow();
        expect(() => request({ p: { ["😀".repeat(129)]: 1 } })).toThrow("512");
    });
});

describe("canonical typed graph results", () => {
    const node = envelope(
        "node",
        record({
            id: 9007199254740993n,
            labels: ["Person"],
            properties: [["name", envelope("string", "界")]],
        }),
    );
    const relationship = envelope(
        "relationship",
        record({
            id: 9223372036854775807n,
            type: "KNOWS",
            source_id: 9007199254740993n,
            destination_id: 2n,
            properties: [],
        }),
    );

    it("materializes all sixteen types without wire parsing or integer coercion", () => {
        const values = [
            envelope("null", null),
            envelope("string", "hello"),
            envelope("integer", -(1n << 63n)),
            envelope("boolean", false),
            envelope("double", 1.25),
            envelope("list", [envelope("integer", 1n)]),
            envelope("map", [
                ["same", envelope("integer", 1n)],
                ["same", envelope("integer", 2n)],
                ["__proto__", envelope("string", "literal")],
            ]),
            node,
            relationship,
            envelope(
                "path",
                record({ nodes: [node], relationships: [relationship] }),
            ),
            envelope("point", record({ latitude: 32.1, longitude: 34.8 })),
            envelope("vector", [1.25, -2.5]),
            ...["datetime", "date", "time", "duration"].map((kind) =>
                envelope(kind, 9223372036854775807n),
            ),
        ];
        const result = values.map(materializeGraphValue);
        expect(result.slice(0, 6)).toEqual([
            null,
            "hello",
            -(1n << 63n),
            false,
            1.25,
            [1n],
        ]);
        expect(result[6]).toBeInstanceOf(GraphMap);
        expect((result[6] as GraphMap).entries).toHaveLength(3);
        expect((result[6] as GraphMap).get("__proto__")).toBe("literal");
        expect(result[7]).toBeInstanceOf(GraphNode);
        expect((result[7] as GraphNode).id).toBe(9007199254740993n);
        expect((result[7] as GraphNode).properties.get("name")).toBe("界");
        expect(result[8]).toBeInstanceOf(GraphRelationship);
        expect(result[9]).toBeInstanceOf(GraphPath);
        expect(result[10]).toBeInstanceOf(GraphPoint);
        expect(result[11]).toBeInstanceOf(GraphVector);

        for (const [index, type] of [
            GraphDateTime,
            GraphDate,
            GraphTime,
            GraphDuration,
        ].entries()) {
            expect(result[12 + index]).toBeInstanceOf(type);
            expect((result[12 + index] as GraphDate).value).toBe(
                9223372036854775807n,
            );
        }
    });

    it("retains duplicate column ordering, empty results and optional/raw statistics", () => {
        const result = materializeGraphResult(
            record({
                columns: ["x", "x"],
                rows: [[envelope("integer", 1n), envelope("integer", 2n)]],
                statistics: record({
                    raw: ["Nodes created: 3", "New statistic: future"],
                    nodes_created: 3n,
                    cached_execution: false,
                    execution_time_ms: 0.125,
                    graph_version: 18446744073709551615n,
                }),
            }),
        );
        expect(result.columns).toEqual(["x", "x"]);
        expect(result.rows).toEqual([[1n, 2n]]);
        expect(result.statistics.nodesCreated).toBe(3n);
        expect(result.statistics.cachedExecution).toBe(false);
        expect(result.statistics.graphVersion).toBe(18446744073709551615n);
        expect(result.statistics.labelsAdded).toBeUndefined();
        expect(result.statistics.raw[1]).toBe("New statistic: future");
        expect(
            materializeGraphResult(
                record({
                    columns: [],
                    rows: [],
                    statistics: record({ raw: [] }),
                }),
            ).rows,
        ).toEqual([]);
    });

    it("preserves prototype-like and numeric-looking keys as ordered GraphMap entries", () => {
        const objectPrototype = Object.getOwnPropertyDescriptors(
            Object.prototype,
        );
        const mapPrototype = Object.getOwnPropertyDescriptors(
            GraphMap.prototype,
        );
        const entries = [
            ["10", envelope("integer", 10n)],
            [
                "__proto__",
                envelope("map", [["polluted", envelope("boolean", true)]]),
            ],
            ["constructor", envelope("string", "literal constructor")],
            ["2", envelope("integer", 2n)],
            ["01", envelope("string", "leading zero")],
            ["__proto__", envelope("string", "duplicate key")],
        ];
        const result = materializeGraphResult(
            record({
                columns: ["map", "node"],
                rows: [
                    [
                        envelope("map", entries),
                        envelope(
                            "node",
                            record({
                                id: 1n,
                                labels: [],
                                properties: entries,
                            }),
                        ),
                    ],
                ],
                statistics: record({ raw: [] }),
            }),
        );
        const value = result.rows[0][0] as GraphMap;
        const properties = (result.rows[0][1] as GraphNode).properties;

        for (const map of [value, properties]) {
            expect(map).toBeInstanceOf(GraphMap);
            expect(Object.getPrototypeOf(map)).toBe(GraphMap.prototype);
            expect(map.constructor).toBe(GraphMap);
            expect(Object.hasOwn(map, "__proto__")).toBe(false);
            expect(Object.hasOwn(map, "constructor")).toBe(false);
            expect(map.entries.map(([key]) => key)).toEqual([
                "10",
                "__proto__",
                "constructor",
                "2",
                "01",
                "__proto__",
            ]);
            expect((map.get("__proto__") as GraphMap).get("polluted")).toBe(
                true,
            );
            expect(map.get("constructor")).toBe("literal constructor");
            expect(map.get("10")).toBe(10n);
            expect(map.get("2")).toBe(2n);
            expect(map.get("01")).toBe("leading zero");
        }

        expect(Object.getOwnPropertyDescriptors(Object.prototype)).toEqual(
            objectPrototype,
        );
        expect(Object.getOwnPropertyDescriptors(GraphMap.prototype)).toEqual(
            mapPrototype,
        );
    });

    it.each(
        [
            envelope("future", null),
            envelope("integer", 9007199254740992),
            envelope("null", 0),
            envelope(
                "path",
                record({ nodes: [envelope("string", "x")], relationships: [] }),
            ),
            envelope("map", [["key"]]),
            [],
        ].map((value) => ({ value })),
    )("rejects malformed or unknown canonical values %#", ({ value }) => {
        expect(() => materializeGraphValue(value)).toThrow();
    });

    it("does not collapse row-width errors", () => {
        expect(() =>
            materializeGraphResult(
                record({
                    columns: ["x"],
                    rows: [[]],
                    statistics: record({ raw: [] }),
                }),
            ),
        ).toThrow("width");
    });

    it("Graph methods retain the selected context and readonly flag", async () => {
        const calls: unknown[] = [];
        const graph = new Graph("name", 4, async (...args) => {
            calls.push(args);
            return { columns: [], rows: [], statistics: { raw: [] } };
        });
        await graph.query("RETURN 1");
        await graph.roQuery("RETURN 2", { timeoutMs: 0 });
        expect(graph.name).toBe("name");
        expect(graph.database).toBe(4);
        expect(calls).toEqual([
            ["RETURN 1", undefined, false],
            ["RETURN 2", { timeoutMs: 0 }, true],
        ]);
    });
});
