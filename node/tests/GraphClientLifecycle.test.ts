/**
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 */

import { afterEach, describe, expect, it, jest } from "@jest/globals";
import type {
    CommandResponse,
    GlideClientHandle,
    GraphHandle,
} from "../build-ts/native";
import { graph_request } from "../build-ts/ProtobufMessage";
import { BaseClient, ClosingError, RequestError } from "../build-ts";

const native =
    jest.requireActual<typeof import("../build-ts/native")>(
        "../build-ts/native",
    );

const record = (value: Record<string, unknown>) =>
    Object.entries(value).map(([key, value]) => ({ key, value }));
const canonical = record({
    columns: ["n"],
    rows: [[record({ kind: "integer", value: 9007199254740993n })]],
    statistics: record({ raw: [] }),
});

function handle() {
    const graph = {
        database: 4,
        databaseEpoch: 12n,
    } as GraphHandle;
    return {
        currentDatabase: 4,
        graphDatabaseEpoch: 12n,
        selectGraph: jest
            .fn<(name: string) => GraphHandle>()
            .mockReturnValue(graph),
        sendGraphQuery: jest
            .fn<(...args: [number, Uint8Array, GraphHandle]) => boolean>()
            .mockReturnValue(true),
        close: jest.fn<() => void>(),
    };
}

class TestClient extends BaseClient {
    constructor(nativeHandle: ReturnType<typeof handle>) {
        super();
        this.clientHandle = nativeHandle as unknown as GlideClientHandle;
    }

    respond(response: Partial<CommandResponse>): void {
        (
            this as unknown as {
                handleResponse: (response: CommandResponse) => void;
            }
        ).handleResponse({ callbackIdx: 0, isPush: false, ...response });
    }

    ordinaryResponse(): Promise<unknown> {
        const index = this.getCallbackIndex();
        return new Promise((resolve, reject) => {
            this.promiseCallbackFunctions[index] = [resolve, reject];
            this.respond({
                callbackIdx: index,
                respPointerHigh: 1,
                respPointerLow: 1,
            });
        });
    }

    detach(): void {
        this.clientHandle = null;
    }
}

afterEach(() => {
    jest.restoreAllMocks();
});

describe("graph callback lifecycle", () => {
    it("selects entirely locally and serializes the retained database snapshot", async () => {
        const fake = handle();
        const client = new TestClient(fake);
        const graph = client.selectGraph("界:{graph}");
        expect(fake.sendGraphQuery).not.toHaveBeenCalled();
        expect(fake.selectGraph).toHaveBeenCalledWith("界:{graph}");
        expect(graph.database).toBe(4);
        const response = jest
            .spyOn(native, "graphValueFromSplitPointer")
            .mockReturnValue(canonical);
        const ordinary = jest.spyOn(native, "valueFromSplitPointer");
        const promise = graph.query("RETURN $id", {
            params: { id: 9223372036854775807n },
        });
        const [index, bytes, retained] = fake.sendGraphQuery.mock.calls[0];
        expect(retained).toBe(fake.selectGraph.mock.results[0].value);
        const request = graph_request.QueryRequest.decode(bytes);
        expect(request.database).toBe(4);
        expect(request.databaseEpoch.toString()).toBe("12");
        expect(request.graphName).toBe("界:{graph}");
        expect(
            request.parameters!.entries[0].value!.integerValue!.toString(),
        ).toBe("9223372036854775807");
        client.respond({
            callbackIdx: index,
            respPointerHigh: 1,
            respPointerLow: 1,
        });
        expect((await promise).rows).toEqual([[9007199254740993n]]);
        expect(response).toHaveBeenCalledTimes(1);
        expect(ordinary).not.toHaveBeenCalled();
        client.close();
    });

    it("restores callback slots after inflight rejection and synchronous native errors", async () => {
        const fake = handle();
        const client = new TestClient(fake);
        const graph = client.selectGraph("g");
        fake.sendGraphQuery.mockReturnValueOnce(false);
        await expect(graph.query("RETURN 1")).rejects.toBeInstanceOf(
            RequestError,
        );
        fake.sendGraphQuery.mockImplementationOnce(() => {
            throw new Error("native rejected");
        });
        await expect(graph.query("RETURN 1")).rejects.toThrow(
            "native rejected",
        );
        jest.spyOn(native, "graphValueFromSplitPointer").mockReturnValue(
            canonical,
        );
        const promise = graph.query("RETURN 1");
        expect(fake.sendGraphQuery.mock.calls.map(([index]) => index)).toEqual([
            0, 0, 0,
        ]);
        expect(fake.selectGraph).toHaveBeenCalledTimes(1);
        expect(
            fake.sendGraphQuery.mock.calls.every(
                ([, , retained]) =>
                    retained === fake.selectGraph.mock.results[0].value,
            ),
        ).toBe(true);
        client.respond({ respPointerHigh: 1, respPointerLow: 1 });
        await expect(promise).resolves.toMatchObject({
            rows: [[9007199254740993n]],
        });
        client.close();
    });

    it("cleans graph markers after errors so ordinary callbacks keep ordinary conversion", async () => {
        const fake = handle();
        const client = new TestClient(fake);
        const graph = client.selectGraph("g");
        const promise = graph.query("invalid");
        client.respond({
            requestError: { errorType: 0, message: "server rejected" },
        });
        await expect(promise).rejects.toThrow("server rejected");
        const graphConverter = jest.spyOn(native, "graphValueFromSplitPointer");
        const ordinaryConverter = jest
            .spyOn(native, "valueFromSplitPointer")
            .mockReturnValue(42);
        expect(await client.ordinaryResponse()).toBe(42);
        expect(ordinaryConverter).toHaveBeenCalledTimes(1);
        expect(graphConverter).not.toHaveBeenCalled();
        client.close();
    });

    it("rejects malformed canonical results without poisoning callback reuse", async () => {
        const fake = handle();
        const client = new TestClient(fake);
        const graph = client.selectGraph("g");
        jest.spyOn(native, "graphValueFromSplitPointer")
            .mockReturnValueOnce([])
            .mockReturnValueOnce(canonical);
        const malformed = graph.query("RETURN 1");
        client.respond({ respPointerHigh: 1, respPointerLow: 1 });
        await expect(malformed).rejects.toThrow();
        const valid = graph.query("RETURN 1");
        client.respond({ respPointerHigh: 1, respPointerLow: 1 });
        expect((await valid).rows).toEqual([[9007199254740993n]]);
        expect(fake.sendGraphQuery.mock.calls.map(([index]) => index)).toEqual([
            0, 0,
        ]);
        client.close();
    });

    it("invalidates handles on close and detached leases without sending another request", async () => {
        const fake = handle();
        const client = new TestClient(fake);
        const graph = client.selectGraph("g");
        const inflight = graph.query("RETURN 1");
        client.close();
        await expect(inflight).rejects.toBeInstanceOf(ClosingError);
        await expect(graph.roQuery("RETURN 1")).rejects.toBeInstanceOf(
            ClosingError,
        );
        expect(fake.close).toHaveBeenCalledTimes(1);
        expect(fake.sendGraphQuery).toHaveBeenCalledTimes(1);
        const detached = new TestClient(handle());
        const detachedGraph = detached.selectGraph("g");
        detached.detach();
        await expect(detachedGraph.query("RETURN 1")).rejects.toBeInstanceOf(
            ClosingError,
        );
    });

    it("rejects odd or changing database epochs before constructing a handle", () => {
        const odd = handle();
        odd.graphDatabaseEpoch = 13n;
        const client = new TestClient(odd);
        expect(() => client.selectGraph("g")).toThrow("unsettled");
        client.close();
        const changing = handle();
        let epoch = 0n;
        Object.defineProperty(changing, "graphDatabaseEpoch", {
            get: () => (epoch += 2n),
        });
        const racy = new TestClient(changing);
        expect(() => racy.selectGraph("g")).toThrow("unsettled");
        racy.close();
    });

    it("rejects a native context selected during a database-generation change", () => {
        const fake = handle();
        fake.selectGraph.mockReturnValue({
            database: 4,
            databaseEpoch: 14n,
        } as GraphHandle);
        const client = new TestClient(fake);
        expect(() => client.selectGraph("g")).toThrow("unsettled");
        expect(fake.sendGraphQuery).not.toHaveBeenCalled();
        client.close();
    });

    it("keeps a closed graph rejected when its native completion arrives late", async () => {
        const fake = handle();
        const client = new TestClient(fake);
        const graph = client.selectGraph("g");
        const inflight = graph.query("RETURN 1");
        client.close();
        await expect(inflight).rejects.toBeInstanceOf(ClosingError);
        const reclaim = jest
            .spyOn(native, "valueFromSplitPointer")
            .mockReturnValue(canonical);
        expect(() =>
            client.respond({ respPointerHigh: 1, respPointerLow: 1 }),
        ).not.toThrow();
        expect(reclaim).toHaveBeenCalledTimes(1);
        await expect(inflight).rejects.toBeInstanceOf(ClosingError);
        await expect(graph.query("RETURN 2")).rejects.toBeInstanceOf(
            ClosingError,
        );
        expect(fake.sendGraphQuery).toHaveBeenCalledTimes(1);
    });
});
