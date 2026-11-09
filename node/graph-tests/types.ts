/**
 * Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
 *
 * Compile-only public API checks; never executed as a server test.
 */

import {
    Graph,
    GraphDate,
    GraphMap,
    GraphNode,
    GraphParameter,
    GraphQueryOptions,
    GraphQueryResult,
    GraphRelationship,
    GraphStatistics,
    GraphValue,
    QueryOptions,
} from "../src/Graph";
import { IsolatedScope } from "../src/IsolatedScope";

export async function assertGraphTypes(graph: Graph): Promise<void> {
    const options: QueryOptions = {
        params: {
            id: 9223372036854775807n,
            nested: [null, true, { text: "界", finite: 1.25 }],
        },
        timeoutMs: 0,
    };
    const alias: GraphQueryOptions = options;
    const result: GraphQueryResult = await graph.query("RETURN $id", alias);
    const readOnly: Promise<GraphQueryResult> = graph.roQuery("RETURN 1");
    const values: readonly (readonly GraphValue[])[] = result.rows;
    const stats: GraphStatistics = result.statistics;
    const count: bigint | undefined = stats.nodesCreated;
    const name: string = graph.name;
    const database: number = graph.database;
    void [readOnly, values, count, name, database];

    // @ts-expect-error Graph integers must not silently become numbers.
    const lossyCount: number = stats.nodesCreated;
    // @ts-expect-error Ordered rows cannot be accessed as column-keyed records.
    const keyedRow: Record<string, GraphValue> = result.rows[0];
    // @ts-expect-error Query options intentionally have no routing override.
    await graph.query("RETURN 1", { route: "allNodes" });
    // @ts-expect-error Native Date objects are not literal graph parameters.
    const dateParameter: GraphParameter = new Date();
    void [lossyCount, keyedRow, dateParameter];
}

export function assertEntityTypes(
    node: GraphNode,
    edge: GraphRelationship,
    date: GraphDate,
    map: GraphMap,
): void {
    const id: bigint = node.id;
    const source: bigint = edge.sourceId;
    const destination: bigint = edge.destinationId;
    const rawTemporal: bigint = date.value;
    const property: GraphValue | undefined = map.get("key");
    // @ts-expect-error Graph entity IDs must remain exact bigint.
    const numberId: number = node.id;
    // @ts-expect-error Temporal wrappers are not JavaScript Dates.
    const jsDate: Date = date;
    // @ts-expect-error Graph entities cannot be reused as literal parameters.
    const entityParameter: GraphParameter = node;
    void [
        id,
        source,
        destination,
        rawTemporal,
        property,
        numberId,
        jsDate,
        entityParameter,
    ];
}

export function assertScopeExclusion(scope: IsolatedScope): void {
    // @ts-expect-error Isolated scopes cannot expose graphs that outlive release.
    scope.selectGraph("not-available");
}
