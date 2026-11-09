/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import glide.api.BaseClient;
import glide.api.GlideClient;
import glide.api.GlideClusterClient;
import glide.api.Graph;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.configuration.ProtocolVersion;
import glide.api.models.configuration.ReadFrom;
import glide.api.models.exceptions.ClosingException;
import glide.api.models.exceptions.RequestException;
import glide.api.models.graph.GraphDate;
import glide.api.models.graph.GraphDateTime;
import glide.api.models.graph.GraphDuration;
import glide.api.models.graph.GraphMap;
import glide.api.models.graph.GraphNode;
import glide.api.models.graph.GraphPath;
import glide.api.models.graph.GraphPoint;
import glide.api.models.graph.GraphQueryOptions;
import glide.api.models.graph.GraphQueryResult;
import glide.api.models.graph.GraphRelationship;
import glide.api.models.graph.GraphTime;
import glide.api.models.graph.GraphVectorF32;
import glide.api.models.pool.ClientPool;
import glide.api.models.pool.ClientPoolConfig;
import glide.api.models.pool.PooledGlideClient;
import glide.internal.AsyncRegistry;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Real-server conformance; only :client:graphTest opts into this suite. */
@Tag("falkordb")
class FalkorDBGraphConformanceTest {
    private static Stream<Arguments> clients() {
        return Stream.of(
                Arguments.of(false, ProtocolVersion.RESP2),
                Arguments.of(false, ProtocolVersion.RESP3),
                Arguments.of(true, ProtocolVersion.RESP2),
                Arguments.of(true, ProtocolVersion.RESP3));
    }

    @ParameterizedTest
    @MethodSource("clients")
    void canonical_values_params_statistics_and_lifetime(boolean cluster, ProtocolVersion protocol)
            throws Exception {
        BaseClient client = connect(cluster, protocol);
        String key = "java:{" + UUID.randomUUID() + "}:graph:🌐";
        Graph graph = client.selectGraph(key);
        try {
            assertSame(client, graph.getClient());
            assertEquals(0L, client.exists(new String[] {key}).get(10, TimeUnit.SECONDS));
            GraphQueryResult all =
                    graph
                            .query(
                                    "CREATE p=(a:Person:Researcher {name:'Ada',age:42})"
                                            + "-[r:KNOWS {since:2020}]->(b:Person {name:'Bob'}) RETURN "
                                            + "null AS null_value, 'textλ🌐' AS string_value, "
                                            + "-9223372036854775808 AS integer_value, true AS boolean_value, "
                                            + "1.5 AS double_value, [1,null,{nested:[false,'x']}] AS list_value, "
                                            + "r AS relationship_value, a AS node_value, p AS path_value, "
                                            + "{first:1,second:'two'} AS map_value, "
                                            + "point({latitude:1.5,longitude:2.5}) AS point_value, "
                                            + "vecf32([1,2.5,-3]) AS vector_value, "
                                            + "localdatetime('2020-01-02T03:04:05') AS datetime_value, "
                                            + "date('2020-01-02') AS date_value, "
                                            + "localtime('03:04:05') AS time_value, "
                                            + "duration('P1Y2M3DT4H5M6S') AS duration_value")
                            .get(10, TimeUnit.SECONDS);
            assertEquals(16, all.getColumns().size());
            assertEquals("textλ🌐", all.getRows().get(0).get(1).getValue());
            assertEquals(Long.MIN_VALUE, all.getRows().get(0).get(2).getValue());
            assertInstanceOf(GraphRelationship.class, all.getRows().get(0).get(6).getValue());
            GraphNode node = assertInstanceOf(GraphNode.class, all.getRows().get(0).get(7).getValue());
            assertTrue(node.getLabels().contains("Researcher"));
            assertInstanceOf(GraphPath.class, all.getRows().get(0).get(8).getValue());
            assertInstanceOf(GraphMap.class, all.getRows().get(0).get(9).getValue());
            assertInstanceOf(GraphPoint.class, all.getRows().get(0).get(10).getValue());
            assertInstanceOf(GraphVectorF32.class, all.getRows().get(0).get(11).getValue());
            assertInstanceOf(GraphDateTime.class, all.getRows().get(0).get(12).getValue());
            assertInstanceOf(GraphDate.class, all.getRows().get(0).get(13).getValue());
            assertInstanceOf(GraphTime.class, all.getRows().get(0).get(14).getValue());
            assertInstanceOf(GraphDuration.class, all.getRows().get(0).get(15).getValue());
            assertEquals(BigInteger.valueOf(2), all.getStatistics().getNodesCreated());
            assertFalse(all.getStatistics().getRaw().isEmpty());

            Map<String, Object> params = new LinkedHashMap<>();
            params.put("min", Long.MIN_VALUE);
            params.put("max", Long.MAX_VALUE);
            params.put("text", "λ🌐'\\\n\t");
            params.put(
                    "nested", Arrays.asList(null, true, Collections.singletonMap("space key", "value")));
            GraphQueryResult roundtrip =
                    graph
                            .roQuery(
                                    "RETURN $min, $max, $text, $nested, 'λ🌐'",
                                    GraphQueryOptions.builder().params(params).timeoutMs(0L).build())
                            .get(10, TimeUnit.SECONDS);
            assertEquals(Long.MIN_VALUE, roundtrip.getRows().get(0).get(0).getValue());
            assertEquals(Long.MAX_VALUE, roundtrip.getRows().get(0).get(1).getValue());
            assertEquals(params.get("text"), roundtrip.getRows().get(0).get(2).getValue());
            assertEquals("λ🌐", roundtrip.getRows().get(0).get(4).getValue());
            ExecutionException readonly =
                    assertThrows(
                            ExecutionException.class, () -> graph.roQuery("CREATE ()").get(10, TimeUnit.SECONDS));
            assertInstanceOf(RequestException.class, readonly.getCause());
            CompletableFuture<String> ordinaryError = client.get(key);
            assertEquals(
                    0,
                    ordinaryError
                            .handle(
                                    (value, error) -> {
                                        assertNotNull(error, "GET must reject a graph key with WRONGTYPE");
                                        return AsyncRegistry.getPendingCount();
                                    })
                            .get(10, TimeUnit.SECONDS));
            assertInstanceOf(
                    RequestException.class,
                    assertThrows(ExecutionException.class, () -> ordinaryError.get(10, TimeUnit.SECONDS))
                            .getCause());

            client.del(new String[] {key}).get(10, TimeUnit.SECONDS);
            GraphQueryResult writeOnly =
                    graph.query("CREATE (:Replacement {fresh:true})").get(10, TimeUnit.SECONDS);
            assertTrue(writeOnly.getColumns().isEmpty());
            assertTrue(writeOnly.getRows().isEmpty());
            GraphQueryResult replacement = graph.roQuery("MATCH(n) RETURN n").get(10, TimeUnit.SECONDS);
            assertEquals(
                    Arrays.asList("Replacement"),
                    assertInstanceOf(GraphNode.class, replacement.getRows().get(0).get(0).getValue())
                            .getLabels());
            Graph lateGraph = client.selectGraph(key);
            CompletableFuture<GraphQueryResult> late =
                    lateGraph.roQuery("UNWIND range(1,100000) AS i RETURN sum(i)");
            lateGraph.close();
            lateGraph.close();
            assertEquals(
                    5000050000.0,
                    assertInstanceOf(
                            Double.class, late.get(10, TimeUnit.SECONDS).getRows().get(0).get(0).getValue()));
            ExecutionException released =
                    assertThrows(
                            ExecutionException.class,
                            () -> lateGraph.query("RETURN 1").get(10, TimeUnit.SECONDS));
            assertInstanceOf(ClosingException.class, released.getCause());
            ExecutionException releasedReadonly =
                    assertThrows(
                            ExecutionException.class,
                            () -> lateGraph.roQuery("RETURN 1").get(10, TimeUnit.SECONDS));
            assertInstanceOf(ClosingException.class, releasedReadonly.getCause());
            assertEquals(
                    1L,
                    graph.roQuery("RETURN 1").get(10, TimeUnit.SECONDS).getRows().get(0).get(0).getValue());
            assertEquals(1L, client.exists(new String[] {key}).get(10, TimeUnit.SECONDS));
            if (!cluster) {
                GlideClient standalone = assertInstanceOf(GlideClient.class, client);
                standalone.select(1).get(10, TimeUnit.SECONDS);
                standalone.select(0).get(10, TimeUnit.SECONDS);
                ExecutionException stale =
                        assertThrows(
                                ExecutionException.class, () -> graph.query("RETURN 1").get(10, TimeUnit.SECONDS));
                assertInstanceOf(RequestException.class, stale.getCause());
                assertEquals(
                        1L,
                        standalone
                                .selectGraph(key)
                                .roQuery("RETURN 1")
                                .get(10, TimeUnit.SECONDS)
                                .getRows()
                                .get(0)
                                .get(0)
                                .getValue());
            }
        } finally {
            try {
                client
                        .del(new String[] {key})
                        .thenApply(
                                deleted -> {
                                    assertEquals(
                                            0,
                                            AsyncRegistry.getPendingCount(),
                                            "Graph teardown must release ordinary command registration before"
                                                    + " completion");
                                    return deleted;
                                })
                        .get(10, TimeUnit.SECONDS);
            } finally {
                client.close();
            }
        }
        ExecutionException closed =
                assertThrows(
                        ExecutionException.class, () -> graph.query("RETURN 1").get(10, TimeUnit.SECONDS));
        assertInstanceOf(ClosingException.class, closed.getCause());
        ExecutionException closedReadonly =
                assertThrows(
                        ExecutionException.class, () -> graph.roQuery("RETURN 1").get(10, TimeUnit.SECONDS));
        assertInstanceOf(ClosingException.class, closedReadonly.getCause());
        assertThrows(ClosingException.class, () -> client.selectGraph(key));
        assertEquals(0, AsyncRegistry.getPendingCount());
    }

    @ParameterizedTest
    @MethodSource("clients")
    void entity_queries_complete_with_one_inflight_slot(boolean cluster, ProtocolVersion protocol)
            throws Exception {
        BaseClient client = connect(cluster, protocol, 1);
        String key = "java:{" + UUID.randomUUID() + "}:single-inflight";
        Graph graph = client.selectGraph(key);
        try {
            GraphQueryResult created =
                    graph
                            .query("CREATE p=(:JavaSingle {name:'Ada'})-[:KNOWS]->(:Other) RETURN p")
                            .get(10, TimeUnit.SECONDS);
            assertInstanceOf(GraphPath.class, created.getRows().get(0).get(0).getValue());
            GraphQueryResult read =
                    graph.roQuery("MATCH p=(:JavaSingle)-[:KNOWS]->() RETURN p").get(10, TimeUnit.SECONDS);
            GraphPath path = assertInstanceOf(GraphPath.class, read.getRows().get(0).get(0).getValue());
            assertEquals(Collections.singletonList("JavaSingle"), path.getNodes().get(0).getLabels());
            assertEquals("name", path.getNodes().get(0).getProperties().getEntries().get(0).getKey());
            assertEquals("KNOWS", path.getRelationships().get(0).getType());
        } finally {
            try {
                assertEquals(
                        0L,
                        client
                                .del(new String[] {key})
                                .thenCompose(deleted -> client.exists(new String[] {key}))
                                .get(10, TimeUnit.SECONDS));
            } finally {
                client.close();
            }
        }
        assertEquals(0, AsyncRegistry.getPendingCount());
    }

    @ParameterizedTest
    @EnumSource(ProtocolVersion.class)
    void pooled_clients_cannot_create_graph_handles_during_or_after_lease(ProtocolVersion protocol)
            throws Exception {
        String key = "java:{" + UUID.randomUUID() + "}:lease";
        try (ClientPool pool =
                ClientPool.create(
                        ClientPoolConfig.builder()
                                .maxSize(1)
                                .minIdle(0)
                                .clientConfig(
                                        GlideClientConfiguration.builder()
                                                .address(address(false))
                                                .protocol(protocol)
                                                .requestTimeout(5000)
                                                .build())
                                .build())) {
            PooledGlideClient lease = pool.acquire().get(10, TimeUnit.SECONDS);
            GlideClient cached = lease.unwrap();
            try {
                assertThrows(RequestException.class, () -> cached.selectGraph(key));
            } finally {
                lease.close();
            }
            assertThrows(IllegalStateException.class, lease::unwrap);
            assertThrows(RequestException.class, () -> cached.selectGraph(key));
            try (PooledGlideClient next = pool.acquire().get(10, TimeUnit.SECONDS)) {
                assertEquals(lease.getClientId(), next.getClientId());
                assertThrows(RequestException.class, () -> next.unwrap().selectGraph(key));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ProtocolVersion.class)
    void configured_replica_runs_readonly_entity_query_and_metadata(ProtocolVersion protocol)
            throws Exception {
        NodeAddress primaryAddress = address(false);
        NodeAddress replicaAddress = address("FALKORDB_REPLICA_ENDPOINT");
        String key = "java:{" + UUID.randomUUID() + "}:replica";
        try (GlideClient primary =
                        GlideClient.createClient(
                                        GlideClientConfiguration.builder()
                                                .address(primaryAddress)
                                                .protocol(protocol)
                                                .requestTimeout(5000)
                                                .build())
                                .get(10, TimeUnit.SECONDS);
                GlideClient reader =
                        GlideClient.createClient(
                                        GlideClientConfiguration.builder()
                                                .address(primaryAddress)
                                                .address(replicaAddress)
                                                .readFrom(ReadFrom.PREFER_REPLICA)
                                                .protocol(protocol)
                                                .inflightRequestsLimit(1)
                                                .requestTimeout(5000)
                                                .build())
                                .get(10, TimeUnit.SECONDS)) {
            try {
                primary
                        .selectGraph(key)
                        .query(
                                "CREATE (:JavaReplicaProbe {replicaMarker:'from-replica'})"
                                        + "-[:JAVA_REPLICA_EDGE {edgeMarker:'edge'}]->(:JavaReplicaOther)")
                        .get(10, TimeUnit.SECONDS);
                assertEquals(
                        1L,
                        primary.customCommand(new String[] {"WAIT", "1", "5000"}).get(10, TimeUnit.SECONDS));
                try (ReplicaMonitor monitor = ReplicaMonitor.start(replicaAddress, key)) {
                    GraphQueryResult result =
                            reader
                                    .selectGraph(key)
                                    .roQuery("MATCH p=(:JavaReplicaProbe)-[:JAVA_REPLICA_EDGE]->() RETURN p")
                                    .get(10, TimeUnit.SECONDS);
                    GraphPath path =
                            assertInstanceOf(GraphPath.class, result.getRows().get(0).get(0).getValue());
                    assertEquals(2, path.getNodes().size());
                    assertEquals("JAVA_REPLICA_EDGE", path.getRelationships().get(0).getType());
                    GraphNode node = path.getNodes().get(0);
                    assertEquals(Collections.singletonList("JavaReplicaProbe"), node.getLabels());
                    assertEquals("replicaMarker", node.getProperties().getEntries().get(0).getKey());
                    assertEquals(
                            "from-replica", node.getProperties().getEntries().get(0).getValue().getValue());
                    System.out.println(protocol + " replica evidence: " + monitor.proveQueryAndMetadata());
                }
            } finally {
                primary.del(new String[] {key}).get(10, TimeUnit.SECONDS);
            }
        }
        assertEquals(0, AsyncRegistry.getPendingCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {"python3", "./python3", ""})
    void monitor_rejects_relative_python_paths(String python) {
        assertThrows(IllegalArgumentException.class, () -> ReplicaMonitor.pythonExecutable(python));
    }

    private static final class ReplicaMonitor implements AutoCloseable {
        private final Process process;
        private final BufferedReader output;
        private final BufferedWriter input;

        private ReplicaMonitor(Process process) {
            this.process = process;
            output =
                    new BufferedReader(
                            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
            input =
                    new BufferedWriter(
                            new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
        }

        private static ReplicaMonitor start(NodeAddress replica, String graph) throws Exception {
            Process process =
                    new ProcessBuilder(
                                    pythonExecutable(
                                            System.getProperty("falkordb.monitor.python", "/usr/bin/python3")),
                                    System.getProperty("falkordb.monitor.script"),
                                    "--endpoint",
                                    replica.getHost() + ":" + replica.getPort(),
                                    "--graph",
                                    graph,
                                    "--timeout",
                                    "30")
                            .redirectErrorStream(true)
                            .start();
            ReplicaMonitor monitor = new ReplicaMonitor(process);
            try {
                assertEquals("READY", monitor.readLine(), "Replica monitor failed to become ready");
                return monitor;
            } catch (Exception | AssertionError failure) {
                monitor.close();
                throw failure;
            }
        }

        private static String pythonExecutable(String configured) throws IOException {
            Path python = Paths.get(configured);
            if (!python.isAbsolute()) {
                throw new IllegalArgumentException(
                        "FalkorDB monitor Python must be an absolute executable path");
            }
            return python.toRealPath().toString();
        }

        private String readLine() throws Exception {
            return CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return output.readLine();
                                } catch (IOException failure) {
                                    throw new CompletionException(failure);
                                }
                            })
                    .get(10, TimeUnit.SECONDS);
        }

        private String proveQueryAndMetadata() throws Exception {
            input.write("STOP\n");
            input.flush();
            String report = readLine();
            assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Replica monitor did not stop");
            assertEquals(0, process.exitValue(), report);
            assertNotNull(report, "Replica monitor returned no evidence");
            for (String flag : Arrays.asList("complete", "same_connection")) {
                assertTrue(
                        Pattern.compile("\"" + flag + "\"\\s*:\\s*true(?=\\s*[,}])").matcher(report).find(),
                        report);
            }
            Matcher counts = Pattern.compile("\"counts\"\\s*:\\s*\\{([^}]*)\\}").matcher(report);
            assertTrue(counts.find(), report);
            for (String kind :
                    Arrays.asList("query", "labels", "relationship_types", "property_keys", "write")) {
                Matcher count =
                        Pattern.compile("\"" + kind + "\"\\s*:\\s*([0-9]+)").matcher(counts.group(1));
                assertTrue(count.find(), report);
                assertEquals(kind.equals("write") ? "0" : "1", count.group(1), report);
            }
            return report;
        }

        @Override
        public void close() throws Exception {
            process.destroy();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Replica monitor did not terminate");
            }
            input.close();
            output.close();
        }
    }

    private static NodeAddress address(boolean cluster) {
        String variable = cluster ? "FALKORDB_CLUSTER_ENDPOINT" : "FALKORDB_STANDALONE_ENDPOINT";
        return address(variable);
    }

    private static NodeAddress address(String variable) {
        String endpoint = System.getenv(variable);
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalStateException(variable + " must point to the pinned FalkorDB harness");
        }
        int colon = endpoint.lastIndexOf(':');
        return NodeAddress.builder()
                .host(endpoint.substring(0, colon))
                .port(Integer.parseInt(endpoint.substring(colon + 1)))
                .build();
    }

    private static BaseClient connect(boolean cluster, ProtocolVersion protocol) throws Exception {
        return connect(cluster, protocol, 1000);
    }

    private static BaseClient connect(
            boolean cluster, ProtocolVersion protocol, int inflightRequestsLimit) throws Exception {
        NodeAddress address = address(cluster);
        return cluster
                ? GlideClusterClient.createClient(
                                GlideClusterClientConfiguration.builder()
                                        .address(address)
                                        .protocol(protocol)
                                        .inflightRequestsLimit(inflightRequestsLimit)
                                        .requestTimeout(5000)
                                        .build())
                        .get(10, TimeUnit.SECONDS)
                : GlideClient.createClient(
                                GlideClientConfiguration.builder()
                                        .address(address)
                                        .protocol(protocol)
                                        .inflightRequestsLimit(inflightRequestsLimit)
                                        .requestTimeout(5000)
                                        .build())
                        .get(10, TimeUnit.SECONDS);
    }
}
