/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import command_request.CommandRequestOuterClass.RequestType;
import glide.api.BaseClient;
import glide.api.GlideClient;
import glide.api.GlideClusterClient;
import glide.api.Graph;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.configuration.ProtocolVersion;
import glide.api.models.graph.GraphQueryOptions;
import glide.managers.CommandManager;
import graph_request.GraphRequest;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Actual transport cancellation, opted into only by :client:graphTest. */
@Tag("falkordb")
@ExtendWith(GlideCoreClientCancellationLiveTest.NativeRegistryLifetime.class)
class GlideCoreClientCancellationLiveTest {
    private static final AtomicLong COUNTER_KEYS = new AtomicLong(Long.MAX_VALUE);

    private static Stream<Arguments> clients() {
        return Stream.of(false, true)
                .flatMap(
                        cluster ->
                                Arrays.stream(ProtocolVersion.values())
                                        .flatMap(
                                                protocol ->
                                                        Stream.of(false, true)
                                                                .map(graph -> Arguments.of(cluster, protocol, graph))));
    }

    @ParameterizedTest
    @MethodSource("clients")
    void actual_cancellation_releases_one_java_slot_without_aborting_server_work(
            boolean cluster, ProtocolVersion protocol, boolean graphRequest) throws Exception {
        String prefix = "java:{" + UUID.randomUUID() + "}:cancellation:";
        String graphKey = prefix + "graph";
        String listKey = prefix + "list";
        try (BaseClient owner = connect(cluster, protocol);
                BaseClient control = connect(cluster, protocol);
                GlideCoreClient oneSlot =
                        new GlideCoreClient(
                                coreClient(owner).getNativeHandle(), 1, 0, COUNTER_KEYS.getAndDecrement())) {
            boolean blockingSubmitted = false;
            boolean released = false;
            try {
                try (Graph seed = owner.selectGraph(graphKey)) {
                    seed.query("CREATE ()").get(10, TimeUnit.SECONDS);
                }
                try (NativeGraphContext context = oneSlot.selectGraphContext(graphKey)) {
                    blockingSubmitted = !graphRequest;
                    CompletableFuture<Object> first =
                            graphRequest
                                    ? oneSlot.executeGraphQueryAsync(
                                            context,
                                            GraphRequest.QueryRequest.newBuilder()
                                                    .setGraphName(context.getName())
                                                    .setDatabase((int) context.getDatabase())
                                                    .setDatabaseEpoch(context.getDatabaseEpoch())
                                                    .setReadonly(true)
                                                    .setQuery("UNWIND range(0,1000000) AS i RETURN sum(i)")
                                                    .setParameters(GraphQueryOptions.builder().build().toProtobuf())
                                                    .setTimeoutMs(2000)
                                                    .build()
                                                    .toByteArray())
                                    : command(oneSlot, "BLPOP", listKey, "0");
                    assertFalse(first.isDone(), "Native fixture must still be pending at cancellation");
                    assertEquals(1, AsyncRegistry.getPendingCount());
                    assertEquals(0, AsyncRegistry.getPendingTimeoutCount());
                    CompletableFuture<?> registered = registeredFuture();
                    CompletableFuture<Integer> atCompletion =
                            first.handle(
                                    (value, error) -> {
                                        assertInstanceOf(CancellationException.class, error);
                                        return AsyncRegistry.getPendingCount();
                                    });
                    assertTrue(first.cancel(false));
                    assertTrue(first.isCancelled());
                    assertTrue(registered.isCancelled());
                    assertEquals(0, atCompletion.get(10, TimeUnit.SECONDS));
                    CompletableFuture<Object> next = command(oneSlot, "EXISTS", graphKey);
                    // Cancellation is Java bookkeeping, not BLPOP/server-work abortion.
                    if (blockingSubmitted) {
                        control.rpush(listKey, new String[] {"release"}).get(10, TimeUnit.SECONDS);
                        released = true;
                    }
                    assertEquals(1L, next.get(10, TimeUnit.SECONDS));
                    assertTrue(first.isCancelled(), "A late native callback must not replace cancellation");
                    assertEquals(0, AsyncRegistry.getPendingCount());
                    assertEquals(0, AsyncRegistry.getPendingTimeoutCount());
                }
            } finally {
                try {
                    if (blockingSubmitted) {
                        if (!released) {
                            control.rpush(listKey, new String[] {"release"}).get(10, TimeUnit.SECONDS);
                        }
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (control.llen(listKey).get(10, TimeUnit.SECONDS) != 0) {
                            assertTrue(System.nanoTime() < deadline, "Owned blocking command was not released");
                            Thread.sleep(10);
                        }
                    }
                } finally {
                    control.del(new String[] {listKey, graphKey}).get(10, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static CompletableFuture<Object> command(GlideCoreClient client, String... arguments) {
        byte[][] args =
                Arrays.stream(arguments)
                        .map(value -> value.getBytes(StandardCharsets.UTF_8))
                        .toArray(byte[][]::new);
        return client.executeCommandAsync(
                RequestType.CustomCommand.getNumber(), args, false, 0, null, true, 0, 0);
    }

    private static CompletableFuture<?> registeredFuture() throws Exception {
        Field field = AsyncRegistry.class.getDeclaredField("activeFutures");
        field.setAccessible(true);
        @SuppressWarnings(
                "unchecked") // Test-only inspection; no registry API or native cleanup handle.
        Map<Long, CompletableFuture<?>> entries = (Map<Long, CompletableFuture<?>>) field.get(null);
        assertEquals(1, entries.size());
        return entries.values().iterator().next();
    }

    private static GlideCoreClient coreClient(BaseClient client) throws Exception {
        Field manager = BaseClient.class.getDeclaredField("commandManager");
        manager.setAccessible(true);
        Field core = CommandManager.class.getDeclaredField("coreClient");
        core.setAccessible(true);
        return (GlideCoreClient) core.get(manager.get(client));
    }

    private static BaseClient connect(boolean cluster, ProtocolVersion protocol) throws Exception {
        String variable = cluster ? "FALKORDB_CLUSTER_ENDPOINT" : "FALKORDB_STANDALONE_ENDPOINT";
        String endpoint = System.getenv(variable);
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalStateException(variable + " must point to the pinned FalkorDB harness");
        }
        int colon = endpoint.lastIndexOf(':');
        NodeAddress address =
                NodeAddress.builder()
                        .host(endpoint.substring(0, colon))
                        .port(Integer.parseInt(endpoint.substring(colon + 1)))
                        .build();
        return cluster
                ? GlideClusterClient.createClient(
                                GlideClusterClientConfiguration.builder()
                                        .address(address)
                                        .protocol(protocol)
                                        .inflightRequestsLimit(1000)
                                        .requestTimeout(5000)
                                        .build())
                        .get(10, TimeUnit.SECONDS)
                : GlideClient.createClient(
                                GlideClientConfiguration.builder()
                                        .address(address)
                                        .protocol(protocol)
                                        .inflightRequestsLimit(1000)
                                        .requestTimeout(5000)
                                        .build())
                        .get(10, TimeUnit.SECONDS);
    }

    static final class NativeRegistryLifetime implements BeforeEachCallback {
        @Override
        public void beforeEach(ExtensionContext context) {
            // Keep IDs monotonic until all tests finish; canceled native work may still callback.
            context
                    .getRoot()
                    .getStore(ExtensionContext.Namespace.create(NativeRegistryLifetime.class))
                    .getOrComputeIfAbsent(
                            NativeRegistryLifetime.class,
                            key -> {
                                try {
                                    BaseClient guard = connect(false, ProtocolVersion.RESP3);
                                    return (ExtensionContext.Store.CloseableResource) guard::close;
                                } catch (Exception error) {
                                    throw new IllegalStateException("Cannot retain owned registry lifetime", error);
                                }
                            },
                            ExtensionContext.Store.CloseableResource.class);
        }
    }
}
