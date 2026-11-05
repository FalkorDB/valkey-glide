/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import glide.api.models.exceptions.ClosingException;
import glide.api.models.exceptions.RequestException;
import glide.api.models.graph.GraphQueryOptions;
import glide.api.models.graph.GraphQueryResult;
import glide.api.models.pool.PooledGlideClient;
import glide.api.models.scope.IsolatedScope;
import glide.ffi.resolvers.NativeUtils;
import glide.internal.NativeGraphContext;
import glide.managers.CommandManager;
import graph_request.GraphRequest.QueryRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

class GraphTest {
    private CommandManager commands;
    private GlideClient client;
    private NativeGraphContext context;

    @BeforeEach
    void setup() {
        commands = mock(CommandManager.class);
        context = mock(NativeGraphContext.class);
        when(context.getName()).thenReturn("graph");
        when(commands.selectGraphContext(any())).thenReturn(context);
        try (MockedStatic<NativeUtils> ignored = mockStatic(NativeUtils.class)) {
            client = new GlideClient(new BaseClient.ClientBuilder(null, commands, null, null));
        }
    }

    @Test
    void select_is_local_and_retains_parent_database_and_epoch() throws Exception {
        when(context.getDatabase()).thenReturn(7L);
        when(context.getDatabaseEpoch()).thenReturn(4L);
        when(context.getName()).thenReturn("graph:{unicode-🌐}");
        Graph graph = client.selectGraph("graph:{unicode-🌐}");
        assertSame(client, graph.getClient());
        assertEquals(7, graph.getDatabase());
        assertEquals("graph:{unicode-🌐}", graph.getName());
        verify(commands).selectGraphContext("graph:{unicode-🌐}");
        verifyNoMoreInteractions(commands);

        when(commands.submitGraphQuery(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(empty()));
        graph
                .query(
                        "RETURN $x",
                        GraphQueryOptions.builder()
                                .params(Collections.singletonMap("x", Long.MIN_VALUE))
                                .timeoutMs(0L)
                                .build())
                .join();
        ArgumentCaptor<byte[]> bytes = ArgumentCaptor.forClass(byte[].class);
        verify(commands).submitGraphQuery(eq(context), bytes.capture());
        QueryRequest request = QueryRequest.parseFrom(bytes.getValue());
        assertEquals(7, request.getDatabase());
        assertEquals(4, request.getDatabaseEpoch());
        assertEquals(
                Long.MIN_VALUE, request.getParameters().getEntries(0).getValue().getIntegerValue());
        assertTrue(request.hasTimeoutMs());
        assertEquals(0, request.getTimeoutMs());
    }

    @Test
    void read_only_uses_same_transport_and_preserves_async_failure() throws Exception {
        when(commands.submitGraphQuery(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(empty()));
        client.selectGraph("graph").roQuery("RETURN 1").join();
        ArgumentCaptor<byte[]> bytes = ArgumentCaptor.forClass(byte[].class);
        verify(commands).submitGraphQuery(eq(context), bytes.capture());
        QueryRequest request = QueryRequest.parseFrom(bytes.getValue());
        assertTrue(request.getReadonly());
        assertTrue(!request.hasTimeoutMs());

        CompletableFuture<Object> failure = new CompletableFuture<>();
        ClosingException closed = new ClosingException("Client is closed");
        failure.completeExceptionally(closed);
        when(commands.submitGraphQuery(any(), any())).thenReturn(failure);
        CompletionException error =
                assertThrows(
                        CompletionException.class, () -> client.selectGraph("graph").query("RETURN 1").join());
        assertSame(closed, error.getCause());
    }

    @Test
    void invalid_or_unsettled_selection_rejects_locally() {
        assertThrows(NullPointerException.class, () -> client.selectGraph(null));
        assertThrows(IllegalArgumentException.class, () -> client.selectGraph(""));
        when(commands.selectGraphContext("graph"))
                .thenThrow(new RequestException("Database selection is unsettled"));
        assertThrows(RequestException.class, () -> client.selectGraph("graph"));
    }

    @Test
    void graph_wrappers_share_the_selected_native_context() {
        when(commands.submitGraphQuery(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(empty()));
        Graph selected = client.selectGraph("graph");
        Graph copy = new Graph(client, commands, context);
        selected.query("RETURN 1").join();
        copy.roQuery("RETURN 2").join();
        verify(commands).selectGraphContext("graph");
        verify(commands, org.mockito.Mockito.times(2)).submitGraphQuery(eq(context), any());
    }

    @Test
    void public_close_releases_only_selection_without_cancelling_inflight_query() {
        CompletableFuture<Object> pending = new CompletableFuture<>();
        when(commands.submitGraphQuery(eq(context), any())).thenReturn(pending);
        CompletableFuture<GraphQueryResult> result;
        Graph graph = client.selectGraph("graph");
        try (Graph owned = graph) {
            result = owned.roQuery("RETURN 1");
        }
        graph.close();
        verify(context, org.mockito.Mockito.times(2)).close();
        assertSame(client, graph.getClient());
        assertFalse(result.isDone());
        assertFalse(pending.isCancelled());
        pending.complete(empty());
        assertTrue(result.join().getRows().isEmpty());
        verify(commands).selectGraphContext("graph");
        verify(commands).submitGraphQuery(eq(context), any());
        verifyNoMoreInteractions(commands);
    }

    private static Map<String, Object> empty() {
        Map<String, Object> result = new HashMap<>();
        result.put("columns", new Object[0]);
        result.put("rows", new Object[0]);
        result.put("statistics", Collections.singletonMap("raw", new Object[0]));
        return result;
    }

    @Test
    void scope_and_lease_wrappers_do_not_expose_graph_selection() {
        assertFalse(BaseClient.class.isAssignableFrom(IsolatedScope.class));
        assertFalse(BaseClient.class.isAssignableFrom(PooledGlideClient.class));
        assertThrows(
                NoSuchMethodException.class,
                () -> IsolatedScope.class.getMethod("selectGraph", String.class));
        assertThrows(
                NoSuchMethodException.class,
                () -> PooledGlideClient.class.getMethod("selectGraph", String.class));
    }
}
