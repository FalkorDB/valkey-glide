/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import glide.api.models.exceptions.ClosingException;
import glide.api.models.exceptions.RequestException;
import java.lang.ref.PhantomReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NativeGraphContextTest {
    @Test
    void every_close_waits_for_invalidation_when_another_closer_has_claimed_cleanup()
            throws Exception {
        AsyncRegistry.reset();
        try (GlideCoreClient owner = new GlideCoreClient(Long.MAX_VALUE - 301, 1, 0)) {
            Constructor<NativeGraphContext> constructor =
                    NativeGraphContext.class.getDeclaredConstructor(
                            GlideCoreClient.class, String.class, long[].class);
            constructor.setAccessible(true);
            NativeGraphContext context =
                    constructor.newInstance(owner, "close-race", new long[] {Long.MAX_VALUE - 302, 0, 0});
            Field stateField = NativeGraphContext.class.getDeclaredField("state");
            stateField.setAccessible(true);
            Object state = stateField.get(context);
            Field cleanupField = NativeGraphContext.class.getDeclaredField("cleanup");
            cleanupField.setAccessible(true);
            Object cleanup = cleanupField.get(context);
            Field actionsField = GlideCoreClient.class.getDeclaredField("CLEANUP_ACTIONS");
            actionsField.setAccessible(true);
            Map<?, ?> actions = (Map<?, ?>) actionsField.get(null);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                CompletableFuture<Void> first;
                CompletableFuture<Void> second;
                synchronized (state) {
                    first = CompletableFuture.runAsync(context::close, executor);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (actions.containsKey(cleanup)) {
                        assertTrue(System.nanoTime() < deadline, "First closer never claimed cleanup");
                        Thread.yield();
                    }
                    assertFalse(first.isDone(), "The first closer must be blocked before invalidation");
                    CountDownLatch secondEntered = new CountDownLatch(1);
                    second =
                            CompletableFuture.runAsync(
                                    () -> {
                                        secondEntered.countDown();
                                        context.close();
                                    },
                                    executor);
                    assertTrue(secondEntered.await(5, TimeUnit.SECONDS));
                    assertThrows(TimeoutException.class, () -> second.get(100, TimeUnit.MILLISECONDS));
                }
                first.get(5, TimeUnit.SECONDS);
                second.get(5, TimeUnit.SECONDS);
                ClosingException closed =
                        assertThrows(
                                ClosingException.class,
                                () -> context.dispatch(owner, owner.getNativeHandle(), 1, new byte[0]));
                assertEquals("Graph context is released", closed.getMessage());
                assertInstanceOf(
                        ClosingException.class,
                        assertThrows(
                                        CompletionException.class,
                                        () -> owner.executeGraphQueryAsync(context, new byte[0]).join())
                                .getCause());
                assertEquals(0, AsyncRegistry.getPendingCount());
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
                context.close();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rejected_ordinary_graph_cleanup_finishes_without_registration(boolean closed) {
        AsyncRegistry.reset();
        try (GlideCoreClient client = new GlideCoreClient(Long.MAX_VALUE - 103, 1, 0)) {
            if (closed) {
                client.close();
            } else {
                AsyncRegistry.failAllWithError("test shutdown");
            }
            CompletableFuture<Object> rejected =
                    client.executeCommandAsync(0, new byte[0][], false, 0, null, true, 0, 0);
            assertTrue(rejected.isCompletedExceptionally());
            assertInstanceOf(
                    ClosingException.class,
                    assertThrows(CompletionException.class, rejected::join).getCause());
            assertEquals(0, AsyncRegistry.getPendingCount());
        } finally {
            AsyncRegistry.reset();
        }
    }

    @Test
    void ordinary_graph_cleanup_inflight_rejection_does_not_leave_a_cleanup_barrier_pending() {
        AsyncRegistry.reset();
        long handle = Long.MAX_VALUE - 104;
        try (GlideCoreClient client = new GlideCoreClient(handle, 1, 0)) {
            CompletableFuture<Object> pending = new CompletableFuture<>();
            AsyncRegistry.register(pending, 1, handle, 0);
            CompletableFuture<Object> rejected =
                    client.executeCommandAsync(0, new byte[0][], false, 0, null, true, 0, 0);
            assertTrue(rejected.isCompletedExceptionally());
            assertInstanceOf(
                    RequestException.class,
                    assertThrows(CompletionException.class, rejected::join).getCause());
            assertEquals(1, AsyncRegistry.getPendingCount());
            pending.complete(null);
            assertEquals(0, AsyncRegistry.getPendingCount());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rejected_graph_dispatch_finishes_without_a_registered_cleanup_hook(boolean closed) {
        AsyncRegistry.reset();
        try (GlideCoreClient client = new GlideCoreClient(Long.MAX_VALUE - 101, 1, 0)) {
            if (closed) {
                client.close();
            } else {
                AsyncRegistry.failAllWithError("test shutdown");
            }
            CompletableFuture<Object> rejected =
                    client.executeGraphQueryAsync(mock(NativeGraphContext.class), new byte[0]);
            assertTrue(rejected.isCompletedExceptionally());
            assertInstanceOf(
                    ClosingException.class,
                    assertThrows(CompletionException.class, rejected::join).getCause());
        } finally {
            AsyncRegistry.reset();
        }
    }

    @Test
    void synchronous_graph_dispatch_failure_cleans_registration_before_retry() {
        AsyncRegistry.reset();
        long handle = Long.MAX_VALUE - 102;
        try (GlideCoreClient client = new GlideCoreClient(handle, 1, 0)) {
            NativeGraphContext context = mock(NativeGraphContext.class);
            AtomicInteger calls = new AtomicInteger();
            doAnswer(
                            invocation -> {
                                if (calls.incrementAndGet() == 1) {
                                    throw new ClosingException("released context");
                                }
                                AsyncRegistry.completeCallback(invocation.getArgument(2), "second");
                                return null;
                            })
                    .when(context)
                    .dispatch(eq(client), eq(handle), anyLong(), any());
            CompletableFuture<Object> rejected = client.executeGraphQueryAsync(context, new byte[0]);
            assertTrue(rejected.isCompletedExceptionally());
            assertEquals(0, AsyncRegistry.getPendingCount());
            assertEquals("second", client.executeGraphQueryAsync(context, new byte[0]).join());
            assertEquals(0, AsyncRegistry.getPendingCount());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void graph_completion_releases_inflight_slot_before_next_dispatch(boolean failure) {
        AsyncRegistry.reset();
        long handle = Long.MAX_VALUE - 100;
        try (GlideCoreClient client = new GlideCoreClient(handle, 1, 0)) {
            NativeGraphContext context = mock(NativeGraphContext.class);
            AtomicLong callback = new AtomicLong();
            doAnswer(
                            invocation -> {
                                callback.set(invocation.getArgument(2));
                                return null;
                            })
                    .when(context)
                    .dispatch(eq(client), eq(handle), anyLong(), any());
            CompletableFuture<Object> first = client.executeGraphQueryAsync(context, new byte[0]);
            CompletableFuture<Object> chained =
                    first
                            .handle((value, error) -> client.executeGraphQueryAsync(context, new byte[0]))
                            .thenCompose(next -> next);
            long firstId = callback.get();
            if (failure) {
                AsyncRegistry.completeCallbackWithErrorCode(firstId, 0, "expected graph error");
            } else {
                AsyncRegistry.completeCallback(firstId, "first");
            }
            assertFalse(
                    chained.isCompletedExceptionally(),
                    "A completed graph request must release its inflight slot before dependent callbacks");
            assertNotEquals(firstId, callback.get());
            AsyncRegistry.completeCallback(callback.get(), "second");
            assertEquals("second", chained.join());
            assertEquals(0, AsyncRegistry.getPendingCount());
        }
    }

    @Test
    void existing_cleanup_worker_releases_registered_graph_owner_once() throws Exception {
        AtomicInteger releases = new AtomicInteger();
        CountDownLatch cleaned = new CountDownLatch(1);
        Object owner = new Object();
        synchronized (owner) {
            PhantomReference<Object> reference =
                    GlideCoreClient.registerNativeCleanup(
                            owner,
                            () -> {
                                releases.incrementAndGet();
                                cleaned.countDown();
                            });
            assertTrue(reference.enqueue());
            assertTrue(cleaned.await(5, TimeUnit.SECONDS));
            GlideCoreClient.runNativeCleanup(reference);
        }
        assertEquals(1, releases.get());
    }
}
