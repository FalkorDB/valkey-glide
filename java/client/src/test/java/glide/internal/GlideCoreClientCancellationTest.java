/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import glide.api.models.exceptions.RequestException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

class GlideCoreClientCancellationTest {
    private enum Transport {
        COMMAND,
        GRAPH
    }

    @BeforeEach
    void resetRegistry() {
        AsyncRegistry.reset();
    }

    @AfterEach
    void cleanupRegistry() {
        AsyncRegistry.reset();
    }

    private static Stream<Arguments> requests() {
        return Arrays.stream(Transport.values())
                .flatMap(
                        transport -> Stream.of(0L, 60_000L).map(timeout -> Arguments.of(transport, timeout)));
    }

    private static Stream<Arguments> completions() {
        return Arrays.stream(Transport.values())
                .flatMap(
                        transport ->
                                Stream.of(0L, 60_000L)
                                        .flatMap(
                                                timeout ->
                                                        Stream.of(false, true)
                                                                .map(failure -> Arguments.of(transport, timeout, failure))));
    }

    private static Stream<Arguments> nativeOutcomes() {
        return Arrays.stream(Transport.values())
                .flatMap(
                        transport -> Stream.of(false, true).map(failure -> Arguments.of(transport, failure)));
    }

    @ParameterizedTest
    @MethodSource("requests")
    void caller_cancellation_releases_registration_timeout_and_one_slot(
            Transport transport, long timeout) throws Exception {
        try (PendingRequest fixture = new PendingRequest(transport, timeout)) {
            CompletableFuture<Object> first = fixture.dispatch();
            long firstId = fixture.callback.get();
            CompletableFuture<?> registered =
                    (CompletableFuture<?>) registryEntry("activeFutures", firstId);
            ScheduledFuture<?> timer = (ScheduledFuture<?>) registryEntry("timeoutTasks", firstId);
            CompletableFuture<List<Integer>> atCompletion =
                    first.handle(
                            (value, error) -> {
                                assertInstanceOf(CancellationException.class, error);
                                return Arrays.asList(
                                        AsyncRegistry.getPendingCount(), AsyncRegistry.getPendingTimeoutCount());
                            });
            assertEquals(1, AsyncRegistry.getPendingCount());
            assertEquals(timeout == 0 ? 0 : 1, AsyncRegistry.getPendingTimeoutCount());
            assertTrue(first.cancel(false));
            assertTrue(first.isCancelled());
            assertEquals(Arrays.asList(0, 0), atCompletion.get(5, TimeUnit.SECONDS));
            assertTrue(registered.isCancelled());
            assertTrue(first.cancel(true), "Repeated cancellation must remain idempotent");
            if (timer != null) {
                assertTrue(timer.isCancelled());
            }

            CompletableFuture<Object> second = fixture.dispatch();
            long secondId = fixture.callback.get();
            assertNotEquals(firstId, secondId);
            assertFalse(second.isDone(), "Cancellation must release the Java one-request limit");
            assertFalse(AsyncRegistry.completeCallback(firstId, "late native value"));
            assertFalse(AsyncRegistry.completeCallbackWithErrorCode(firstId, 0, "late native error"));
            assertEquals(1, AsyncRegistry.getPendingCount());
            assertTrue(first.isCancelled());
            assertThrows(CancellationException.class, first::join);
            CompletableFuture<Object> third = fixture.dispatch();
            assertInstanceOf(
                    RequestException.class, assertThrows(CompletionException.class, third::join).getCause());
            assertFalse(third.cancel(false));
            assertTrue(AsyncRegistry.completeCallback(secondId, "second"));
            assertEquals("second", second.get(5, TimeUnit.SECONDS));
            assertEquals(0, AsyncRegistry.getPendingCount());
            assertEquals(0, AsyncRegistry.getPendingTimeoutCount());
        }
    }

    @ParameterizedTest
    @MethodSource("requests")
    void registered_cancellation_is_exposed_only_after_cleanup(Transport transport, long timeout)
            throws Exception {
        try (PendingRequest fixture = new PendingRequest(transport, timeout)) {
            CompletableFuture<Object> result = fixture.dispatch();
            CompletableFuture<?> registered =
                    (CompletableFuture<?>) registryEntry("activeFutures", fixture.callback.get());
            CompletableFuture<Integer> atCompletion =
                    result.handle((value, error) -> AsyncRegistry.getPendingCount());
            assertTrue(registered.cancel(true));
            assertTrue(result.isCancelled());
            assertEquals(0, atCompletion.get(5, TimeUnit.SECONDS));
            assertEquals(0, AsyncRegistry.getPendingTimeoutCount());
        }
    }

    @ParameterizedTest
    @MethodSource("completions")
    void normal_and_error_completion_keep_the_cleanup_boundary(
            Transport transport, long timeout, boolean failure) throws Exception {
        try (PendingRequest fixture = new PendingRequest(transport, timeout)) {
            CompletableFuture<Object> result = fixture.dispatch();
            long callback = fixture.callback.get();
            CompletableFuture<List<Integer>> atCompletion =
                    result.handle(
                            (value, error) ->
                                    Arrays.asList(
                                            AsyncRegistry.getPendingCount(), AsyncRegistry.getPendingTimeoutCount()));
            if (failure) {
                assertTrue(AsyncRegistry.completeCallbackWithErrorCode(callback, 0, "expected error"));
                assertInstanceOf(
                        RequestException.class,
                        assertThrows(CompletionException.class, result::join).getCause());
            } else {
                assertTrue(AsyncRegistry.completeCallback(callback, "value"));
                assertEquals("value", result.get(5, TimeUnit.SECONDS));
            }
            assertEquals(Arrays.asList(0, 0), atCompletion.get(5, TimeUnit.SECONDS));
            assertFalse(result.isCancelled());
            assertFalse(result.cancel(true), "Cancellation must not replace a completed native outcome");
        }
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    void cancellation_is_not_observable_while_registry_cleanup_is_in_progress(Transport transport)
            throws Exception {
        try (PendingRequest fixture = new PendingRequest(transport, 60_000)) {
            CompletableFuture<Object> result = fixture.dispatch();
            long callback = fixture.callback.get();
            ScheduledFuture<?> timer = (ScheduledFuture<?>) registryEntry("timeoutTasks", callback);
            ScheduledFuture<?> heldTimer = mock(ScheduledFuture.class);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            doAnswer(
                            invocation -> {
                                entered.countDown();
                                assertTrue(release.await(5, TimeUnit.SECONDS));
                                return timer.cancel(false);
                            })
                    .when(heldTimer)
                    .cancel(false);
            registryEntries("timeoutTasks").replace(callback, heldTimer);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                CompletableFuture<Boolean> cancelled =
                        CompletableFuture.supplyAsync(() -> result.cancel(false), executor);
                assertTrue(
                        entered.await(5, TimeUnit.SECONDS), "Cancellation never reached registry cleanup");
                assertFalse(result.isDone(), "Cancellation must not be exposed before local cleanup");
                CountDownLatch secondEntered = new CountDownLatch(1);
                CompletableFuture<Boolean> secondCancellation =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    secondEntered.countDown();
                                    return result.cancel(true);
                                },
                                executor);
                assertTrue(secondEntered.await(5, TimeUnit.SECONDS));
                assertThrows(
                        TimeoutException.class, () -> secondCancellation.get(100, TimeUnit.MILLISECONDS));
                assertFalse(result.isDone(), "Concurrent cancellation must also wait for cleanup");
                release.countDown();
                assertTrue(cancelled.get(5, TimeUnit.SECONDS));
                assertTrue(secondCancellation.get(5, TimeUnit.SECONDS));
                assertTrue(result.isCancelled());
                assertTrue(timer.isCancelled());
                assertEquals(0, AsyncRegistry.getPendingCount());
            } finally {
                release.countDown();
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("nativeOutcomes")
    void unsuccessful_cancellation_of_a_completed_wrapper_does_not_cancel_its_registered_request(
            Transport transport, boolean failure) throws Exception {
        try (PendingRequest fixture = new PendingRequest(transport, 60_000)) {
            CompletableFuture<Object> result = fixture.dispatch();
            long callback = fixture.callback.get();
            CompletableFuture<?> registered =
                    (CompletableFuture<?>) registryEntry("activeFutures", callback);
            if (failure) {
                assertTrue(result.completeExceptionally(new IllegalStateException("caller-completed")));
            } else {
                assertTrue(result.complete("caller-completed"));
            }
            assertFalse(result.cancel(false));
            assertFalse(registered.isCancelled());
            assertEquals(1, AsyncRegistry.getPendingCount());
            assertEquals(1, AsyncRegistry.getPendingTimeoutCount());
            assertTrue(AsyncRegistry.completeCallback(callback, "native value"));
            assertEquals(0, AsyncRegistry.getPendingCount());
            assertEquals(0, AsyncRegistry.getPendingTimeoutCount());
            if (failure) {
                assertInstanceOf(
                        IllegalStateException.class,
                        assertThrows(CompletionException.class, result::join).getCause());
            } else {
                assertEquals("caller-completed", result.join());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("nativeOutcomes")
    void cancellation_cannot_replace_a_native_outcome_while_its_cleanup_is_in_progress(
            Transport transport, boolean failure) throws Exception {
        try (PendingRequest fixture = new PendingRequest(transport, 60_000)) {
            CompletableFuture<Object> result = fixture.dispatch();
            long callback = fixture.callback.get();
            ScheduledFuture<?> timer = (ScheduledFuture<?>) registryEntry("timeoutTasks", callback);
            ScheduledFuture<?> heldTimer = mock(ScheduledFuture.class);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            doAnswer(
                            invocation -> {
                                entered.countDown();
                                assertTrue(release.await(5, TimeUnit.SECONDS));
                                return timer.cancel(false);
                            })
                    .when(heldTimer)
                    .cancel(false);
            registryEntries("timeoutTasks").replace(callback, heldTimer);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                CompletableFuture<Boolean> nativeCompletion =
                        CompletableFuture.supplyAsync(
                                () ->
                                        failure
                                                ? AsyncRegistry.completeCallbackWithErrorCode(callback, 0, "expected error")
                                                : AsyncRegistry.completeCallback(callback, "value"),
                                executor);
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertFalse(result.isDone(), "Native completion must wait for local cleanup");
                assertFalse(result.cancel(false), "The registered native outcome has already won");
                assertFalse(result.isDone());
                release.countDown();
                assertTrue(nativeCompletion.get(5, TimeUnit.SECONDS));
                if (failure) {
                    assertInstanceOf(
                            RequestException.class,
                            assertThrows(CompletionException.class, result::join).getCause());
                } else {
                    assertEquals("value", result.get(5, TimeUnit.SECONDS));
                }
                assertFalse(result.isCancelled());
                assertTrue(timer.isCancelled());
                assertEquals(0, AsyncRegistry.getPendingCount());
                assertEquals(0, AsyncRegistry.getPendingTimeoutCount());
            } finally {
                release.countDown();
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    private static Object registryEntry(String name, long callback) throws Exception {
        return registryEntries(name).get(callback);
    }

    private static Map<Long, Object> registryEntries(String name) throws Exception {
        Field field = AsyncRegistry.class.getDeclaredField(name);
        field.setAccessible(true);
        @SuppressWarnings("unchecked") // Test-only inspection of the registry's long-keyed maps.
        Map<Long, Object> entries = (Map<Long, Object>) field.get(null);
        return entries;
    }

    private static final class PendingRequest implements AutoCloseable {
        private static final long HANDLE = Long.MAX_VALUE - 205;
        private final Transport transport;
        private final long timeout;
        private final GlideCoreClient client;
        private final NativeGraphContext context;
        private final MockedStatic<AsyncRegistry> registry;
        private final AtomicLong callback = new AtomicLong();

        private PendingRequest(Transport transport, long timeout) {
            this.transport = transport;
            this.timeout = timeout;
            client = new GlideCoreClient(HANDLE, 1, timeout);
            context = mock(NativeGraphContext.class);
            registry = mockStatic(AsyncRegistry.class, CALLS_REAL_METHODS);
            registry
                    .when(() -> AsyncRegistry.completeCallbackWithErrorCode(anyLong(), anyInt(), anyString()))
                    .thenAnswer(
                            invocation -> {
                                String error = invocation.getArgument(2);
                                if (error.startsWith("Failed to extract command args:")) {
                                    // Defer a synchronous JNI rejection to simulate a pending native callback.
                                    callback.set(invocation.getArgument(0));
                                    return false;
                                }
                                return invocation.callRealMethod();
                            });
            doAnswer(
                            invocation -> {
                                callback.set(invocation.getArgument(2));
                                return null;
                            })
                    .when(context)
                    .dispatch(eq(client), eq(HANDLE), anyLong(), any());
        }

        private CompletableFuture<Object> dispatch() {
            if (transport == Transport.GRAPH) {
                return client.executeGraphQueryAsync(context, new byte[0]);
            }
            return client.executeCommandAsync(0, new byte[][] {null}, false, 0, null, true, timeout, 0);
        }

        @Override
        public void close() {
            registry.close();
            client.close();
        }
    }
}
