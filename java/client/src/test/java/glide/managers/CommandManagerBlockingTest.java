/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.managers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import command_request.CommandRequestOuterClass.RequestType;
import glide.api.models.GlideString;
import glide.api.models.configuration.RequestRoutingConfiguration.Route;
import glide.api.models.configuration.RequestRoutingConfiguration.SimpleSingleNodeRoute;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import response.ResponseOuterClass.Response;

/** Checks which custom commands skip the Java-side timer by taking the blocking path. */
public class CommandManagerBlockingTest {

    private static final String BLOCKING = "blocking";
    private static final String NORMAL = "normal";

    /** Records which submit path a custom command took, without sending anything. */
    private static class RecordingCommandManager extends CommandManager {
        String path;

        RecordingCommandManager() {
            super(null);
        }

        @Override
        protected <T> CompletableFuture<T> submitCommandAsync(
                RequestType requestType,
                byte[][] args,
                Route route,
                boolean expectUtf8Response,
                GlideExceptionCheckedFunction<Response, T> responseHandler) {
            path = NORMAL;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected <T> CompletableFuture<T> submitBlockingCommandAsync(
                RequestType requestType,
                byte[][] args,
                Route route,
                boolean expectUtf8Response,
                GlideExceptionCheckedFunction<Response, T> responseHandler) {
            path = BLOCKING;
            return CompletableFuture.completedFuture(null);
        }
    }

    private static String pathFor(String command) {
        RecordingCommandManager manager = new RecordingCommandManager();
        manager.submitCustomCommand(new String[] {command, "g", "RETURN 1"}, response -> null);
        String stringPath = manager.path;

        manager.path = null;
        manager.submitCustomCommand(
                new GlideString[] {GlideString.of(command), GlideString.of("g")}, response -> null);
        assertEquals(stringPath, manager.path, command + ": GlideString overload");

        manager.path = null;
        manager.submitCustomCommand(
                new String[] {command, "g"}, SimpleSingleNodeRoute.RANDOM, response -> null);
        assertEquals(stringPath, manager.path, command + ": routed overload");
        return stringPath;
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "GRAPH.QUERY",
                "GRAPH.RO_QUERY",
                "GRAPH.EXPLAIN",
                "GRAPH.PROFILE",
                "graph.query",
                "BLPOP"
            })
    public void blocking_custom_commands_skip_the_java_timer(String command) {
        assertEquals(BLOCKING, pathFor(command), command);
    }

    @ParameterizedTest
    @ValueSource(strings = {"GRAPH.COPY", "GRAPH.DELETE", "GRAPH.LIST", "GET"})
    public void other_custom_commands_use_the_java_timer(String command) {
        assertEquals(NORMAL, pathFor(command), command);
    }
}
