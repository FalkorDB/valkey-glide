/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import glide.api.models.graph.GraphQueryOptions;
import glide.api.models.graph.GraphQueryResult;
import glide.internal.GraphResponseMapper;
import glide.internal.NativeGraphContext;
import glide.managers.CommandManager;
import graph_request.GraphRequest;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A local FalkorDB graph selection. Graph lifetime is independent of graph existence on the server.
 * Closing the parent client makes subsequent operations fail with the normal closing exception.
 */
public final class Graph {
    private final BaseClient parent;
    private final CommandManager commands;
    private final NativeGraphContext context;

    Graph(BaseClient parent, CommandManager commands, NativeGraphContext context) {
        this.parent = parent;
        this.commands = commands;
        this.context = context;
    }

    /** Return the parent client retained by this local handle. */
    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP",
            justification = "A graph deliberately retains and exposes the caller-owned parent client")
    public BaseClient getClient() {
        return parent;
    }

    /** Return the graph key exactly as selected. */
    public String getName() {
        return context.getName();
    }

    /** Return the database captured when this handle was selected. */
    public long getDatabase() {
        return context.getDatabase();
    }

    /** Execute a Cypher query. Use parameters rather than interpolating values into Cypher. */
    public CompletableFuture<GraphQueryResult> query(String query) {
        return query(query, GraphQueryOptions.builder().build());
    }

    /** Execute a Cypher query with typed parameters and optional server timeout. */
    public CompletableFuture<GraphQueryResult> query(String query, GraphQueryOptions options) {
        return execute(query, options, false);
    }

    /** Execute a read-only Cypher query using GRAPH.RO_QUERY. */
    public CompletableFuture<GraphQueryResult> roQuery(String query) {
        return roQuery(query, GraphQueryOptions.builder().build());
    }

    /** Execute a read-only Cypher query with typed parameters and optional server timeout. */
    public CompletableFuture<GraphQueryResult> roQuery(String query, GraphQueryOptions options) {
        return execute(query, options, true);
    }

    private CompletableFuture<GraphQueryResult> execute(
            String query, GraphQueryOptions options, boolean readonly) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(options, "options");
        GraphRequest.QueryRequest.Builder request =
                GraphRequest.QueryRequest.newBuilder()
                        .setGraphName(context.getName())
                        .setQuery(query)
                        .setDatabase((int) context.getDatabase())
                        .setDatabaseEpoch(context.getDatabaseEpoch())
                        .setReadonly(readonly)
                        .setParameters(options.toProtobuf());
        if (options.getTimeoutMs() != null) {
            request.setTimeoutMs(options.getTimeoutMs());
        }
        return commands
                .submitGraphQuery(context, request.build().toByteArray())
                // The bound callback keeps this graph and its parent alive until native completion.
                .thenApply(this::materialize);
    }

    private GraphQueryResult materialize(Object response) {
        return GraphResponseMapper.map(response);
    }
}
