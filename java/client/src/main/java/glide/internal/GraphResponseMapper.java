/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import glide.api.models.exceptions.RequestException;
import glide.api.models.graph.GraphDate;
import glide.api.models.graph.GraphDateTime;
import glide.api.models.graph.GraphDuration;
import glide.api.models.graph.GraphMap;
import glide.api.models.graph.GraphNode;
import glide.api.models.graph.GraphPath;
import glide.api.models.graph.GraphPoint;
import glide.api.models.graph.GraphQueryResult;
import glide.api.models.graph.GraphRelationship;
import glide.api.models.graph.GraphStatistics;
import glide.api.models.graph.GraphTime;
import glide.api.models.graph.GraphValue;
import glide.api.models.graph.GraphVectorF32;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Materializes only the canonical graph codec result. No compact protocol or schema parsing. */
public final class GraphResponseMapper {
    private GraphResponseMapper() {}

    /** Convert a canonical core response into immutable Java graph models. */
    public static GraphQueryResult map(Object response) {
        Map<?, ?> result = fields(response);
        List<String> columns = strings(required(result, "columns"));
        List<List<GraphValue>> rows = new ArrayList<>();
        for (Object rawRow : sequence(required(result, "rows"))) {
            List<GraphValue> row = new ArrayList<>();
            for (Object cell : sequence(rawRow)) {
                row.add(value(cell));
            }
            if (row.size() != columns.size()) {
                throw invalid("row width differs from columns");
            }
            rows.add(row);
        }
        return new GraphQueryResult(columns, rows, statistics(required(result, "statistics")));
    }

    private static GraphValue value(Object envelope) {
        Map<?, ?> cell = fields(envelope);
        String kind = typed(required(cell, "kind"), String.class);
        if (!cell.containsKey("value")) {
            throw invalid("missing value");
        }
        Object payload = cell.get("value");
        Object materialized;
        switch (kind) {
            case "null":
                if (payload != null) {
                    throw invalid("null payload is not null");
                }
                materialized = null;
                break;
            case "string":
                materialized = typed(payload, String.class);
                break;
            case "integer":
                materialized = typed(payload, Long.class);
                break;
            case "boolean":
                materialized = typed(payload, Boolean.class);
                break;
            case "double":
                materialized = typed(payload, Double.class);
                break;
            case "list":
                List<GraphValue> list = new ArrayList<>();
                for (Object item : sequence(payload)) {
                    list.add(value(item));
                }
                materialized = Collections.unmodifiableList(list);
                break;
            case "map":
                materialized = graphMap(payload);
                break;
            case "node":
                materialized = node(payload);
                break;
            case "relationship":
                materialized = relationship(payload);
                break;
            case "path":
                Map<?, ?> path = fields(payload);
                List<GraphNode> nodes = new ArrayList<>();
                for (Object item : sequence(required(path, "nodes"))) {
                    nodes.add(typed(value(item).getValue(), GraphNode.class));
                }
                List<GraphRelationship> relationships = new ArrayList<>();
                for (Object item : sequence(required(path, "relationships"))) {
                    relationships.add(typed(value(item).getValue(), GraphRelationship.class));
                }
                materialized = new GraphPath(nodes, relationships);
                break;
            case "point":
                Map<?, ?> point = fields(payload);
                materialized =
                        new GraphPoint(
                                typed(required(point, "latitude"), Double.class),
                                typed(required(point, "longitude"), Double.class));
                break;
            case "vector":
                List<Float> components = new ArrayList<>();
                for (Object item : sequence(payload)) {
                    double component = typed(item, Double.class);
                    float narrowed = (float) component;
                    if (Double.compare(component, (double) narrowed) != 0
                            && !(Double.isNaN(component) && Float.isNaN(narrowed))) {
                        throw invalid("vector component is not an exact float32");
                    }
                    components.add(narrowed);
                }
                materialized = new GraphVectorF32(components);
                break;
            case "datetime":
                materialized = new GraphDateTime(typed(payload, Long.class));
                break;
            case "date":
                materialized = new GraphDate(typed(payload, Long.class));
                break;
            case "time":
                materialized = new GraphTime(typed(payload, Long.class));
                break;
            case "duration":
                materialized = new GraphDuration(typed(payload, Long.class));
                break;
            default:
                throw invalid("unknown value kind " + kind);
        }
        return new GraphValue(GraphValue.Kind.valueOf(kind.toUpperCase(Locale.ROOT)), materialized);
    }

    private static GraphNode node(Object payload) {
        Map<?, ?> node = fields(payload);
        return new GraphNode(
                typed(required(node, "id"), Long.class),
                strings(required(node, "labels")),
                graphMap(required(node, "properties")));
    }

    private static GraphRelationship relationship(Object payload) {
        Map<?, ?> relationship = fields(payload);
        return new GraphRelationship(
                typed(required(relationship, "id"), Long.class),
                typed(required(relationship, "type"), String.class),
                typed(required(relationship, "source_id"), Long.class),
                typed(required(relationship, "destination_id"), Long.class),
                graphMap(required(relationship, "properties")));
    }

    private static GraphMap graphMap(Object payload) {
        List<GraphMap.Entry> entries = new ArrayList<>();
        for (Object item : sequence(payload)) {
            List<?> entry = sequence(item);
            if (entry.size() != 2) {
                throw invalid("map entry must have two elements");
            }
            entries.add(new GraphMap.Entry(typed(entry.get(0), String.class), value(entry.get(1))));
        }
        return new GraphMap(entries);
    }

    private static GraphStatistics statistics(Object payload) {
        Map<?, ?> stats = fields(payload);
        return GraphStatistics.builder()
                .raw(strings(required(stats, "raw")))
                .labelsAdded(optionalUnsigned(stats, "labels_added"))
                .labelsRemoved(optionalUnsigned(stats, "labels_removed"))
                .nodesCreated(optionalUnsigned(stats, "nodes_created"))
                .nodesDeleted(optionalUnsigned(stats, "nodes_deleted"))
                .propertiesSet(optionalUnsigned(stats, "properties_set"))
                .propertiesRemoved(optionalUnsigned(stats, "properties_removed"))
                .relationshipsCreated(optionalUnsigned(stats, "relationships_created"))
                .relationshipsDeleted(optionalUnsigned(stats, "relationships_deleted"))
                .indicesCreated(optionalUnsigned(stats, "indices_created"))
                .indicesDeleted(optionalUnsigned(stats, "indices_deleted"))
                .cachedExecution(optional(stats, "cached_execution", Boolean.class))
                .executionTimeMs(optional(stats, "execution_time_ms", Double.class))
                .graphVersion(optionalUnsigned(stats, "graph_version"))
                .build();
    }

    private static BigInteger optionalUnsigned(Map<?, ?> stats, String key) {
        if (!stats.containsKey(key)) {
            return null;
        }
        Object value = stats.get(key);
        BigInteger number =
                value instanceof Long ? BigInteger.valueOf((Long) value) : typed(value, BigInteger.class);
        if (number.signum() < 0 || number.bitLength() > 64) {
            throw invalid(key + " must be an unsigned 64-bit integer");
        }
        return number;
    }

    private static Map<?, ?> fields(Object value) {
        return typed(value, Map.class);
    }

    private static List<?> sequence(Object value) {
        if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        }
        if (value instanceof List<?>) {
            return (List<?>) value;
        }
        throw invalid("expected array");
    }

    private static List<String> strings(Object value) {
        List<String> strings = new ArrayList<>();
        for (Object item : sequence(value)) {
            strings.add(typed(item, String.class));
        }
        return strings;
    }

    private static Object required(Map<?, ?> map, String key) {
        if (!map.containsKey(key)) {
            throw invalid("missing " + key);
        }
        return map.get(key);
    }

    private static <T> T optional(Map<?, ?> map, String key, Class<T> type) {
        return map.containsKey(key) ? typed(map.get(key), type) : null;
    }

    private static <T> T typed(Object value, Class<T> type) {
        if (!type.isInstance(value)) {
            throw invalid("expected " + type.getSimpleName());
        }
        return type.cast(value);
    }

    private static RequestException invalid(String message) {
        return new RequestException("Invalid canonical graph result: " + message);
    }
}
