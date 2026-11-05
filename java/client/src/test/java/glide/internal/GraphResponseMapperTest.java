/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GraphResponseMapperTest {
    @Test
    void materializes_all_sixteen_kinds_without_compact_parsing() {
        Map<String, Object> node =
                fields(
                        "id",
                        1L,
                        "labels",
                        array("Person", "Other"),
                        "properties",
                        new Object[] {array("name", cell("string", "λ"))});
        Map<String, Object> relationship =
                fields(
                        "id",
                        2L,
                        "type",
                        "KNOWS",
                        "source_id",
                        1L,
                        "destination_id",
                        3L,
                        "properties",
                        array());
        Object[] cells =
                array(
                        cell("null", null),
                        cell("string", "🌐"),
                        cell("integer", Long.MIN_VALUE),
                        cell("boolean", true),
                        cell("double", -0.0),
                        cell("list", array(cell("null", null), cell("integer", Long.MAX_VALUE))),
                        cell(
                                "map",
                                array(
                                        array("duplicate", cell("integer", 1L)),
                                        array("duplicate", cell("integer", 2L)))),
                        cell("node", node),
                        cell("relationship", relationship),
                        cell(
                                "path",
                                fields(
                                        "nodes",
                                        array(cell("node", node)),
                                        "relationships",
                                        array(cell("relationship", relationship)))),
                        cell("point", fields("latitude", 1.5, "longitude", 2.5)),
                        cell("vector", array(1.0, -0.0, (double) 0.1f)),
                        cell("datetime", Long.MIN_VALUE),
                        cell("date", Long.MAX_VALUE),
                        cell("time", 123L),
                        cell("duration", -2678400L));
        String[] columns = new String[cells.length];
        Arrays.fill(columns, "duplicate");
        GraphQueryResult result =
                GraphResponseMapper.map(
                        fields(
                                "columns",
                                columns,
                                "rows",
                                new Object[] {cells},
                                "statistics",
                                fields("raw", array("Future statistic: x"))));
        assertEquals(cells.length, result.getColumns().size());
        List<GraphValue> row = result.getRows().get(0);
        assertNull(row.get(0).getValue());
        assertEquals(Long.MIN_VALUE, row.get(2).getValue());
        assertEquals(
                Double.doubleToRawLongBits(-0.0),
                Double.doubleToRawLongBits((Double) row.get(4).getValue()));
        assertEquals(2, assertInstanceOf(GraphMap.class, row.get(6).getValue()).getEntries().size());
        assertEquals(
                Arrays.asList("Person", "Other"),
                assertInstanceOf(GraphNode.class, row.get(7).getValue()).getLabels());
        assertEquals(
                "KNOWS", assertInstanceOf(GraphRelationship.class, row.get(8).getValue()).getType());
        assertEquals(1, assertInstanceOf(GraphPath.class, row.get(9).getValue()).getNodes().size());
        assertEquals(1.5, assertInstanceOf(GraphPoint.class, row.get(10).getValue()).getLatitude());
        assertEquals(
                Arrays.asList(1f, -0f, 0.1f),
                assertInstanceOf(GraphVectorF32.class, row.get(11).getValue()).getValues());
        assertEquals(
                Long.MIN_VALUE,
                assertInstanceOf(GraphDateTime.class, row.get(12).getValue()).getRawSeconds());
        assertEquals(
                Long.MAX_VALUE, assertInstanceOf(GraphDate.class, row.get(13).getValue()).getRawSeconds());
        assertEquals(123, assertInstanceOf(GraphTime.class, row.get(14).getValue()).getRawSeconds());
        assertEquals(
                -2678400, assertInstanceOf(GraphDuration.class, row.get(15).getValue()).getRawSeconds());
        assertNull(result.getStatistics().getNodesCreated());
        assertEquals(Arrays.asList("Future statistic: x"), result.getStatistics().getRaw());
        assertThrows(UnsupportedOperationException.class, () -> result.getRows().get(0).clear());
        assertThrows(UnsupportedOperationException.class, () -> result.getColumns().clear());
    }

    @Test
    void statistics_preserve_absence_zero_raw_and_unsigned_version() {
        BigInteger version = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
        GraphQueryResult result =
                GraphResponseMapper.map(
                        fields(
                                "columns",
                                array(),
                                "rows",
                                array(),
                                "statistics",
                                fields(
                                        "raw",
                                        array("Nodes created: 0", "unknown"),
                                        "nodes_created",
                                        0L,
                                        "cached_execution",
                                        false,
                                        "execution_time_ms",
                                        0.0,
                                        "graph_version",
                                        version)));
        assertEquals(BigInteger.ZERO, result.getStatistics().getNodesCreated());
        assertEquals(Boolean.FALSE, result.getStatistics().getCachedExecution());
        assertEquals(version, result.getStatistics().getGraphVersion());
        assertNull(result.getStatistics().getNodesDeleted());
    }

    private static final List<String> UNSIGNED_STATISTICS =
            Arrays.asList(
                    "labels_added",
                    "labels_removed",
                    "nodes_created",
                    "nodes_deleted",
                    "properties_set",
                    "properties_removed",
                    "relationships_created",
                    "relationships_deleted",
                    "indices_created",
                    "indices_deleted",
                    "graph_version");

    @Test
    void all_known_counters_preserve_full_unsigned_64_bit_range() {
        for (Object value :
                array(
                        0L,
                        Long.MAX_VALUE,
                        BigInteger.ZERO,
                        BigInteger.valueOf(Long.MAX_VALUE),
                        BigInteger.ONE.shiftLeft(63),
                        BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE))) {
            Map<String, Object> stats = fields("raw", array("unknown", "Nodes created: 0"));
            for (String field : UNSIGNED_STATISTICS) {
                stats.put(field, value);
            }
            GraphStatistics mapped =
                    GraphResponseMapper.map(fields("columns", array(), "rows", array(), "statistics", stats))
                            .getStatistics();
            BigInteger expected = new BigInteger(value.toString());
            assertEquals(expected, mapped.getLabelsAdded());
            assertEquals(expected, mapped.getLabelsRemoved());
            assertEquals(expected, mapped.getNodesCreated());
            assertEquals(expected, mapped.getNodesDeleted());
            assertEquals(expected, mapped.getPropertiesSet());
            assertEquals(expected, mapped.getPropertiesRemoved());
            assertEquals(expected, mapped.getRelationshipsCreated());
            assertEquals(expected, mapped.getRelationshipsDeleted());
            assertEquals(expected, mapped.getIndicesCreated());
            assertEquals(expected, mapped.getIndicesDeleted());
            assertEquals(expected, mapped.getGraphVersion());
            assertEquals(Arrays.asList("unknown", "Nodes created: 0"), mapped.getRaw());
        }
    }

    @Test
    void rejects_malformed_known_unsigned_statistics() {
        for (String field : UNSIGNED_STATISTICS) {
            for (Object invalid :
                    array(-1L, BigInteger.valueOf(-1), BigInteger.ONE.shiftLeft(64), 1.0, "1", null)) {
                Map<String, Object> stats = fields("raw", array(), field, invalid);
                assertThrows(
                        RequestException.class,
                        () ->
                                GraphResponseMapper.map(
                                        fields("columns", array(), "rows", array(), "statistics", stats)),
                        field + "=" + invalid);
            }
        }
    }

    @Test
    void rejects_raw_compact_unknown_kinds_and_malformed_canonical_values() {
        assertThrows(
                RequestException.class,
                () -> GraphResponseMapper.map(array(array(1L, "x"), array(), array())));
        for (Object bad :
                array(
                        cell("future", 1L),
                        cell("integer", 1.0),
                        cell("null", "wrong"),
                        cell("map", new Object[] {array("x")}),
                        cell("vector", array(0.1)))) {
            assertThrows(
                    RequestException.class,
                    () ->
                            GraphResponseMapper.map(
                                    fields(
                                            "columns",
                                            array("x"),
                                            "rows",
                                            new Object[] {array(bad)},
                                            "statistics",
                                            fields("raw", array()))));
        }
        assertThrows(
                RequestException.class,
                () ->
                        GraphResponseMapper.map(
                                fields(
                                        "columns",
                                        array("x"),
                                        "rows",
                                        new Object[] {array()},
                                        "statistics",
                                        fields("raw", array()))));
    }

    private static Object[] array(Object... values) {
        return values;
    }

    private static Map<String, Object> cell(String kind, Object value) {
        return fields("kind", kind, "value", value);
    }

    private static Map<String, Object> fields(Object... values) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put((String) values[i], values[i + 1]);
        }
        return map;
    }
}
