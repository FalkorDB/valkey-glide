/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import graph_request.GraphRequest.Parameter;
import java.math.BigInteger;
import java.util.AbstractSequentialList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GraphQueryOptionsTest {
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 4096})
    void sequential_lists_are_encoded_in_one_pass_without_indexed_access(int size) {
        List<Object> expected = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            expected.add((long) i);
        }
        SequentialValues values = new SequentialValues(expected);
        GraphQueryOptions actual =
                GraphQueryOptions.builder().params(Collections.singletonMap("items", values)).build();
        assertEquals(
                GraphQueryOptions.builder()
                        .params(Collections.singletonMap("items", expected))
                        .build()
                        .toProtobuf(),
                actual.toProtobuf());
        assertEquals(1, values.iteratorStarts);
        assertEquals(size, values.nextCalls);
    }

    @Test
    void sequential_lists_preserve_mixed_nested_values_and_snapshot_output() {
        List<Object> nested = Arrays.asList(null, true, "λ🌐", Long.MIN_VALUE, Long.MAX_VALUE, 1.5);
        List<Object> expected = Arrays.asList(7L, nested, Collections.singletonMap("field", nested));
        SequentialValues values =
                new SequentialValues(
                        Arrays.asList(
                                7L,
                                new SequentialValues(nested),
                                Collections.singletonMap("field", new SequentialValues(nested))));
        GraphQueryOptions actual =
                GraphQueryOptions.builder().params(Collections.singletonMap("items", values)).build();
        assertEquals(
                GraphQueryOptions.builder()
                        .params(Collections.singletonMap("items", expected))
                        .build()
                        .toProtobuf(),
                actual.toProtobuf());
        values.clear();
        assertEquals(3, actual.toProtobuf().getEntries(0).getValue().getListValue().getValuesCount());
    }

    @Test
    void sequential_list_errors_keep_indexed_paths_and_cycle_detection() {
        List<Object> invalid = Arrays.asList("ok", Arrays.asList(1L, 2L, new Date(0)));
        IllegalArgumentException expected =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                GraphQueryOptions.builder()
                                        .params(Collections.singletonMap("items", invalid))
                                        .build());
        IllegalArgumentException actual =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                GraphQueryOptions.builder()
                                        .params(
                                                Collections.singletonMap(
                                                        "items",
                                                        new SequentialValues(
                                                                Arrays.asList(
                                                                        "ok", new SequentialValues((List<?>) invalid.get(1))))))
                                        .build());
        assertEquals(expected.getMessage(), actual.getMessage());
        assertEquals("params.items[1][2]: unsupported type java.util.Date", actual.getMessage());
        SequentialValues cycle = new SequentialValues(Collections.emptyList());
        cycle.add(cycle);
        assertEquals(
                "params.items[0]: cyclic parameter",
                assertThrows(
                                IllegalArgumentException.class,
                                () ->
                                        GraphQueryOptions.builder()
                                                .params(Collections.singletonMap("items", cycle))
                                                .build())
                        .getMessage());
    }

    private static final class SequentialValues extends AbstractSequentialList<Object> {
        private final LinkedList<Object> values;
        private int iteratorStarts;
        private int nextCalls;

        private SequentialValues(List<?> values) {
            this.values = new LinkedList<>(values);
        }

        @Override
        public Object get(int index) {
            throw new AssertionError("Indexed sequential-list access at " + index);
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public ListIterator<Object> listIterator(int index) {
            assertEquals(0, index, "Encoding must start one forward traversal");
            iteratorStarts++;
            ListIterator<Object> iterator = values.listIterator(index);
            return new ListIterator<Object>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Object next() {
                    nextCalls++;
                    return iterator.next();
                }

                @Override
                public boolean hasPrevious() {
                    return iterator.hasPrevious();
                }

                @Override
                public Object previous() {
                    return iterator.previous();
                }

                @Override
                public int nextIndex() {
                    return iterator.nextIndex();
                }

                @Override
                public int previousIndex() {
                    return iterator.previousIndex();
                }

                @Override
                public void remove() {
                    iterator.remove();
                }

                @Override
                public void set(Object value) {
                    iterator.set(value);
                }

                @Override
                public void add(Object value) {
                    iterator.add(value);
                }
            };
        }
    }

    @Test
    void typed_parameters_are_exact_and_snapshotted() {
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("min", Long.MIN_VALUE);
        parameters.put("max", BigInteger.valueOf(Long.MAX_VALUE));
        parameters.put(
                "🌐", Arrays.asList(null, true, 1.5, "λ\uD83D\uDE00", Collections.singletonMap("x y", 2)));
        GraphQueryOptions options =
                GraphQueryOptions.builder().params(parameters).timeoutMs(0L).build();
        parameters.clear();
        assertEquals(Long.MIN_VALUE, options.toProtobuf().getEntries(0).getValue().getIntegerValue());
        assertEquals(Long.MAX_VALUE, options.toProtobuf().getEntries(1).getValue().getIntegerValue());
        Parameter list = options.toProtobuf().getEntries(2).getValue();
        assertEquals(Parameter.ValueCase.NULL_VALUE, list.getListValue().getValues(0).getValueCase());
        assertEquals(true, list.getListValue().getValues(1).getBooleanValue());
        assertEquals(1.5, list.getListValue().getValues(2).getDoubleValue());
        assertEquals("λ\uD83D\uDE00", list.getListValue().getValues(3).getStringValue());
        assertEquals("x y", list.getListValue().getValues(4).getMapValue().getEntries(0).getKey());
        assertEquals(Long.valueOf(0), options.getTimeoutMs());
        assertEquals(
                Long.valueOf(Long.MAX_VALUE),
                GraphQueryOptions.builder().timeoutMs(Long.MAX_VALUE).build().getTimeoutMs());
        assertThrows(
                IllegalArgumentException.class, () -> GraphQueryOptions.builder().timeoutMs(-1L).build());
    }

    @Test
    void rejects_overflow_nonfinite_arbitrary_types_and_invalid_unicode_with_path() {
        List<Object> bad =
                Arrays.asList(
                        BigInteger.ONE.shiftLeft(63),
                        BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE),
                        Double.NaN,
                        Double.POSITIVE_INFINITY,
                        new Date(),
                        new Object(),
                        new GraphDate(1),
                        "\uD800",
                        "\uDC00",
                        new int[] {1});
        for (Object value : bad) {
            IllegalArgumentException error =
                    assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    GraphQueryOptions.builder()
                                            .params(Collections.singletonMap("nested", Arrays.asList(value)))
                                            .build());
            assertTrue(error.getMessage().contains("params.nested[0]"), error.getMessage());
        }
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        GraphQueryOptions.builder()
                                .params(Collections.singletonMap("x", Collections.singletonMap(1, "bad")))
                                .build());
    }

    @Test
    void rejects_invalid_identifiers_at_every_map_depth_with_path() {
        List<String> invalidKeys =
                Arrays.asList(
                        "",
                        "back`tick",
                        "line\nbreak",
                        "\u0000",
                        "\u001f",
                        "\u007f",
                        "\u0085",
                        "\u009f",
                        "\uD800",
                        "\uDC00",
                        repeat("a", 513),
                        repeat("🌐", 129));
        for (String key : invalidKeys) {
            IllegalArgumentException root =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> GraphQueryOptions.builder().params(Collections.singletonMap(key, 1)).build());
            assertTrue(root.getMessage().startsWith("params key:"), root.getMessage());
            IllegalArgumentException nested =
                    assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    GraphQueryOptions.builder()
                                            .params(
                                                    Collections.singletonMap(
                                                            "nested", Arrays.asList(Collections.singletonMap(key, 1))))
                                            .build());
            assertTrue(nested.getMessage().startsWith("params.nested[0] key:"), nested.getMessage());
        }
    }

    @Test
    void accepts_unicode_identifiers_through_exact_utf8_byte_limit() {
        for (String key :
                Arrays.asList(
                        "space key",
                        "λ🌐",
                        repeat("a", 511),
                        repeat("a", 512),
                        repeat("🌐", 127) + "abc",
                        repeat("🌐", 128))) {
            GraphQueryOptions options =
                    GraphQueryOptions.builder()
                            .params(Collections.singletonMap(key, Collections.singletonMap(key, 1)))
                            .build();
            assertEquals(key, options.toProtobuf().getEntries(0).getKey());
            assertEquals(
                    key, options.toProtobuf().getEntries(0).getValue().getMapValue().getEntries(0).getKey());
        }
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        GraphQueryOptions.builder()
                                .params(Collections.singletonMap(repeat("🌐", 128) + "a", 1))
                                .build());
    }

    private static String repeat(String value, int count) {
        return String.join("", Collections.nCopies(count, value));
    }

    @Test
    void rejects_cycles_and_deep_nesting_but_accepts_shared_acyclic_values() {
        List<Object> cycle = new ArrayList<>();
        cycle.add(cycle);
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                GraphQueryOptions.builder()
                                        .params(Collections.singletonMap("cycle", cycle))
                                        .build());
        assertTrue(error.getMessage().contains("params.cycle[0]"));
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("self", map);
        assertThrows(
                IllegalArgumentException.class, () -> GraphQueryOptions.builder().params(map).build());
        List<Object> deep = new ArrayList<>();
        List<Object> current = deep;
        for (int i = 0; i < 70; i++) {
            List<Object> next = new ArrayList<>();
            current.add(next);
            current = next;
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> GraphQueryOptions.builder().params(Collections.singletonMap("deep", deep)).build());
        List<Integer> shared = Arrays.asList(1, 2);
        assertEquals(
                2,
                GraphQueryOptions.builder()
                        .params(Collections.singletonMap("ok", Arrays.asList(shared, shared)))
                        .build()
                        .toProtobuf()
                        .getEntries(0)
                        .getValue()
                        .getListValue()
                        .getValuesCount());
    }
}
