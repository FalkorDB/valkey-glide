/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import glide.internal.GraphRequestValidation;
import graph_request.GraphRequest.Parameter;
import graph_request.GraphRequest.ParameterEntry;
import graph_request.GraphRequest.ParameterList;
import graph_request.GraphRequest.ParameterMap;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

/**
 * Immutable graph query options. Parameters are snapshotted at construction and encoded as typed
 * protobuf values, preserving signed 64-bit integers exactly. Only null, booleans, integral Java
 * numbers, finite Float/Double, Unicode strings, lists, and string-keyed maps are accepted.
 */
public final class GraphQueryOptions {
    @Getter private final Long timeoutMs;
    private final ParameterMap parameters;

    @Builder
    private GraphQueryOptions(Map<String, ?> params, Long timeoutMs) {
        if (timeoutMs != null && timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs must be in [0, " + Long.MAX_VALUE + "]");
        }
        this.timeoutMs = timeoutMs;
        parameters =
                params == null
                        ? ParameterMap.getDefaultInstance()
                        : encodeMap(params, "params", new IdentityHashMap<>(), 0);
    }

    /** Internal typed transport representation; protobuf messages are immutable. */
    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP",
            justification = "Generated protobuf messages are immutable snapshots, not mutable builders")
    public ParameterMap toProtobuf() {
        return parameters;
    }

    private static Parameter encode(
            Object value, String path, IdentityHashMap<Object, Boolean> active, int depth) {
        Parameter.Builder result = Parameter.newBuilder();
        if (value == null) {
            return result.setNullValue(true).build();
        }
        if (value instanceof Boolean) {
            return result.setBooleanValue((Boolean) value).build();
        }
        if (value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long) {
            return result.setIntegerValue(((Number) value).longValue()).build();
        }
        if (value instanceof BigInteger) {
            try {
                return result.setIntegerValue(((BigInteger) value).longValueExact()).build();
            } catch (ArithmeticException e) {
                throw invalid(path, "integer is outside signed 64-bit range");
            }
        }
        if (value instanceof Float || value instanceof Double) {
            double number = ((Number) value).doubleValue();
            if (!Double.isFinite(number)) {
                throw invalid(path, "number must be finite");
            }
            return result.setDoubleValue(number).build();
        }
        if (value instanceof String) {
            GraphRequestValidation.validateUnicode((String) value, path);
            return result.setStringValue((String) value).build();
        }
        if (value instanceof Map<?, ?>) {
            return result.setMapValue(encodeMap((Map<?, ?>) value, path, active, depth)).build();
        }
        if (value instanceof List<?>) {
            enter(value, path, active, depth);
            try {
                ParameterList.Builder list = ParameterList.newBuilder();
                List<?> values = (List<?>) value;
                for (int i = 0; i < values.size(); i++) {
                    list.addValues(encode(values.get(i), path + "[" + i + "]", active, depth + 1));
                }
                return result.setListValue(list).build();
            } finally {
                active.remove(value);
            }
        }
        throw invalid(path, "unsupported type " + value.getClass().getName());
    }

    private static ParameterMap encodeMap(
            Map<?, ?> values, String path, IdentityHashMap<Object, Boolean> active, int depth) {
        enter(values, path, active, depth);
        try {
            ParameterMap.Builder map = ParameterMap.newBuilder();
            for (Map.Entry<?, ?> entry : values.entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw invalid(path, "map keys must be strings");
                }
                String key = (String) entry.getKey();
                validateIdentifier(key, path + " key");
                map.addEntries(
                        ParameterEntry.newBuilder()
                                .setKey(key)
                                .setValue(encode(entry.getValue(), path + "." + key, active, depth + 1)));
            }
            return map.build();
        } finally {
            active.remove(values);
        }
    }

    private static void enter(
            Object value, String path, IdentityHashMap<Object, Boolean> active, int depth) {
        if (depth > 64) {
            throw invalid(path, "nesting exceeds 64 levels");
        }
        if (active.put(value, Boolean.TRUE) != null) {
            throw invalid(path, "cyclic parameter");
        }
    }

    private static void validateIdentifier(String key, String path) {
        GraphRequestValidation.validateUnicode(key, path);
        if (key.isEmpty()) {
            throw invalid(path, "map keys must be nonempty");
        }
        for (int i = 0; i < key.length(); i++) {
            if (key.charAt(i) == '`' || Character.isISOControl(key.charAt(i))) {
                throw invalid(path, "map keys must not contain backticks or control characters");
            }
        }
        // Match the shared core serializer's FalkorDB identifier limit; Java does not encode Cypher.
        if (key.getBytes(StandardCharsets.UTF_8).length > 512) {
            throw invalid(path, "map keys must not exceed 512 UTF-8 bytes");
        }
    }

    private static IllegalArgumentException invalid(String path, String reason) {
        return new IllegalArgumentException(path + ": " + reason);
    }
}
