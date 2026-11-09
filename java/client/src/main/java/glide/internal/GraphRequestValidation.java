/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import java.util.Objects;

/** Internal validation before JNI or protobuf can replace malformed graph strings. */
public final class GraphRequestValidation {
    private GraphRequestValidation() {}

    /** Reject unpaired UTF-16 surrogates without restricting valid Unicode scalar values. */
    public static void validateUnicode(String value, String path) {
        Objects.requireNonNull(value, path);
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (Character.isHighSurrogate(current)) {
                if (i + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(++i))) {
                    throw new IllegalArgumentException(path + ": unpaired Unicode surrogate");
                }
            } else if (Character.isLowSurrogate(current)) {
                throw new IllegalArgumentException(path + ": unpaired Unicode surrogate");
            }
        }
    }
}
