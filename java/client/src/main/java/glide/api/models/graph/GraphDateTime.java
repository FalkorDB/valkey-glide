/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import lombok.Value;

/** Lossless FalkorDB datetime seconds. No JVM timezone or date conversion is applied. */
@Value
public class GraphDateTime {
    long rawSeconds;
}
