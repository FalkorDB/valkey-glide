/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import lombok.Value;

/** Lossless FalkorDB time seconds, retained without timezone assumptions. */
@Value
public class GraphTime {
    long rawSeconds;
}
