/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import lombok.Value;

/** Lossless FalkorDB date seconds, retained without a potentially narrowing date conversion. */
@Value
public class GraphDate {
    long rawSeconds;
}
