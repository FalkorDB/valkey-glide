/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import lombok.Value;

/**
 * Lossless FalkorDB duration encoding. Calendar durations use server-specific offset seconds and
 * must not be interpreted as java.time.Duration.
 */
@Value
public class GraphDuration {
    long rawSeconds;
}
