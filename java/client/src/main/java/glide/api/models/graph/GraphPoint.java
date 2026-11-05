/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import lombok.Value;

/** A geographical point returned by FalkorDB. */
@Value
public class GraphPoint {
    double latitude;
    double longitude;
}
