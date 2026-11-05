/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import lombok.Value;

/** A directed relationship with operation-resolved type and property names. */
@Value
public class GraphRelationship {
    long id;
    String type;
    long sourceId;
    long destinationId;
    GraphMap properties;
}
