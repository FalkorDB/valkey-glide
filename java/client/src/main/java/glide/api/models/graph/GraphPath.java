/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Value;

/** A path in traversal order. Relationship direction remains in the relationship's endpoint IDs. */
@Value
public class GraphPath {
    List<GraphNode> nodes;
    List<GraphRelationship> relationships;

    public GraphPath(List<GraphNode> nodes, List<GraphRelationship> relationships) {
        this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
        this.relationships = Collections.unmodifiableList(new ArrayList<>(relationships));
    }
}
