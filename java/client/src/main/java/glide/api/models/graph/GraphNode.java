/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Value;

/** A graph node with operation-resolved labels and property names. */
@Value
public class GraphNode {
    long id;
    List<String> labels;
    GraphMap properties;

    public GraphNode(long id, List<String> labels, GraphMap properties) {
        this.id = id;
        this.labels = Collections.unmodifiableList(new ArrayList<>(labels));
        this.properties = properties;
    }
}
