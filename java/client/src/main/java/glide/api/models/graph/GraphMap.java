/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Value;

/** An ordered graph map, preserving duplicate keys rather than silently dropping values. */
@Value
public class GraphMap {
    List<Entry> entries;

    public GraphMap(List<Entry> entries) {
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
    }

    /** One named value in a graph map or an entity's properties. */
    @Value
    public static class Entry {
        String key;
        GraphValue value;
    }
}
