/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Value;

/** A materialized result preserving column order, duplicate column names and all typed cells. */
@Value
public class GraphQueryResult {
    List<String> columns;
    List<List<GraphValue>> rows;
    GraphStatistics statistics;

    public GraphQueryResult(
            List<String> columns, List<List<GraphValue>> rows, GraphStatistics statistics) {
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        List<List<GraphValue>> copy = new ArrayList<>();
        for (List<GraphValue> row : rows) {
            copy.add(Collections.unmodifiableList(new ArrayList<>(row)));
        }
        this.rows = Collections.unmodifiableList(copy);
        this.statistics = statistics;
    }
}
