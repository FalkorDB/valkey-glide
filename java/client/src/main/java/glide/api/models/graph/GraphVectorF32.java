/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Value;

/** A lossless vector of the server's float32 components. */
@Value
public class GraphVectorF32 {
    List<Float> values;

    public GraphVectorF32(List<Float> values) {
        this.values = Collections.unmodifiableList(new ArrayList<>(values));
    }
}
