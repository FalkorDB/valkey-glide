/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import lombok.Value;

/**
 * A typed graph value. Scalars retain their exact Java representation: Long, Double, Boolean,
 * String or null. Lists contain GraphValue; maps use GraphMap to preserve order and duplicate keys.
 * Entities, paths, points, vectors and temporal values have dedicated immutable model classes.
 */
@Value
public class GraphValue {
    /** The complete set of values supported by the pinned FalkorDB compact protocol. */
    public enum Kind {
        NULL,
        STRING,
        INTEGER,
        BOOLEAN,
        DOUBLE,
        LIST,
        MAP,
        NODE,
        RELATIONSHIP,
        PATH,
        POINT,
        VECTOR,
        DATETIME,
        DATE,
        TIME,
        DURATION
    }

    Kind kind;
    Object value;
}
