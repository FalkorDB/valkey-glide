/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.graph;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/**
 * Optional known statistics and the complete unmodified raw server lines. Absent statistics are
 * null, never fabricated as zero. Counters and graph version preserve the full unsigned 64-bit
 * range as BigInteger. Unknown server lines remain available through raw.
 */
@Value
public class GraphStatistics {
    List<String> raw;
    BigInteger labelsAdded;
    BigInteger labelsRemoved;
    BigInteger nodesCreated;
    BigInteger nodesDeleted;
    BigInteger propertiesSet;
    BigInteger propertiesRemoved;
    BigInteger relationshipsCreated;
    BigInteger relationshipsDeleted;
    BigInteger indicesCreated;
    BigInteger indicesDeleted;
    Boolean cachedExecution;
    Double executionTimeMs;
    BigInteger graphVersion;

    @Builder
    public GraphStatistics(
            List<String> raw,
            BigInteger labelsAdded,
            BigInteger labelsRemoved,
            BigInteger nodesCreated,
            BigInteger nodesDeleted,
            BigInteger propertiesSet,
            BigInteger propertiesRemoved,
            BigInteger relationshipsCreated,
            BigInteger relationshipsDeleted,
            BigInteger indicesCreated,
            BigInteger indicesDeleted,
            Boolean cachedExecution,
            Double executionTimeMs,
            BigInteger graphVersion) {
        this.raw = Collections.unmodifiableList(new ArrayList<>(raw));
        this.labelsAdded = labelsAdded;
        this.labelsRemoved = labelsRemoved;
        this.nodesCreated = nodesCreated;
        this.nodesDeleted = nodesDeleted;
        this.propertiesSet = propertiesSet;
        this.propertiesRemoved = propertiesRemoved;
        this.relationshipsCreated = relationshipsCreated;
        this.relationshipsDeleted = relationshipsDeleted;
        this.indicesCreated = indicesCreated;
        this.indicesDeleted = indicesDeleted;
        this.cachedExecution = cachedExecution;
        this.executionTimeMs = executionTimeMs;
        this.graphVersion = graphVersion;
    }
}
