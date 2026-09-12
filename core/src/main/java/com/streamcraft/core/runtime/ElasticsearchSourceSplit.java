package com.streamcraft.core.runtime;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.api.connector.source.SourceSplit;

record ElasticsearchSourceSplit(
        String splitId,
        IncrementalSourceState state,
        String scrollId,
        long pageIndex,
        int sliceId,
        int sliceMax) implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    ElasticsearchSourceSplit {
        splitId = Objects.requireNonNull(splitId, "splitId");
        state = Objects.requireNonNull(state, "state");
        scrollId = scrollId == null ? "" : scrollId;
        if (splitId.isBlank() || pageIndex < 0) {
            throw new IllegalArgumentException("Invalid Elasticsearch source split.");
        }
        if (sliceId < 0 || sliceMax < 1 || sliceId >= sliceMax) {
            throw new IllegalArgumentException("Invalid Elasticsearch source slice.");
        }
    }

    ElasticsearchSourceSplit(String splitId, IncrementalSourceState state, String scrollId, long pageIndex) {
        this(splitId, state, scrollId, pageIndex, 0, 1);
    }
}
