package com.streamcraft.core.runtime;

import java.io.Serializable;
import java.util.List;

record ElasticsearchSourceEnumeratorState(List<ElasticsearchSourceSplit> pendingSplits) implements Serializable {

    private static final long serialVersionUID = 1L;

    ElasticsearchSourceEnumeratorState {
        pendingSplits = pendingSplits == null ? List.of() : List.copyOf(pendingSplits);
    }

    ElasticsearchSourceEnumeratorState(ElasticsearchSourceSplit pendingSplit) {
        this(pendingSplit == null ? List.of() : List.of(pendingSplit));
    }

    ElasticsearchSourceSplit pendingSplit() {
        return pendingSplits.isEmpty() ? null : pendingSplits.get(0);
    }
}
