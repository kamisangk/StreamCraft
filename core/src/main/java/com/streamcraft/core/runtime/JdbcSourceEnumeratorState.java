package com.streamcraft.core.runtime;

import java.io.Serializable;
import java.util.List;

record JdbcSourceEnumeratorState(List<JdbcSourceSplit> pendingSplits) implements Serializable {

    private static final long serialVersionUID = 1L;

    JdbcSourceEnumeratorState {
        pendingSplits = pendingSplits == null ? List.of() : List.copyOf(pendingSplits);
    }

    JdbcSourceEnumeratorState(JdbcSourceSplit pendingSplit) {
        this(pendingSplit == null ? List.of() : List.of(pendingSplit));
    }

    /** Compatibility accessor retained for callers that only support one pending split. */
    JdbcSourceSplit pendingSplit() {
        return pendingSplits.isEmpty() ? null : pendingSplits.get(0);
    }
}
