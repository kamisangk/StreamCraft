package com.streamcraft.core.runtime;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.api.connector.source.SourceSplit;

/** Single JDBC source split; its progress is the reader checkpoint state. */
record JdbcSourceSplit(String splitId, IncrementalSourceState state) implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    JdbcSourceSplit {
        splitId = Objects.requireNonNull(splitId, "splitId");
        if (splitId.isBlank()) {
            throw new IllegalArgumentException("JDBC source split id must not be blank.");
        }
        state = Objects.requireNonNull(state, "state");
    }
}
