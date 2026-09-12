package com.streamcraft.core.runtime;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.api.connector.source.SourceSplit;

/** JDBC work unit. Range bounds are optional for the single-reader fallback. */
record JdbcSourceSplit(
        String splitId,
        IncrementalSourceState state,
        String rangeColumn,
        String lowerBound,
        String upperBound,
        boolean upperInclusive) implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    JdbcSourceSplit {
        splitId = Objects.requireNonNull(splitId, "splitId");
        if (splitId.isBlank()) {
            throw new IllegalArgumentException("JDBC source split id must not be blank.");
        }
        state = Objects.requireNonNull(state, "state");
        rangeColumn = rangeColumn == null ? "" : rangeColumn.trim();
        lowerBound = lowerBound == null ? "" : lowerBound.trim();
        upperBound = upperBound == null ? "" : upperBound.trim();
        if (rangeColumn.isBlank() && (!lowerBound.isBlank() || !upperBound.isBlank())) {
            throw new IllegalArgumentException("JDBC range bounds require a range column.");
        }
        if (!rangeColumn.isBlank() && lowerBound.isBlank() != upperBound.isBlank()) {
            throw new IllegalArgumentException("JDBC range bounds must be provided together.");
        }
    }

    JdbcSourceSplit(String splitId, IncrementalSourceState state) {
        this(splitId, state, "", "", "", false);
    }
}
