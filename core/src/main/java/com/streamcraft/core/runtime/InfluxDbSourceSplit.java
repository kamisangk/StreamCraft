package com.streamcraft.core.runtime;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.api.connector.source.SourceSplit;

record InfluxDbSourceSplit(
        String splitId,
        IncrementalSourceState state,
        long pageIndex,
        String measurement,
        String rangeStart,
        String rangeEnd,
        boolean endInclusive)
        implements SourceSplit, Serializable {
    private static final long serialVersionUID = 1L;
    InfluxDbSourceSplit {
        splitId = Objects.requireNonNull(splitId, "splitId");
        state = Objects.requireNonNull(state, "state");
        if (splitId.isBlank() || pageIndex < 0) throw new IllegalArgumentException("Invalid InfluxDB source split.");
        measurement = measurement == null ? "" : measurement.trim();
        rangeStart = rangeStart == null ? "" : rangeStart.trim();
        rangeEnd = rangeEnd == null ? "" : rangeEnd.trim();
        if (rangeStart.isBlank() != rangeEnd.isBlank()) {
            throw new IllegalArgumentException("InfluxDB range bounds must be provided together.");
        }
    }

    InfluxDbSourceSplit(String splitId, IncrementalSourceState state, long pageIndex) {
        this(splitId, state, pageIndex, "", "", "", false);
    }
}
