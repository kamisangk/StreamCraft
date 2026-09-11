package com.streamcraft.core.runtime;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.api.connector.source.SourceSplit;

record InfluxDbSourceSplit(String splitId, IncrementalSourceState state, long pageIndex)
        implements SourceSplit, Serializable {
    private static final long serialVersionUID = 1L;
    InfluxDbSourceSplit {
        splitId = Objects.requireNonNull(splitId, "splitId");
        state = Objects.requireNonNull(state, "state");
        if (splitId.isBlank() || pageIndex < 0) throw new IllegalArgumentException("Invalid InfluxDB source split.");
    }
}
