package com.streamcraft.core.runtime;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/** Enumerator checkpoint state. Active reader offsets belong to reader snapshots. */
record KafkaEnumeratorState(List<KafkaPartitionSplit> pendingSplits, Set<String> knownSplitIds)
        implements Serializable {

    private static final long serialVersionUID = 1L;

    KafkaEnumeratorState {
        pendingSplits = List.copyOf(pendingSplits);
        knownSplitIds = Set.copyOf(knownSplitIds);
    }
}
