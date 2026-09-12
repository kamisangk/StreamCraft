package com.streamcraft.core.runtime;

import java.io.Serializable;
import java.util.List;

/** Operator state containing only prepared, not-yet-committed transactions. */
record HdfsFileSinkState(List<HdfsFileSinkCommittable> pendingTransactions) implements Serializable {

    private static final long serialVersionUID = 1L;

    HdfsFileSinkState {
        pendingTransactions = pendingTransactions == null ? List.of() : List.copyOf(pendingTransactions);
    }

    HdfsFileSinkState without(HdfsFileSinkTransaction transaction) {
        return new HdfsFileSinkState(pendingTransactions.stream()
                .filter(committable -> !committable.transaction().equals(transaction))
                .toList());
    }
}
