package com.streamcraft.core.runtime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

final class JdbcSourceEnumerator implements SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> {

    private final SplitEnumeratorContext<JdbcSourceSplit> context;
    private JdbcSourceSplit pendingSplit;
    private final TreeSet<Integer> waitingReaders = new TreeSet<>();

    JdbcSourceEnumerator(SplitEnumeratorContext<JdbcSourceSplit> context, JdbcSourceEnumeratorState state) {
        this.context = context;
        this.pendingSplit = state == null ? null : state.pendingSplit();
    }

    @Override
    public void start() {
        assignPendingSplit();
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        waitingReaders.add(subtaskId);
        assignPendingSplit();
    }

    @Override
    public void addReader(int subtaskId) {
        waitingReaders.add(subtaskId);
        assignPendingSplit();
    }

    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit = splits.get(0);
        }
        waitingReaders.add(subtaskId);
        assignPendingSplit();
    }

    @Override
    public JdbcSourceEnumeratorState snapshotState(long checkpointId) {
        return new JdbcSourceEnumeratorState(pendingSplit);
    }

    @Override
    public void close() throws IOException {
        // No external resources are owned by the enumerator.
    }

    private void assignPendingSplit() {
        if (pendingSplit == null) {
            return;
        }
        Map<Integer, ?> readers = context.registeredReaders();
        Integer reader = waitingReaders.stream().filter(readers::containsKey).findFirst().orElse(null);
        if (reader == null && !readers.isEmpty()) {
            reader = readers.keySet().stream().sorted().findFirst().orElse(null);
        }
        if (reader == null) {
            return;
        }
        context.assignSplits(new SplitsAssignment<>(Map.of(reader, List.of(pendingSplit))));
        pendingSplit = null;
        waitingReaders.remove(reader);
    }
}
