package com.streamcraft.core.runtime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

final class ElasticsearchSourceEnumerator
        implements SplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceEnumeratorState> {

    private final SplitEnumeratorContext<ElasticsearchSourceSplit> context;
    private ElasticsearchSourceSplit pendingSplit;
    private final TreeSet<Integer> waitingReaders = new TreeSet<>();

    ElasticsearchSourceEnumerator(
            SplitEnumeratorContext<ElasticsearchSourceSplit> context,
            ElasticsearchSourceEnumeratorState state) {
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
    public void addSplitsBack(List<ElasticsearchSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit = splits.get(0);
        }
        waitingReaders.add(subtaskId);
        assignPendingSplit();
    }

    @Override
    public ElasticsearchSourceEnumeratorState snapshotState(long checkpointId) {
        return new ElasticsearchSourceEnumeratorState(pendingSplit);
    }

    @Override
    public void close() throws IOException {
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
