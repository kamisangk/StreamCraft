package com.streamcraft.core.runtime;

import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

final class ElasticsearchSourceEnumerator
        implements SplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceEnumeratorState> {
    private final SplitEnumeratorContext<ElasticsearchSourceSplit> context;
    private final ArrayDeque<ElasticsearchSourceSplit> pendingSplits = new ArrayDeque<>();
    private final TreeSet<Integer> waitingReaders = new TreeSet<>();
    private final boolean bounded;
    private final TreeSet<Integer> noMoreSignaled = new TreeSet<>();

    ElasticsearchSourceEnumerator(SplitEnumeratorContext<ElasticsearchSourceSplit> context,
            ElasticsearchSourceEnumeratorState state, ElasticsearchSourceConfig config) {
        this.context = context;
        this.bounded = config.readMode() == ElasticsearchSourceConfig.ReadMode.FULL;
        if (state != null) pendingSplits.addAll(state.pendingSplits());
    }

    @Override public void start() { assignPendingSplits(); }
    @Override public void handleSplitRequest(int subtaskId, String requesterHostname) {
        waitingReaders.add(subtaskId); assignPendingSplits();
    }
    @Override public void addReader(int subtaskId) { waitingReaders.add(subtaskId); assignPendingSplits(); }
    @Override public void addSplitsBack(List<ElasticsearchSourceSplit> splits, int subtaskId) {
        if (splits != null) splits.stream().sorted(java.util.Comparator.comparing(ElasticsearchSourceSplit::splitId))
                .forEach(pendingSplits::addFirst);
        waitingReaders.add(subtaskId); assignPendingSplits();
    }
    @Override public ElasticsearchSourceEnumeratorState snapshotState(long checkpointId) {
        return new ElasticsearchSourceEnumeratorState(new ArrayList<>(pendingSplits));
    }
    @Override public void close() throws IOException { }

    private void assignPendingSplits() {
        if (pendingSplits.isEmpty()) { signalNoMoreSplits(); return; }
        Map<Integer, ?> readers = context.registeredReaders();
        List<Integer> targets = waitingReaders.stream().filter(readers::containsKey).toList();
        if (targets.isEmpty()) targets = readers.keySet().stream().sorted().toList();
        if (targets.isEmpty()) return;
        Map<Integer, List<ElasticsearchSourceSplit>> assignment = new java.util.LinkedHashMap<>();
        int index = 0;
        while (!pendingSplits.isEmpty()) {
            ElasticsearchSourceSplit split = pendingSplits.removeFirst();
            int target = targets.get(index++ % targets.size());
            assignment.computeIfAbsent(target, ignored -> new ArrayList<>()).add(split);
        }
        context.assignSplits(new SplitsAssignment<>(assignment));
        waitingReaders.removeAll(assignment.keySet()); signalNoMoreSplits();
    }

    private void signalNoMoreSplits() {
        if (!bounded || !pendingSplits.isEmpty()) return;
        context.registeredReaders().keySet().stream().sorted().filter(noMoreSignaled::add)
                .forEach(context::signalNoMoreSplits);
    }
}
