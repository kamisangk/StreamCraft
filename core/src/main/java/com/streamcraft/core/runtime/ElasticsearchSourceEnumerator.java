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
    private final boolean bounded;
    private final TreeSet<Integer> noMoreSignaled = new TreeSet<>();
    private final Map<String, Integer> splitOwners = new java.util.HashMap<>();
    private final TreeSet<Integer> failedReaders = new TreeSet<>();

    ElasticsearchSourceEnumerator(SplitEnumeratorContext<ElasticsearchSourceSplit> context,
            ElasticsearchSourceEnumeratorState state, ElasticsearchSourceConfig config) {
        this.context = context;
        this.bounded = config.readMode() == ElasticsearchSourceConfig.ReadMode.FULL || config.maxPolls() > 0;
        if (state != null) pendingSplits.addAll(state.pendingSplits());
        // Reserve work for readers that have not registered yet; restore recomputes for rescaling.
        int owner = 0;
        for (ElasticsearchSourceSplit split : pendingSplits) {
            splitOwners.put(split.splitId(), owner++ % Math.max(1, context.currentParallelism()));
        }
    }

    @Override public void start() { assignPendingSplits(); }
    @Override public void handleSplitRequest(int subtaskId, String requesterHostname) {
        assignPendingSplits();
    }
    @Override public void addReader(int subtaskId) { failedReaders.remove(subtaskId); noMoreSignaled.remove(subtaskId); assignPendingSplits(); }
    @Override public void addSplitsBack(List<ElasticsearchSourceSplit> splits, int subtaskId) {
        failedReaders.add(subtaskId);
        if (splits != null) splits.stream().sorted(java.util.Comparator.comparing(ElasticsearchSourceSplit::splitId))
                .forEach(split -> { splitOwners.put(split.splitId(), -1); pendingSplits.addLast(split); });
        assignPendingSplits();
    }
    @Override public ElasticsearchSourceEnumeratorState snapshotState(long checkpointId) {
        return new ElasticsearchSourceEnumeratorState(new ArrayList<>(pendingSplits));
    }
    @Override public void close() throws IOException { }

    private void assignPendingSplits() {
        if (pendingSplits.isEmpty()) { signalNoMoreSplits(); return; }
        Map<Integer, ?> readers = context.registeredReaders().entrySet().stream()
                .filter(entry -> !failedReaders.contains(entry.getKey()) && !noMoreSignaled.contains(entry.getKey()))
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (readers.isEmpty()) return;
        Map<Integer, List<ElasticsearchSourceSplit>> assignment = new java.util.LinkedHashMap<>();
        int count = pendingSplits.size();
        for (int index = 0; index < count; index++) {
            ElasticsearchSourceSplit split = pendingSplits.removeFirst();
            int target = splitOwners.getOrDefault(split.splitId(), 0);
            if (target < 0) target = readers.keySet().stream().min(Integer::compareTo).orElseThrow();
            if (readers.containsKey(target)) assignment.computeIfAbsent(target, ignored -> new ArrayList<>()).add(split);
            else pendingSplits.addLast(split);
        }
        if (!assignment.isEmpty()) context.assignSplits(new SplitsAssignment<>(assignment));
        signalNoMoreSplits();
    }

    private void signalNoMoreSplits() {
        if (!bounded || !pendingSplits.isEmpty()) return;
        context.registeredReaders().keySet().stream().sorted()
                .filter(reader -> !failedReaders.contains(reader)).filter(noMoreSignaled::add)
                .forEach(context::signalNoMoreSplits);
    }
}
