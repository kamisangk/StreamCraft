package com.streamcraft.core.runtime;

import com.streamcraft.shared.jdbc.JdbcSourceConfig;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

final class JdbcSourceEnumerator implements SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> {

    private final SplitEnumeratorContext<JdbcSourceSplit> context;
    private final ArrayDeque<JdbcSourceSplit> pendingSplits = new ArrayDeque<>();
    private final TreeSet<Integer> waitingReaders = new TreeSet<>();
    private final boolean bounded;
    private final TreeSet<Integer> noMoreSignaled = new TreeSet<>();

    JdbcSourceEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> context,
            JdbcSourceEnumeratorState state,
            JdbcSourceConfig config) {
        this.context = context;
        this.bounded = config.readMode() == JdbcSourceConfig.ReadMode.FULL;
        if (state != null) {
            pendingSplits.addAll(state.pendingSplits());
        }
    }

    @Override
    public void start() {
        assignPendingSplits();
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        waitingReaders.add(subtaskId);
        assignPendingSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        waitingReaders.add(subtaskId);
        assignPendingSplits();
    }

    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
        if (splits != null) {
            // Preserve deterministic order when a reader returns more than one split.
            splits.stream().sorted(java.util.Comparator.comparing(JdbcSourceSplit::splitId))
                    .forEach(pendingSplits::addFirst);
        }
        waitingReaders.add(subtaskId);
        assignPendingSplits();
    }

    @Override
    public JdbcSourceEnumeratorState snapshotState(long checkpointId) {
        return new JdbcSourceEnumeratorState(new ArrayList<>(pendingSplits));
    }

    @Override
    public void close() throws IOException {
        // No external resources are owned by the enumerator.
    }

    private void assignPendingSplits() {
        if (pendingSplits.isEmpty()) {
            signalNoMoreSplits();
            return;
        }
        Map<Integer, ?> readers = context.registeredReaders();
        List<Integer> targets = waitingReaders.stream().filter(readers::containsKey).toList();
        if (targets.isEmpty()) {
            targets = readers.keySet().stream().sorted().toList();
        }
        if (targets.isEmpty()) {
            return;
        }
        Map<Integer, List<JdbcSourceSplit>> assignment = new java.util.LinkedHashMap<>();
        int index = 0;
        while (!pendingSplits.isEmpty()) {
            JdbcSourceSplit split = pendingSplits.removeFirst();
            int target = targets.get(index++ % targets.size());
            assignment.computeIfAbsent(target, ignored -> new ArrayList<>()).add(split);
        }
        context.assignSplits(new SplitsAssignment<>(assignment));
        waitingReaders.removeAll(assignment.keySet());
        signalNoMoreSplits();
    }

    private void signalNoMoreSplits() {
        if (!bounded || !pendingSplits.isEmpty()) {
            return;
        }
        context.registeredReaders().keySet().stream().sorted()
                .filter(noMoreSignaled::add)
                .forEach(context::signalNoMoreSplits);
    }
}
