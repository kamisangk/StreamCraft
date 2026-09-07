package com.streamcraft.core.runtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class KafkaSplitEnumerator implements SplitEnumerator<KafkaPartitionSplit, KafkaEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSplitEnumerator.class);
    private static final long DISCOVERY_INTERVAL_MILLIS = 30_000L;

    private final SplitEnumeratorContext<KafkaPartitionSplit> context;
    private final KafkaSourceConfig config;
    private final KafkaMetadataClient metadataClient;
    private final Map<String, KafkaPartitionSplit> pendingSplits = new LinkedHashMap<>();
    private final Set<String> knownSplitIds = new TreeSet<>();
    private final Set<Integer> waitingReaders = new TreeSet<>();

    KafkaSplitEnumerator(
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            KafkaSourceConfig config,
            KafkaMetadataClient metadataClient,
            KafkaEnumeratorState restoredState) {
        this.context = context;
        this.config = config;
        this.metadataClient = metadataClient;
        knownSplitIds.addAll(restoredState.knownSplitIds());
        restoredState.pendingSplits().forEach(this::addPendingSplit);
    }

    @Override
    public void start() {
        context.callAsync(
                () -> metadataClient.discover(config),
                this::handleDiscovery,
                0L,
                DISCOVERY_INTERVAL_MILLIS);
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        waitingReaders.add(subtaskId);
        assignPendingSplits();
    }

    @Override
    public void addSplitsBack(List<KafkaPartitionSplit> splits, int subtaskId) {
        splits.forEach(this::addPendingSplit);
        waitingReaders.add(subtaskId);
        assignPendingSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        waitingReaders.add(subtaskId);
        assignPendingSplits();
    }

    @Override
    public KafkaEnumeratorState snapshotState(long checkpointId) {
        return new KafkaEnumeratorState(sortedPendingSplits(), knownSplitIds);
    }

    @Override
    public void close() throws IOException {
        metadataClient.close();
    }

    private void handleDiscovery(List<KafkaPartitionSplit> discovered, Throwable error) {
        if (error != null) {
            String message = "Kafka Source partition discovery failed for bootstrapServers='"
                    + config.bootstrapServers()
                    + "', topics="
                    + config.topics()
                    + ": "
                    + String.valueOf(error.getMessage());
            LOG.error(message, error);
            throw new IllegalStateException(message, error);
        }
        for (KafkaPartitionSplit split : discovered) {
            if (knownSplitIds.add(split.splitId())) {
                addPendingSplit(split);
            }
        }
        assignPendingSplits();
    }

    private void addPendingSplit(KafkaPartitionSplit split) {
        knownSplitIds.add(split.splitId());
        pendingSplits.merge(split.splitId(), split, (left, right) ->
                left.nextOffset() >= right.nextOffset() ? left : right);
    }

    private void assignPendingSplits() {
        List<Integer> registeredReaders = context.registeredReaders().keySet().stream().sorted().toList();
        List<Integer> readers = waitingReaders.stream()
                .filter(context.registeredReaders()::containsKey)
                .toList();
        if (readers.isEmpty()) {
            // Some runtimes register readers before issuing their first request.
            // Keep restored/pending work assignable in that startup window.
            readers = registeredReaders;
        }
        if (readers.isEmpty() || pendingSplits.isEmpty()) {
            return;
        }
        Map<Integer, List<KafkaPartitionSplit>> assignments = new LinkedHashMap<>();
        List<KafkaPartitionSplit> splits = sortedPendingSplits();
        for (int index = 0; index < splits.size(); index++) {
            int reader = readers.get(index % readers.size());
            assignments.computeIfAbsent(reader, ignored -> new ArrayList<>()).add(splits.get(index));
        }
        pendingSplits.clear();
        waitingReaders.removeAll(assignments.keySet());
        context.assignSplits(new SplitsAssignment<>(assignments));
    }

    private List<KafkaPartitionSplit> sortedPendingSplits() {
        return pendingSplits.values().stream()
                .sorted(Comparator.comparing(KafkaPartitionSplit::topic)
                        .thenComparingInt(KafkaPartitionSplit::partition))
                .toList();
    }
}
