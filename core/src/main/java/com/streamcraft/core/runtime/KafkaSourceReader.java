package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class KafkaSourceReader implements SourceReader<DataEntity, KafkaPartitionSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceReader.class);

    private final SourceReaderContext context;
    private final KafkaSourceConfig config;
    private final KafkaSourceFetcherClient fetcher;
    private final Object lock = new Object();
    private final Map<String, KafkaPartitionSplit> activeSplits = new HashMap<>();
    private final Map<TopicPartition, Long> currentOffsets = new HashMap<>();
    private final NavigableMap<Long, Map<TopicPartition, OffsetAndMetadata>> checkpointOffsets = new TreeMap<>();
    private final TreeSet<Long> completedCheckpoints = new TreeSet<>();
    private final TreeSet<Long> abortedCheckpoints = new TreeSet<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private long inFlightCheckpointId = -1L;

    KafkaSourceReader(SourceReaderContext context, KafkaSourceConfig config) {
        this(context, config, new KafkaSourceFetcher(config));
    }

    KafkaSourceReader(SourceReaderContext context, KafkaSourceConfig config, KafkaSourceFetcherClient fetcher) {
        this.context = context;
        this.config = config;
        this.fetcher = fetcher;
    }

    @Override
    public void start() {
        fetcher.start();
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<DataEntity> output) {
        fetcher.throwIfFailed();
        drainCommitResults();
        dispatchLatestCompletedCheckpoint();
        while (true) {
            KafkaSourceFetcher.FetchedRecord record = fetcher.pollRecord();
            if (record == null) {
                return InputStatus.NOTHING_AVAILABLE;
            }

            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            if (isStaleRecord(partition, record.offset())) {
                continue;
            }

            Map<String, Object> fields;
            try {
                fields = KafkaSourceFactory.decodeFields(config.format(), record.value(), objectMapper);
            } catch (Exception exception) {
                synchronized (lock) {
                    Long expectedOffset = currentOffsets.get(partition);
                    if (expectedOffset == null || record.offset() < expectedOffset) {
                        continue;
                    }
                    LOG.warn(
                            "Skipping Kafka Source record from topic '{}' partition {} offset {} because {}",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            exception.getMessage());
                    currentOffsets.merge(partition, record.offset() + 1L, Math::max);
                }
                return InputStatus.MORE_AVAILABLE;
            }

            Map<String, String> headers = new HashMap<>();
            headers.put("topic", record.topic());
            headers.put("partition", String.valueOf(record.partition()));
            headers.put("offset", String.valueOf(record.offset()));
            synchronized (lock) {
                Long expectedOffset = currentOffsets.get(partition);
                if (expectedOffset == null || record.offset() < expectedOffset) {
                    continue;
                }
                output.collect(new DataEntity(
                        record.key() == null ? UUID.randomUUID().toString() : record.key(),
                        record.timestamp(),
                        fields,
                        headers));
                currentOffsets.merge(partition, record.offset() + 1L, Math::max);
            }
            return InputStatus.MORE_AVAILABLE;
        }
    }

    @Override
    public List<KafkaPartitionSplit> snapshotState(long checkpointId) {
        synchronized (lock) {
            List<KafkaPartitionSplit> snapshot = activeSplits.values().stream()
                    .map(split -> new KafkaPartitionSplit(
                            split.topic(),
                            split.partition(),
                            currentOffsets.getOrDefault(
                                    new TopicPartition(split.topic(), split.partition()), split.nextOffset())))
                    .sorted(Comparator.comparing(KafkaPartitionSplit::topic)
                            .thenComparingInt(KafkaPartitionSplit::partition))
                    .toList();
            if (config.commitOnCheckpoint()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                for (KafkaPartitionSplit split : snapshot) {
                    offsets.put(new TopicPartition(split.topic(), split.partition()), new OffsetAndMetadata(split.nextOffset()));
                }
                checkpointOffsets.put(checkpointId, Map.copyOf(offsets));
            }
            return snapshot;
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return fetcher.availability();
    }

    @Override
    public void addSplits(List<KafkaPartitionSplit> splits) {
        List<KafkaPartitionSplit> effectiveSplits = new ArrayList<>(splits.size());
        synchronized (lock) {
            for (KafkaPartitionSplit split : splits) {
                activeSplits.merge(split.splitId(), split,
                        (left, right) -> left.nextOffset() >= right.nextOffset() ? left : right);
                TopicPartition partition = new TopicPartition(split.topic(), split.partition());
                currentOffsets.merge(partition, split.nextOffset(), Math::max);
                effectiveSplits.add(new KafkaPartitionSplit(
                        split.topic(), split.partition(), currentOffsets.get(partition)));
            }
        }
        fetcher.addSplits(effectiveSplits);
    }

    @Override
    public void notifyNoMoreSplits() {
        // Kafka Source is unbounded: this is only relevant if Flink stops assigning splits.
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (!config.commitOnCheckpoint()) {
            return;
        }
        synchronized (lock) {
            completedCheckpoints.add(checkpointId);
        }
        fetcher.wakeupAndSignalAvailability();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        boolean cancelCommit;
        synchronized (lock) {
            cancelCommit = checkpointOffsets.remove(checkpointId) != null
                    || inFlightCheckpointId == checkpointId;
            completedCheckpoints.remove(checkpointId);
            if (cancelCommit) {
                abortedCheckpoints.add(checkpointId);
            }
        }
        if (cancelCommit) {
            fetcher.abortCheckpointCommit(checkpointId);
        }
    }

    @Override
    public void close() {
        fetcher.close();
    }

    private void drainCommitResults() {
        KafkaSourceFetcher.CommitResult result;
        while ((result = fetcher.pollCommitResult()) != null) {
            synchronized (lock) {
                if (inFlightCheckpointId != result.checkpointId()) {
                    continue;
                }
                boolean aborted = abortedCheckpoints.remove(result.checkpointId());
                inFlightCheckpointId = -1L;
                if (result.succeeded()) {
                    checkpointOffsets.headMap(result.checkpointId(), true).clear();
                    completedCheckpoints.headSet(result.checkpointId(), true).clear();
                } else if (!aborted && checkpointOffsets.containsKey(result.checkpointId())) {
                    LOG.warn("Failed to commit Kafka Source offsets for checkpoint {}; retrying", result.checkpointId());
                    completedCheckpoints.add(result.checkpointId());
                }
            }
        }
    }

    private void dispatchLatestCompletedCheckpoint() {
        if (!config.commitOnCheckpoint()) {
            return;
        }
        synchronized (lock) {
            if (inFlightCheckpointId >= 0) {
                return;
            }
            Long checkpointId = completedCheckpoints.descendingSet().stream()
                    .filter(checkpointOffsets::containsKey)
                    .findFirst()
                    .orElse(null);
            if (checkpointId == null) {
                return;
            }
            inFlightCheckpointId = checkpointId;
            try {
                fetcher.submitCheckpointCommit(checkpointId, checkpointOffsets.get(checkpointId));
            } catch (RuntimeException exception) {
                inFlightCheckpointId = -1L;
                LOG.warn(
                        "Failed to submit Kafka Source offsets for checkpoint {}; retrying",
                        checkpointId,
                        exception);
                try {
                    fetcher.wakeupAndSignalAvailability();
                } catch (RuntimeException wakeupException) {
                    LOG.warn("Unable to reschedule Kafka Source checkpoint commit retry", wakeupException);
                }
            }
        }
    }

    private boolean isStaleRecord(TopicPartition partition, long offset) {
        synchronized (lock) {
            Long expectedOffset = currentOffsets.get(partition);
            return expectedOffset == null || offset < expectedOffset;
        }
    }
}
