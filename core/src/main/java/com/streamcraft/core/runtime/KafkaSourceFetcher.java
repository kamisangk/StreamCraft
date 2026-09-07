package com.streamcraft.core.runtime;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns every KafkaConsumer operation on one dedicated worker thread. */
interface KafkaSourceFetcherClient extends AutoCloseable {

    void start();

    void addSplits(List<KafkaPartitionSplit> splits);

    KafkaSourceFetcher.FetchedRecord pollRecord();

    CompletableFuture<Void> availability();

    void submitCheckpointCommit(long checkpointId, Map<TopicPartition, OffsetAndMetadata> offsets);

    void submitPollCommit(Map<TopicPartition, OffsetAndMetadata> offsets);

    default void abortCheckpointCommit(long checkpointId) {
        // Implementations that queue Kafka operations may cancel a queued request.
    }

    KafkaSourceFetcher.CommitResult pollCommitResult();

    void throwIfFailed();

    void wakeupAndSignalAvailability();

    @Override
    void close();
}

final class KafkaSourceFetcher implements KafkaSourceFetcherClient {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceFetcher.class);

    record FetchedRecord(String key, long timestamp, String topic, int partition, long offset, String value) {}

    record CommitResult(long checkpointId, boolean succeeded) {}

    private record CommitRequest(long checkpointId, Map<TopicPartition, OffsetAndMetadata> offsets) {}

    private final KafkaSourceConfig config;
    private final ConcurrentLinkedQueue<KafkaPartitionSplit> splitAssignments = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<FetchedRecord> records = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<TopicPartition, Long> recordFloorOffsets = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<CommitRequest> commitRequests = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<CommitResult> commitResults = new ConcurrentLinkedQueue<>();
    private final Set<Long> abortedCheckpointIds = ConcurrentHashMap.newKeySet();
    private final AtomicReference<CompletableFuture<Void>> availability = new AtomicReference<>(new CompletableFuture<>());
    private final Object availabilityLock = new Object();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile KafkaConsumer<String, String> consumer;
    private volatile RuntimeException failure;
    private Thread worker;

    KafkaSourceFetcher(KafkaSourceConfig config) {
        this.config = config;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        worker = new Thread(this::run, "streamcraft-kafka-source-fetcher");
        worker.setDaemon(true);
        worker.start();
    }

    @Override
    public void addSplits(List<KafkaPartitionSplit> splits) {
        for (KafkaPartitionSplit split : splits) {
            recordFloorOffsets.merge(
                    new TopicPartition(split.topic(), split.partition()), split.nextOffset(), Math::max);
        }
        splitAssignments.addAll(splits);
        wakeup();
        signalAvailable();
    }

    @Override
    public FetchedRecord pollRecord() {
        FetchedRecord record;
        synchronized (availabilityLock) {
            while ((record = records.poll()) != null) {
                TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                Long floor = recordFloorOffsets.get(partition);
                if (floor == null || record.offset() >= floor) {
                    break;
                }
            }
            if (records.isEmpty() && splitAssignments.isEmpty() && commitResults.isEmpty()) {
                availability.updateAndGet(future -> future.isDone() ? new CompletableFuture<>() : future);
            }
        }
        return record;
    }

    @Override
    public CompletableFuture<Void> availability() {
        synchronized (availabilityLock) {
            return availability.get();
        }
    }

    @Override
    public void submitCheckpointCommit(long checkpointId, Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitRequests.add(new CommitRequest(checkpointId, Map.copyOf(offsets)));
        wakeup();
    }

    @Override
    public void submitPollCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitRequests.add(new CommitRequest(-1L, Map.copyOf(offsets)));
        wakeup();
    }

    @Override
    public void abortCheckpointCommit(long checkpointId) {
        abortedCheckpointIds.add(checkpointId);
        wakeup();
    }

    @Override
    public CommitResult pollCommitResult() {
        return commitResults.poll();
    }

    @Override
    public void throwIfFailed() {
        if (failure != null) {
            throw failure;
        }
    }

    void wakeup() {
        KafkaConsumer<String, String> activeConsumer = consumer;
        if (activeConsumer != null) {
            activeConsumer.wakeup();
        }
    }

    @Override
    public void wakeupAndSignalAvailability() {
        wakeup();
        signalAvailable();
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        wakeup();
        if (worker != null) {
            try {
                worker.join(Duration.ofSeconds(5).toMillis());
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void run() {
        KafkaConsumer<String, String> createdConsumer = null;
        try {
            createdConsumer = new KafkaConsumer<>(consumerProperties());
            consumer = createdConsumer;
            Map<TopicPartition, Long> assignedOffsets = new HashMap<>();
            Map<TopicPartition, Long> appliedOffsets = new HashMap<>();
            while (running.get()) {
                try {
                    applySplitAssignments(createdConsumer, assignedOffsets, appliedOffsets);
                    drainCommitRequests(createdConsumer);
                    ConsumerRecords<String, String> polled = createdConsumer.poll(Duration.ofMillis(250));
                    for (ConsumerRecord<String, String> record : polled) {
                        records.add(new FetchedRecord(
                                record.key(),
                                record.timestamp(),
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.value()));
                    }
                    if (!polled.isEmpty()) {
                        signalAvailable();
                    }
                } catch (WakeupException ignored) {
                    // Wakeups are control flow for assignment, commit, and shutdown requests.
                    // A close wakeup is observed on the next loop condition.
                }
            }
        } catch (RuntimeException exception) {
            if (running.get()) {
                failure = exception;
                signalAvailable();
            }
        } finally {
            if (createdConsumer != null) {
                try {
                    createdConsumer.close(Duration.ofSeconds(5));
                } catch (RuntimeException exception) {
                    LOG.warn("Failed to close Kafka Source consumer: {}", exception.getMessage());
                }
            }
            consumer = null;
        }
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.autoOffsetReset());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaAuthProperties.apply(config, properties);
        return properties;
    }

    private void applySplitAssignments(
            KafkaConsumer<String, String> activeConsumer,
            Map<TopicPartition, Long> assignedOffsets,
            Map<TopicPartition, Long> appliedOffsets) {
        KafkaPartitionSplit split;
        while ((split = splitAssignments.poll()) != null) {
            TopicPartition topicPartition = new TopicPartition(split.topic(), split.partition());
            Long previous = assignedOffsets.get(topicPartition);
            if (previous == null || split.nextOffset() > previous) {
                assignedOffsets.put(topicPartition, split.nextOffset());
            }
        }
        if (assignedOffsets.isEmpty()) {
            return;
        }
        if (!activeConsumer.assignment().equals(assignedOffsets.keySet())) {
            activeConsumer.assign(assignedOffsets.keySet());
        }
        for (Map.Entry<TopicPartition, Long> assignment : assignedOffsets.entrySet()) {
            Long applied = appliedOffsets.get(assignment.getKey());
            if (applied == null || assignment.getValue() > applied) {
                activeConsumer.seek(assignment.getKey(), assignment.getValue());
                appliedOffsets.put(assignment.getKey(), assignment.getValue());
            }
        }
    }

    private void drainCommitRequests(KafkaConsumer<String, String> activeConsumer) {
        CommitRequest request;
        while ((request = commitRequests.poll()) != null) {
            if (request.checkpointId() < 0) {
                try {
                    activeConsumer.commitAsync(request.offsets(), (ignored, exception) -> {
                        if (exception != null) {
                            LOG.warn("Failed to commit Kafka Source poll offsets: {}", exception.getMessage());
                        }
                    });
                } catch (RuntimeException exception) {
                    LOG.warn("Failed to submit Kafka Source poll offsets: {}", exception.getMessage());
                }
                continue;
            }
            long checkpointId = request.checkpointId();
            if (abortedCheckpointIds.remove(checkpointId)) {
                commitResults.add(new CommitResult(checkpointId, false));
                signalAvailable();
                continue;
            }
            try {
                activeConsumer.commitAsync(
                        request.offsets(),
                        (ignored, exception) -> {
                            abortedCheckpointIds.remove(checkpointId);
                            commitResults.add(new CommitResult(checkpointId, exception == null));
                            signalAvailable();
                        });
            } catch (RuntimeException exception) {
                LOG.warn(
                        "Failed to submit Kafka Source offsets for checkpoint {}; retrying",
                        checkpointId,
                        exception);
                commitResults.add(new CommitResult(checkpointId, false));
                signalAvailable();
            }
        }
    }

    private void signalAvailable() {
        synchronized (availabilityLock) {
            availability.get().complete(null);
        }
    }
}
