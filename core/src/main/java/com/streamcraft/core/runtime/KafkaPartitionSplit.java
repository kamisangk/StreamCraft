package com.streamcraft.core.runtime;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.api.connector.source.SourceSplit;

/** Immutable progress state for one Kafka topic partition. */
record KafkaPartitionSplit(String topic, int partition, long nextOffset) implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    KafkaPartitionSplit {
        topic = Objects.requireNonNull(topic, "topic");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("Kafka split topic must not be blank.");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("Kafka split partition must not be negative.");
        }
        if (nextOffset < 0) {
            throw new IllegalArgumentException("Kafka split nextOffset must not be negative.");
        }
    }

    @Override
    public String splitId() {
        return topic + "-" + partition;
    }
}
