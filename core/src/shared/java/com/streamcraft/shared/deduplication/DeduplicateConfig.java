package com.streamcraft.shared.deduplication;

import java.io.Serializable;
import java.util.List;

public record DeduplicateConfig(
        List<String> keyFields,
        TimeMode timeMode,
        long ttlSeconds,
        KeepStrategy keepStrategy,
        DuplicateStrategy duplicateStrategy,
        String eventTimeField,
        Long windowSeconds,
        Long watermarkDelaySeconds,
        LateDataStrategy lateDataStrategy) implements Serializable {

    public DeduplicateConfig {
        keyFields = keyFields == null ? List.of() : List.copyOf(keyFields);
        timeMode = timeMode == null ? TimeMode.PROCESSING_TIME : timeMode;
        keepStrategy = keepStrategy == null ? KeepStrategy.FIRST : keepStrategy;
        duplicateStrategy = duplicateStrategy == null ? DuplicateStrategy.DISCARD : duplicateStrategy;
        eventTimeField = eventTimeField == null ? "" : eventTimeField.trim();
        lateDataStrategy = lateDataStrategy == null ? LateDataStrategy.DISCARD : lateDataStrategy;
    }

    public enum TimeMode {
        PROCESSING_TIME,
        EVENT_TIME
    }

    public enum KeepStrategy {
        FIRST,
        LAST,
        EVENT_TIME_LATEST
    }

    public enum DuplicateStrategy {
        DISCARD
    }

    public enum LateDataStrategy {
        DISCARD
    }
}
