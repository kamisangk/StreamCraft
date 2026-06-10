package com.streamcraft.shared.streamjoin;

import java.io.Serializable;

public record StreamJoinConfig(
        String leftKeyField,
        String rightKeyField,
        String targetField,
        JoinType joinType,
        MissingStrategy missingStrategy,
        boolean overwriteTargetField,
        TimeMode timeMode,
        TimeUnit timeUnit,
        long windowBefore,
        long windowAfter,
        long watermarkDelay) implements Serializable {

    public StreamJoinConfig {
        leftKeyField = leftKeyField == null ? "" : leftKeyField.trim();
        rightKeyField = rightKeyField == null ? "" : rightKeyField.trim();
        targetField = targetField == null ? "" : targetField.trim();
        joinType = joinType == null ? JoinType.LEFT : joinType;
        missingStrategy = missingStrategy == null ? MissingStrategy.KEEP_ORIGINAL : missingStrategy;
        timeMode = timeMode == null ? TimeMode.PROCESSING_TIME : timeMode;
        timeUnit = timeUnit == null ? TimeUnit.SECONDS : timeUnit;
    }

    public enum JoinType {
        LEFT,
        INNER
    }

    public enum MissingStrategy {
        KEEP_ORIGINAL,
        PUT_NULL
    }

    public enum TimeMode {
        PROCESSING_TIME,
        EVENT_TIME
    }

    public enum TimeUnit {
        MILLISECONDS,
        SECONDS,
        MINUTES,
        HOURS
    }
}
