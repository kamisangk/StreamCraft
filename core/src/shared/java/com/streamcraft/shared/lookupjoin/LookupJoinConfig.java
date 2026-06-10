package com.streamcraft.shared.lookupjoin;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public record LookupJoinConfig(
        String sourceField,
        String targetField,
        JoinType joinType,
        MissingStrategy missingStrategy,
        boolean overwriteTargetField,
        List<LookupJoinEntry> entries) implements Serializable {

    public LookupJoinConfig {
        sourceField = sourceField == null ? "" : sourceField.trim();
        targetField = targetField == null ? "" : targetField.trim();
        joinType = joinType == null ? JoinType.LEFT : joinType;
        missingStrategy = missingStrategy == null ? MissingStrategy.KEEP_ORIGINAL : missingStrategy;
        entries = entries == null ? List.of() : List.copyOf(entries);
    }

    public enum JoinType {
        LEFT,
        INNER
    }

    public enum MissingStrategy {
        KEEP_ORIGINAL,
        PUT_NULL
    }

    public record LookupJoinEntry(String key, Map<String, Object> fields) implements Serializable {

        public LookupJoinEntry {
            key = key == null ? "" : key.trim();
            fields = fields == null ? Map.of() : Map.copyOf(fields);
        }
    }
}
