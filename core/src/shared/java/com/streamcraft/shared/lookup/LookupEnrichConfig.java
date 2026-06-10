package com.streamcraft.shared.lookup;

import java.util.List;

public record LookupEnrichConfig(
        String sourceField,
        String targetField,
        List<LookupEnrichEntry> entries,
        MissingStrategy missingStrategy,
        boolean overwriteTargetField) {

    public LookupEnrichConfig {
        sourceField = sourceField == null ? "" : sourceField.trim();
        targetField = targetField == null ? "" : targetField.trim();
        entries = entries == null ? List.of() : List.copyOf(entries);
    }

    public enum MissingStrategy {
        KEEP_ORIGINAL,
        PUT_NULL,
        DISCARD,
        FAIL
    }

    public enum ValueType {
        STRING,
        NUMBER,
        BOOLEAN,
        JSON
    }

    public record LookupEnrichEntry(String key, Object value, ValueType valueType) {

        public LookupEnrichEntry {
            key = key == null ? "" : key.trim();
            valueType = valueType == null ? ValueType.STRING : valueType;
        }
    }
}
