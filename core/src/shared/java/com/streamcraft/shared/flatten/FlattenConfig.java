package com.streamcraft.shared.flatten;

import java.io.Serializable;

public record FlattenConfig(
        String sourceField,
        String targetPrefix,
        String delimiter,
        boolean removeSourceField) implements Serializable {

    public FlattenConfig {
        sourceField = sourceField == null ? "" : sourceField.trim();
        targetPrefix = targetPrefix == null ? "" : targetPrefix.trim();
        delimiter = delimiter == null ? "_" : delimiter.trim();
    }
}
