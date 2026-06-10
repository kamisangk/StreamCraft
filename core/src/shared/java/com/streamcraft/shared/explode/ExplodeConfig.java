package com.streamcraft.shared.explode;

import java.io.Serializable;

public record ExplodeConfig(
        String sourceField,
        String targetField,
        boolean keepEmpty) implements Serializable {

    public ExplodeConfig {
        sourceField = sourceField == null ? "" : sourceField.trim();
        targetField = targetField == null ? "" : targetField.trim();
    }
}
