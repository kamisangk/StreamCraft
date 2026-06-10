package com.streamcraft.shared.eval;

import java.io.Serializable;

public record EvalConfig(
        String targetField,
        String expression,
        OutputMode outputMode,
        ErrorStrategy errorStrategy) implements Serializable {

    public EvalConfig {
        targetField = targetField == null ? "" : targetField.trim();
        expression = expression == null ? "" : expression.trim();
        outputMode = outputMode == null ? OutputMode.OVERWRITE : outputMode;
        errorStrategy = errorStrategy == null ? ErrorStrategy.KEEP_ORIGINAL : errorStrategy;
    }

    public enum OutputMode {
        OVERWRITE,
        WRITE_IF_ABSENT
    }

    public enum ErrorStrategy {
        KEEP_ORIGINAL,
        PUT_NULL,
        DISCARD,
        FAIL
    }
}
