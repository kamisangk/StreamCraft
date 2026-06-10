package com.streamcraft.shared.timederive;

import java.io.Serializable;
import java.util.List;

public record TimeDeriveConfig(
        String sourceField,
        SourceFormat sourceFormat,
        String sourcePattern,
        String sourceTimeZone,
        String outputTimeZone,
        ParseErrorStrategy parseErrorStrategy,
        List<Derivation> derivations) implements Serializable {

    public TimeDeriveConfig {
        sourceField = sourceField == null ? "" : sourceField.trim();
        sourceFormat = sourceFormat == null ? SourceFormat.AUTO : sourceFormat;
        sourcePattern = sourcePattern == null ? "" : sourcePattern.trim();
        sourceTimeZone = sourceTimeZone == null || sourceTimeZone.isBlank() ? "UTC" : sourceTimeZone.trim();
        outputTimeZone = outputTimeZone == null || outputTimeZone.isBlank() ? "UTC" : outputTimeZone.trim();
        parseErrorStrategy = parseErrorStrategy == null ? ParseErrorStrategy.KEEP_ORIGINAL : parseErrorStrategy;
        derivations = derivations == null ? List.of() : List.copyOf(derivations);
    }

    public enum SourceFormat {
        AUTO,
        ISO,
        EPOCH_MILLIS,
        EPOCH_SECONDS,
        PATTERN
    }

    public enum DerivationType {
        DATE,
        DATETIME,
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND,
        WEEK,
        QUARTER,
        DAY_OF_WEEK,
        DAY_OF_YEAR,
        EPOCH_MILLIS,
        EPOCH_SECONDS,
        FORMAT
    }

    public enum ParseErrorStrategy {
        KEEP_ORIGINAL,
        SET_NULL,
        FAIL
    }

    public record Derivation(
            String outputField,
            DerivationType type,
            String pattern) implements Serializable {

        public Derivation {
            outputField = outputField == null ? "" : outputField.trim();
            type = type == null ? DerivationType.DATE : type;
            pattern = pattern == null ? "" : pattern.trim();
        }
    }
}
