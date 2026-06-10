package com.streamcraft.shared.streamjoin;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.streamjoin.StreamJoinConfig.JoinType;
import com.streamcraft.shared.streamjoin.StreamJoinConfig.MissingStrategy;
import com.streamcraft.shared.streamjoin.StreamJoinConfig.TimeMode;
import com.streamcraft.shared.streamjoin.StreamJoinConfig.TimeUnit;
import java.util.Locale;
import java.util.function.Function;

public final class StreamJoinConfigParser {

    private StreamJoinConfigParser() {
    }

    public static StreamJoinConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static StreamJoinConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        String leftKeyField = text(safeConfig, "leftKeyField", "");
        if (leftKeyField.isBlank()) {
            throw error.apply(ValidationError.leftKeyFieldRequired());
        }
        String rightKeyField = text(safeConfig, "rightKeyField", "");
        if (rightKeyField.isBlank()) {
            throw error.apply(ValidationError.rightKeyFieldRequired());
        }
        String targetField = text(safeConfig, "targetField", "");
        if (targetField.isBlank()) {
            throw error.apply(ValidationError.targetFieldRequired());
        }

        long windowBefore = longValue(safeConfig, "windowBefore", 0L, error);
        if (windowBefore < 0) {
            throw error.apply(ValidationError.windowBeforeNonNegative());
        }
        long windowAfter = longValue(safeConfig, "windowAfter", 0L, error);
        if (windowAfter < 0) {
            throw error.apply(ValidationError.windowAfterNonNegative());
        }
        if (windowBefore == 0L && windowAfter == 0L) {
            throw error.apply(ValidationError.windowRangeRequired());
        }

        long watermarkDelay = longValue(safeConfig, "watermarkDelay", 30L, error);
        if (watermarkDelay < 0) {
            throw error.apply(ValidationError.watermarkDelayNonNegative());
        }

        return new StreamJoinConfig(
                leftKeyField,
                rightKeyField,
                targetField,
                parseEnum(text(safeConfig, "joinType", "LEFT"), JoinType.class, "joinType", error),
                parseEnum(text(safeConfig, "missingStrategy", "KEEP_ORIGINAL"), MissingStrategy.class, "missingStrategy", error),
                booleanValue(safeConfig, "overwriteTargetField", false),
                parseEnum(text(safeConfig, "timeMode", "PROCESSING_TIME"), TimeMode.class, "timeMode", error),
                parseEnum(text(safeConfig, "timeUnit", "SECONDS"), TimeUnit.class, "timeUnit", error),
                windowBefore,
                windowAfter,
                watermarkDelay);
    }

    private static String text(JsonNode config, String fieldName, String fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        String text = value == null || value.isMissingNode() || value.isNull() ? fallback : value.asText(fallback);
        return text == null ? "" : text.trim();
    }

    private static boolean booleanValue(JsonNode config, String fieldName, boolean fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        return value == null || value.isMissingNode() || value.isNull() ? fallback : value.asBoolean(fallback);
    }

    private static long longValue(
            JsonNode config,
            String fieldName,
            long fallback,
            ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || value.isMissingNode() || value.isNull()) {
            return fallback;
        }
        if (value.isTextual()) {
            return parseLongText(value.asText(), fieldName, error);
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong()) {
            throw error.apply(ValidationError.integerRequired(fieldName));
        }
        return value.asLong();
    }

    private static long parseLongText(String value, String fieldName, ValidationErrorFactory error) {
        try {
            return Long.parseLong(value.trim());
        } catch (Exception exception) {
            throw error.apply(ValidationError.integerRequired(fieldName));
        }
    }

    private static <T extends Enum<T>> T parseEnum(
            String value,
            Class<T> enumType,
            String fieldName,
            ValidationErrorFactory error) {
        try {
            return Enum.valueOf(enumType, value.trim().toUpperCase(Locale.ROOT));
        } catch (Exception exception) {
            throw error.apply(ValidationError.unsupportedValue(fieldName, value));
        }
    }

    @FunctionalInterface
    public interface ValidationErrorFactory {
        IllegalArgumentException apply(ValidationError error);
    }

    public record ValidationError(String messageKey, String defaultMessage, Object... args) {

        static ValidationError leftKeyFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.streamJoin.leftKeyFieldRequired",
                    "Stream join config leftKeyField is required.");
        }

        static ValidationError rightKeyFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.streamJoin.rightKeyFieldRequired",
                    "Stream join config rightKeyField is required.");
        }

        static ValidationError targetFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.streamJoin.targetFieldRequired",
                    "Stream join config targetField is required.");
        }

        static ValidationError windowBeforeNonNegative() {
            return new ValidationError(
                    "pipeline.validation.streamJoin.windowBeforeNonNegative",
                    "Stream join config windowBefore must be greater than or equal to 0.");
        }

        static ValidationError windowAfterNonNegative() {
            return new ValidationError(
                    "pipeline.validation.streamJoin.windowAfterNonNegative",
                    "Stream join config windowAfter must be greater than or equal to 0.");
        }

        static ValidationError windowRangeRequired() {
            return new ValidationError(
                    "pipeline.validation.streamJoin.windowRangeRequired",
                    "Stream join config windowBefore and windowAfter cannot both be 0.");
        }

        static ValidationError watermarkDelayNonNegative() {
            return new ValidationError(
                    "pipeline.validation.streamJoin.watermarkDelayNonNegative",
                    "Stream join config watermarkDelay must be greater than or equal to 0.");
        }

        static ValidationError integerRequired(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.streamJoin.integerRequired",
                    "Stream join config " + fieldName + " must be a valid integer.",
                    fieldName);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.streamJoin.unsupportedValue",
                    "Stream join config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
