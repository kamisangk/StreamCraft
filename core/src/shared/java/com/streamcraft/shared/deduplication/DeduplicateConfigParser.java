package com.streamcraft.shared.deduplication;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.deduplication.DeduplicateConfig.DuplicateStrategy;
import com.streamcraft.shared.deduplication.DeduplicateConfig.KeepStrategy;
import com.streamcraft.shared.deduplication.DeduplicateConfig.LateDataStrategy;
import com.streamcraft.shared.deduplication.DeduplicateConfig.TimeMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

public final class DeduplicateConfigParser {

    private DeduplicateConfigParser() {
    }

    public static DeduplicateConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static DeduplicateConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        List<String> keyFields = textArray(safeConfig, "keyFields");
        TimeMode timeMode = parseEnum(text(safeConfig, "timeMode", "PROCESSING_TIME"), TimeMode.class, "timeMode", error);
        KeepStrategy keepStrategy = parseEnum(
                text(safeConfig, "keepStrategy", timeMode == TimeMode.EVENT_TIME ? "EVENT_TIME_LATEST" : "FIRST"),
                KeepStrategy.class,
                "keepStrategy",
                error);
        DuplicateStrategy duplicateStrategy = parseEnum(
                text(safeConfig, "duplicateStrategy", "DISCARD"),
                DuplicateStrategy.class,
                "duplicateStrategy",
                error);

        if (keyFields.isEmpty()) {
            throw error.apply(ValidationError.keyFieldsRequired());
        }
        Long ttlSeconds = optionalLong(safeConfig, "ttlSeconds", error);
        Long windowSeconds = optionalLong(safeConfig, "windowSeconds", error);
        Long watermarkDelaySeconds = optionalLong(safeConfig, "watermarkDelaySeconds", error);
        String eventTimeField = text(safeConfig, "eventTimeField", "");
        LateDataStrategy lateDataStrategy = parseEnum(
                text(safeConfig, "lateDataStrategy", "DISCARD"),
                LateDataStrategy.class,
                "lateDataStrategy",
                error);

        if (timeMode == TimeMode.PROCESSING_TIME) {
            if (ttlSeconds == null || ttlSeconds <= 0) {
                throw error.apply(ValidationError.ttlSecondsPositive());
            }
            if (keepStrategy == KeepStrategy.EVENT_TIME_LATEST) {
                throw error.apply(ValidationError.unsupportedValue("keepStrategy", keepStrategy.name()));
            }
            return new DeduplicateConfig(
                    keyFields,
                    timeMode,
                    ttlSeconds,
                    keepStrategy,
                    duplicateStrategy,
                    eventTimeField,
                    windowSeconds,
                    watermarkDelaySeconds,
                    lateDataStrategy);
        }

        if (eventTimeField.isBlank()) {
            throw error.apply(ValidationError.eventTimeFieldRequired());
        }
        if (windowSeconds == null || windowSeconds <= 0) {
            throw error.apply(ValidationError.windowSecondsPositive());
        }
        if (watermarkDelaySeconds == null || watermarkDelaySeconds < 0) {
            throw error.apply(ValidationError.watermarkDelaySecondsNonNegative());
        }
        return new DeduplicateConfig(
                keyFields,
                timeMode,
                ttlSeconds == null ? 0 : ttlSeconds,
                keepStrategy,
                duplicateStrategy,
                eventTimeField,
                windowSeconds,
                watermarkDelaySeconds,
                lateDataStrategy);
    }

    private static List<String> textArray(JsonNode config, String fieldName) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || !value.isArray()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (JsonNode item : value) {
            String text = item.asText("").trim();
            if (!text.isBlank()) {
                result.add(text);
            }
        }
        return result;
    }

    private static long longValue(JsonNode config, String fieldName, ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || value.isMissingNode() || value.isNull()) {
            throw error.apply(ValidationError.integerRequired(fieldName));
        }
        if (value.isTextual()) {
            return parseLongText(value.asText(), fieldName, error);
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong()) {
            throw error.apply(ValidationError.integerRequired(fieldName));
        }
        return value.asLong();
    }

    private static Long optionalLong(JsonNode config, String fieldName, ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || value.isMissingNode() || value.isNull() || value.asText("").isBlank()) {
            return null;
        }
        return longValue(config, fieldName, error);
    }

    private static long parseLongText(String value, String fieldName, ValidationErrorFactory error) {
        try {
            return Long.parseLong(value.trim());
        } catch (Exception exception) {
            throw error.apply(ValidationError.integerRequired(fieldName));
        }
    }

    private static String text(JsonNode config, String fieldName, String fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        String text = value == null || value.isMissingNode() || value.isNull() ? fallback : value.asText(fallback);
        return text == null ? "" : text.trim();
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

        static ValidationError keyFieldsRequired() {
            return new ValidationError(
                    "pipeline.validation.deduplicate.keyFieldsRequired",
                    "Deduplicate config keyFields must contain at least one field.");
        }

        static ValidationError ttlSecondsPositive() {
            return new ValidationError(
                    "pipeline.validation.deduplicate.ttlSecondsPositive",
                    "Deduplicate config ttlSeconds must be greater than 0.");
        }

        static ValidationError eventTimeFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.deduplicate.eventTimeFieldRequired",
                    "Deduplicate config eventTimeField is required when timeMode is EVENT_TIME.");
        }

        static ValidationError windowSecondsPositive() {
            return new ValidationError(
                    "pipeline.validation.deduplicate.windowSecondsPositive",
                    "Deduplicate config windowSeconds must be greater than 0.");
        }

        static ValidationError watermarkDelaySecondsNonNegative() {
            return new ValidationError(
                    "pipeline.validation.deduplicate.watermarkDelaySecondsNonNegative",
                    "Deduplicate config watermarkDelaySeconds must be greater than or equal to 0.");
        }

        static ValidationError integerRequired(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.deduplicate.integerRequired",
                    "Deduplicate config " + fieldName + " must be a valid integer.",
                    fieldName);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.deduplicate.unsupportedValue",
                    "Deduplicate config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
