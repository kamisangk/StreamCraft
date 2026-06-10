package com.streamcraft.shared.timederive;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.timederive.TimeDeriveConfig.Derivation;
import com.streamcraft.shared.timederive.TimeDeriveConfig.DerivationType;
import com.streamcraft.shared.timederive.TimeDeriveConfig.ParseErrorStrategy;
import com.streamcraft.shared.timederive.TimeDeriveConfig.SourceFormat;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

public final class TimeDeriveConfigParser {

    private TimeDeriveConfigParser() {
    }

    public static TimeDeriveConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static TimeDeriveConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        String sourceField = text(safeConfig, "sourceField", "");
        if (sourceField.isBlank()) {
            throw error.apply(ValidationError.sourceFieldRequired());
        }

        SourceFormat sourceFormat = parseEnum(text(safeConfig, "sourceFormat", "AUTO"), SourceFormat.class, "sourceFormat", error);
        String sourcePattern = text(safeConfig, "sourcePattern", "");
        if (sourceFormat == SourceFormat.PATTERN && sourcePattern.isBlank()) {
            throw error.apply(ValidationError.patternRequired());
        }
        validatePattern(sourcePattern, error);

        String sourceTimeZone = text(safeConfig, "sourceTimeZone", "UTC");
        validateZone(sourceTimeZone, error);
        String outputTimeZone = text(safeConfig, "outputTimeZone", "UTC");
        validateZone(outputTimeZone, error);
        ParseErrorStrategy parseErrorStrategy = parseEnum(
                text(safeConfig, "parseErrorStrategy", "KEEP_ORIGINAL"),
                ParseErrorStrategy.class,
                "parseErrorStrategy",
                error);

        List<Derivation> derivations = derivations(safeConfig, error);
        if (derivations.isEmpty()) {
            throw error.apply(ValidationError.derivationsRequired());
        }

        return new TimeDeriveConfig(
                sourceField,
                sourceFormat,
                sourcePattern,
                sourceTimeZone,
                outputTimeZone,
                parseErrorStrategy,
                derivations);
    }

    private static List<Derivation> derivations(JsonNode config, ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path("derivations");
        if (value == null || !value.isArray()) {
            return List.of();
        }

        List<Derivation> result = new ArrayList<>();
        Set<String> outputFields = new HashSet<>();
        for (JsonNode item : value) {
            String outputField = text(item, "outputField", "");
            if (outputField.isBlank()) {
                throw error.apply(ValidationError.outputFieldRequired());
            }
            if (!outputFields.add(outputField)) {
                throw error.apply(ValidationError.outputFieldUnique(outputField));
            }
            DerivationType type = parseEnum(text(item, "type", "DATE"), DerivationType.class, "type", error);
            String pattern = text(item, "pattern", "");
            if (type == DerivationType.FORMAT && pattern.isBlank()) {
                throw error.apply(ValidationError.patternRequired());
            }
            validatePattern(pattern, error);
            result.add(new Derivation(outputField, type, pattern));
        }
        return result;
    }

    private static void validateZone(String zoneId, ValidationErrorFactory error) {
        try {
            ZoneId.of(zoneId);
        } catch (DateTimeException exception) {
            throw error.apply(ValidationError.invalidZoneId(zoneId));
        }
    }

    private static void validatePattern(String pattern, ValidationErrorFactory error) {
        if (pattern == null || pattern.isBlank()) {
            return;
        }
        try {
            DateTimeFormatter.ofPattern(pattern);
        } catch (IllegalArgumentException exception) {
            throw error.apply(ValidationError.invalidPattern(pattern));
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

        static ValidationError sourceFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.timeDerive.sourceFieldRequired",
                    "Time derive config sourceField is required.");
        }

        static ValidationError derivationsRequired() {
            return new ValidationError(
                    "pipeline.validation.timeDerive.derivationsRequired",
                    "Time derive config derivations must contain at least one item.");
        }

        static ValidationError outputFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.timeDerive.outputFieldRequired",
                    "Time derive config derivation outputField is required.");
        }

        static ValidationError outputFieldUnique(String outputField) {
            return new ValidationError(
                    "pipeline.validation.timeDerive.outputFieldUnique",
                    "Time derive config derivation outputField must be unique: " + outputField,
                    outputField);
        }

        static ValidationError patternRequired() {
            return new ValidationError(
                    "pipeline.validation.timeDerive.patternRequired",
                    "Time derive config pattern is required for PATTERN or FORMAT.");
        }

        static ValidationError invalidPattern(String pattern) {
            return new ValidationError(
                    "pipeline.validation.timeDerive.invalidPattern",
                    "Time derive config pattern is invalid: " + pattern,
                    pattern);
        }

        static ValidationError invalidZoneId(String zoneId) {
            return new ValidationError(
                    "pipeline.validation.timeDerive.invalidZoneId",
                    "Time derive config zoneId is invalid: " + zoneId,
                    zoneId);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.timeDerive.unsupportedValue",
                    "Time derive config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
