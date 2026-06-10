package com.streamcraft.shared.explode;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.function.Function;

public final class ExplodeConfigParser {

    private ExplodeConfigParser() {
    }

    public static ExplodeConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static ExplodeConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        String sourceField = text(safeConfig, "sourceField", "");
        if (sourceField.isBlank()) {
            throw error.apply(ValidationError.sourceFieldRequired());
        }

        String targetField = text(safeConfig, "targetField", "");
        if (targetField.isBlank()) {
            throw error.apply(ValidationError.targetFieldRequired());
        }

        return new ExplodeConfig(sourceField, targetField, booleanValue(safeConfig, "keepEmpty", false));
    }

    private static String text(JsonNode config, String fieldName, String fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        String text = value == null || value.isMissingNode() || value.isNull() ? fallback : value.asText(fallback);
        return text == null ? "" : text.trim();
    }

    private static boolean booleanValue(JsonNode config, String fieldName, boolean fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || value.isMissingNode() || value.isNull()) {
            return fallback;
        }
        return value.asBoolean(fallback);
    }

    @FunctionalInterface
    public interface ValidationErrorFactory {
        IllegalArgumentException apply(ValidationError error);
    }

    public record ValidationError(String messageKey, String defaultMessage, Object... args) {

        static ValidationError sourceFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.explode.sourceFieldRequired",
                    "Explode config sourceField is required.");
        }

        static ValidationError targetFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.explode.targetFieldRequired",
                    "Explode config targetField is required.");
        }
    }
}
