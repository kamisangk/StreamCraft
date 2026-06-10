package com.streamcraft.shared.flatten;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.function.Function;

public final class FlattenConfigParser {

    private FlattenConfigParser() {
    }

    public static FlattenConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static FlattenConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        String sourceField = text(safeConfig, "sourceField", "");
        if (sourceField.isBlank()) {
            throw error.apply(ValidationError.sourceFieldRequired());
        }

        JsonNode delimiterNode = safeConfig == null ? null : safeConfig.get("delimiter");
        String delimiter = text(safeConfig, "delimiter", "_");
        if (delimiterNode != null && !delimiterNode.isMissingNode() && !delimiterNode.isNull() && delimiter.isBlank()) {
            throw error.apply(ValidationError.delimiterRequired());
        }

        return new FlattenConfig(
                sourceField,
                text(safeConfig, "targetPrefix", ""),
                delimiter,
                booleanValue(safeConfig, "removeSourceField", false));
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
                    "pipeline.validation.flatten.sourceFieldRequired",
                    "Flatten config sourceField is required.");
        }

        static ValidationError delimiterRequired() {
            return new ValidationError(
                    "pipeline.validation.flatten.delimiterRequired",
                    "Flatten config delimiter must not be blank.");
        }
    }
}
