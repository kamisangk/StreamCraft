package com.streamcraft.shared.maskhash;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.maskhash.MaskHashConfig.Action;
import com.streamcraft.shared.maskhash.MaskHashConfig.Algorithm;
import com.streamcraft.shared.maskhash.MaskHashConfig.Rule;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

public final class MaskHashConfigParser {

    private MaskHashConfigParser() {
    }

    public static MaskHashConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static MaskHashConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        List<Rule> rules = rules(safeConfig, error);
        if (rules.isEmpty()) {
            throw error.apply(ValidationError.rulesRequired());
        }
        return new MaskHashConfig(rules);
    }

    private static List<Rule> rules(JsonNode config, ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path("rules");
        if (value == null || !value.isArray()) {
            return List.of();
        }

        List<Rule> result = new ArrayList<>();
        for (JsonNode item : value) {
            String sourceField = text(item, "sourceField", "");
            if (sourceField.isBlank()) {
                throw error.apply(ValidationError.sourceFieldRequired());
            }
            String targetField = text(item, "targetField", "");
            if (targetField.isBlank()) {
                throw error.apply(ValidationError.targetFieldRequired());
            }
            Action action = parseEnum(text(item, "action", "MASK"), Action.class, "action", error);
            Algorithm algorithm = parseEnum(text(item, "algorithm", "SHA256"), Algorithm.class, "algorithm", error);
            int keepFirst = intValue(item, "keepFirst", 3, error);
            int keepLast = intValue(item, "keepLast", 4, error);
            if (keepFirst < 0 || keepLast < 0) {
                throw error.apply(ValidationError.nonNegativeKeep());
            }
            result.add(new Rule(
                    sourceField,
                    targetField,
                    action,
                    algorithm,
                    text(item, "salt", ""),
                    text(item, "maskChar", "*"),
                    keepFirst,
                    keepLast));
        }
        return result;
    }

    private static int intValue(JsonNode config, String fieldName, int fallback, ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || value.isMissingNode() || value.isNull()) {
            return fallback;
        }
        if (value.isTextual()) {
            try {
                return Integer.parseInt(value.asText().trim());
            } catch (Exception exception) {
                throw error.apply(ValidationError.integerRequired(fieldName));
            }
        }
        if (!value.isIntegralNumber() || !value.canConvertToInt()) {
            throw error.apply(ValidationError.integerRequired(fieldName));
        }
        return value.asInt();
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
            return Enum.valueOf(enumType, value.trim().replace("-", "").toUpperCase(Locale.ROOT));
        } catch (Exception exception) {
            throw error.apply(ValidationError.unsupportedValue(fieldName, value));
        }
    }

    @FunctionalInterface
    public interface ValidationErrorFactory {
        IllegalArgumentException apply(ValidationError error);
    }

    public record ValidationError(String messageKey, String defaultMessage, Object... args) {

        static ValidationError rulesRequired() {
            return new ValidationError(
                    "pipeline.validation.maskHash.rulesRequired",
                    "Mask/hash config rules must contain at least one item.");
        }

        static ValidationError sourceFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.maskHash.sourceFieldRequired",
                    "Mask/hash config rule sourceField is required.");
        }

        static ValidationError targetFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.maskHash.targetFieldRequired",
                    "Mask/hash config rule targetField is required.");
        }

        static ValidationError integerRequired(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.maskHash.integerRequired",
                    "Mask/hash config " + fieldName + " must be a valid integer.",
                    fieldName);
        }

        static ValidationError nonNegativeKeep() {
            return new ValidationError(
                    "pipeline.validation.maskHash.nonNegativeKeep",
                    "Mask/hash config keepFirst and keepLast must be greater than or equal to 0.");
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.maskHash.unsupportedValue",
                    "Mask/hash config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
