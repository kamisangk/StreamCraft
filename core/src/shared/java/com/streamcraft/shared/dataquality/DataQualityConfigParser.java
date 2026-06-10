package com.streamcraft.shared.dataquality;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.dataquality.DataQualityConfig.Mode;
import com.streamcraft.shared.dataquality.DataQualityConfig.Rule;
import com.streamcraft.shared.dataquality.DataQualityConfig.RuleType;
import com.streamcraft.shared.dataquality.DataQualityConfig.ValueType;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.regex.Pattern;

public final class DataQualityConfigParser {

    private DataQualityConfigParser() {
    }

    public static DataQualityConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static DataQualityConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        Mode mode = parseEnum(text(safeConfig, "mode", "DIRTY_PORT"), Mode.class, "mode", error);
        String errorField = text(safeConfig, "errorField", DataQualityConfig.DEFAULT_ERROR_FIELD);
        List<Rule> rules = rules(safeConfig, error);
        if (rules.isEmpty()) {
            throw error.apply(ValidationError.rulesRequired());
        }
        return new DataQualityConfig(rules, mode, errorField);
    }

    private static List<Rule> rules(JsonNode config, ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path("rules");
        if (value == null || !value.isArray()) {
            return List.of();
        }

        List<Rule> result = new ArrayList<>();
        for (JsonNode item : value) {
            String field = text(item, "field", "");
            if (field.isBlank()) {
                throw error.apply(ValidationError.ruleFieldRequired());
            }

            RuleType ruleType = parseEnum(text(item, "ruleType", null), RuleType.class, "ruleType", error);
            ValueType valueType = parseEnum(text(item, "valueType", null), ValueType.class, "valueType", error, true);
            Double min = numberValue(item, "min", error);
            Double max = numberValue(item, "max", error);
            Integer minLength = integerValue(item, "minLength", error);
            Integer maxLength = integerValue(item, "maxLength", error);
            List<String> enumValues = textArray(item, "enumValues");
            String pattern = text(item, "pattern", "");
            if (!pattern.isBlank()) {
                try {
                    Pattern.compile(pattern);
                } catch (Exception exception) {
                    throw error.apply(ValidationError.invalidPattern(field));
                }
            }

            validateRuleParameters(field, ruleType, valueType, min, max, minLength, maxLength, enumValues, pattern, error);
            result.add(new Rule(
                    field,
                    ruleType,
                    valueType,
                    min,
                    max,
                    minLength,
                    maxLength,
                    enumValues,
                    pattern,
                    text(item, "customMessage", "")));
        }
        return result;
    }

    private static void validateRuleParameters(
            String field,
            RuleType ruleType,
            ValueType valueType,
            Double min,
            Double max,
            Integer minLength,
            Integer maxLength,
            List<String> enumValues,
            String pattern,
            ValidationErrorFactory error) {
        switch (ruleType) {
            case TYPE -> {
                if (valueType == null) {
                    throw error.apply(ValidationError.requiredParameter(field, "valueType"));
                }
            }
            case RANGE -> {
                if (min == null && max == null) {
                    throw error.apply(ValidationError.requiredParameter(field, "min or max"));
                }
                if (min != null && max != null && min > max) {
                    throw error.apply(ValidationError.invalidRange(field));
                }
            }
            case LENGTH -> {
                if (minLength == null && maxLength == null) {
                    throw error.apply(ValidationError.requiredParameter(field, "minLength or maxLength"));
                }
                if (minLength != null && minLength < 0 || maxLength != null && maxLength < 0) {
                    throw error.apply(ValidationError.nonNegativeLength(field));
                }
                if (minLength != null && maxLength != null && minLength > maxLength) {
                    throw error.apply(ValidationError.invalidLengthRange(field));
                }
            }
            case ENUM -> {
                if (enumValues.isEmpty()) {
                    throw error.apply(ValidationError.requiredParameter(field, "enumValues"));
                }
            }
            case REGEX -> {
                if (pattern.isBlank()) {
                    throw error.apply(ValidationError.requiredParameter(field, "pattern"));
                }
            }
            case NOT_NULL -> {
            }
        }
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

    private static Double numberValue(JsonNode config, String fieldName, ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || value.isMissingNode() || value.isNull()) {
            return null;
        }
        if (value.isNumber()) {
            return value.asDouble();
        }
        if (value.isTextual()) {
            try {
                return Double.parseDouble(value.asText().trim());
            } catch (Exception exception) {
                throw error.apply(ValidationError.numberRequired(fieldName));
            }
        }
        throw error.apply(ValidationError.numberRequired(fieldName));
    }

    private static Integer integerValue(JsonNode config, String fieldName, ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || value.isMissingNode() || value.isNull()) {
            return null;
        }
        if (value.isIntegralNumber()) {
            return value.asInt();
        }
        if (value.isTextual()) {
            try {
                return Integer.parseInt(value.asText().trim());
            } catch (Exception exception) {
                throw error.apply(ValidationError.numberRequired(fieldName));
            }
        }
        throw error.apply(ValidationError.numberRequired(fieldName));
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
        return parseEnum(value, enumType, fieldName, error, false);
    }

    private static <T extends Enum<T>> T parseEnum(
            String value,
            Class<T> enumType,
            String fieldName,
            ValidationErrorFactory error,
            boolean nullable) {
        if (nullable && (value == null || value.isBlank())) {
            return null;
        }
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

        static ValidationError rulesRequired() {
            return new ValidationError(
                    "pipeline.validation.dataQuality.rulesRequired",
                    "Data quality config rules must contain at least one item.");
        }

        static ValidationError ruleFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.dataQuality.ruleFieldRequired",
                    "Data quality config rule field is required.");
        }

        static ValidationError numberRequired(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.dataQuality.numberRequired",
                    "Data quality config " + fieldName + " must be a valid number.",
                    fieldName);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.dataQuality.unsupportedValue",
                    "Data quality config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }

        static ValidationError invalidPattern(String field) {
            return new ValidationError(
                    "pipeline.validation.dataQuality.invalidPattern",
                    "Data quality config rule pattern is invalid for field: " + field,
                    field);
        }

        static ValidationError requiredParameter(String field, String parameter) {
            return new ValidationError(
                    "pipeline.validation.dataQuality.requiredParameter",
                    "Data quality config rule for field " + field + " requires " + parameter + ".",
                    field,
                    parameter);
        }

        static ValidationError invalidRange(String field) {
            return new ValidationError(
                    "pipeline.validation.dataQuality.invalidRange",
                    "Data quality config range rule min must be less than or equal to max for field: " + field,
                    field);
        }

        static ValidationError nonNegativeLength(String field) {
            return new ValidationError(
                    "pipeline.validation.dataQuality.nonNegativeLength",
                    "Data quality config length rule bounds must be greater than or equal to 0 for field: " + field,
                    field);
        }

        static ValidationError invalidLengthRange(String field) {
            return new ValidationError(
                    "pipeline.validation.dataQuality.invalidLengthRange",
                    "Data quality config length rule minLength must be less than or equal to maxLength for field: " + field,
                    field);
        }
    }
}
