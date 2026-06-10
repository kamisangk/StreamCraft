package com.streamcraft.shared.eval;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.eval.EvalConfig.ErrorStrategy;
import com.streamcraft.shared.eval.EvalConfig.OutputMode;
import com.streamcraft.shared.expression.SafeExpressionSupport;
import java.util.Locale;
import java.util.function.Function;

public final class EvalConfigParser {

    private EvalConfigParser() {
    }

    public static EvalConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static EvalConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        String targetField = text(safeConfig, "targetField", "");
        String expression = text(safeConfig, "expression", "");

        if (targetField.isBlank()) {
            throw error.apply(ValidationError.targetFieldRequired());
        }
        if (expression.isBlank()) {
            throw error.apply(ValidationError.expressionRequired());
        }

        SafeExpressionSupport.validate(expression, "Eval expression");
        return new EvalConfig(
                targetField,
                expression,
                parseEnum(text(safeConfig, "outputMode", "OVERWRITE"), OutputMode.class, "outputMode", error),
                parseEnum(text(safeConfig, "errorStrategy", "KEEP_ORIGINAL"),
                        ErrorStrategy.class,
                        "errorStrategy",
                        error));
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

        static ValidationError targetFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.eval.targetFieldRequired",
                    "Eval config targetField is required.");
        }

        static ValidationError expressionRequired() {
            return new ValidationError(
                    "pipeline.validation.eval.expressionRequired",
                    "Eval config expression is required.");
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.eval.unsupportedValue",
                    "Eval config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
