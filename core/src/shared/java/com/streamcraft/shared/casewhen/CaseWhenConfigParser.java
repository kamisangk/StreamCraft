package com.streamcraft.shared.casewhen;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.casewhen.CaseWhenConfig.CaseRule;
import com.streamcraft.shared.casewhen.CaseWhenConfig.ValueSpec;
import com.streamcraft.shared.expression.SafeExpressionSupport;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public final class CaseWhenConfigParser {

    private CaseWhenConfigParser() {
    }

    public static CaseWhenConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static CaseWhenConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        String targetField = text(safeConfig, "targetField", "");
        if (targetField.isBlank()) {
            throw error.apply(ValidationError.targetFieldRequired());
        }

        List<CaseRule> cases = cases(safeConfig, error);
        if (cases.isEmpty()) {
            throw error.apply(ValidationError.casesRequired());
        }

        return new CaseWhenConfig(targetField, cases, defaultValueSpec(safeConfig, error));
    }

    private static List<CaseRule> cases(JsonNode config, ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path("cases");
        if (value == null || !value.isArray()) {
            return List.of();
        }
        List<CaseRule> result = new ArrayList<>();
        for (JsonNode item : value) {
            String condition = text(item, "condition", text(item, "when", ""));
            if (condition.isBlank()) {
                throw error.apply(ValidationError.conditionRequired());
            }
            SafeExpressionSupport.validate(condition, "Case when condition");
            ValueSpec caseValue = valueSpec(item, "value", "expression", true, error);
            if (caseValue.emptyValue()) {
                throw error.apply(ValidationError.valueRequired());
            }
            result.add(new CaseRule(condition, caseValue));
        }
        return result;
    }

    private static ValueSpec valueSpec(
            JsonNode node,
            String literalField,
            String expressionField,
            boolean required,
            ValidationErrorFactory error) {
        String expression = text(node, expressionField, "");
        if (!expression.isBlank()) {
            SafeExpressionSupport.validate(expression, "Case when value expression");
            return ValueSpec.expression(expression);
        }
        JsonNode literal = node == null ? null : node.path(literalField);
        if (literal != null && !literal.isMissingNode()) {
            return ValueSpec.literal(toValue(literal));
        }
        if (required) {
            throw error.apply(ValidationError.valueRequired());
        }
        return ValueSpec.empty();
    }

    private static ValueSpec defaultValueSpec(JsonNode node, ValidationErrorFactory error) {
        DefaultMode defaultMode = parseDefaultMode(text(node, "defaultMode", null), error);
        return switch (defaultMode) {
            case NONE -> ValueSpec.empty();
            case VALUE -> {
                JsonNode literal = node == null ? null : node.path("defaultValue");
                if (literal == null || literal.isMissingNode() || literal.isNull()
                        || literal.isTextual() && literal.asText().trim().isBlank()) {
                    throw error.apply(ValidationError.valueRequired());
                }
                yield ValueSpec.literal(toValue(literal));
            }
            case EXPRESSION -> {
                String expression = text(node, "defaultExpression", "");
                if (expression.isBlank()) {
                    throw error.apply(ValidationError.valueRequired());
                }
                SafeExpressionSupport.validate(expression, "Case when value expression");
                yield ValueSpec.expression(expression);
            }
            case LEGACY -> valueSpec(node, "defaultValue", "defaultExpression", false, error);
        };
    }

    private static DefaultMode parseDefaultMode(String value, ValidationErrorFactory error) {
        if (value == null || value.isBlank()) {
            return DefaultMode.LEGACY;
        }
        try {
            DefaultMode defaultMode = DefaultMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
            if (defaultMode == DefaultMode.LEGACY) {
                throw new IllegalArgumentException();
            }
            return defaultMode;
        } catch (Exception exception) {
            throw error.apply(ValidationError.unsupportedDefaultMode(value));
        }
    }

    private static Object toValue(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        if (node.isIntegralNumber()) {
            return node.asLong();
        }
        if (node.isFloatingPointNumber()) {
            return node.asDouble();
        }
        if (node.isArray()) {
            List<Object> values = new ArrayList<>();
            node.forEach(item -> values.add(toValue(item)));
            return values;
        }
        if (node.isObject()) {
            Map<String, Object> values = new LinkedHashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                values.put(field.getKey(), toValue(field.getValue()));
            }
            return values;
        }
        return node.asText();
    }

    private static String text(JsonNode config, String fieldName, String fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        String text = value == null || value.isMissingNode() || value.isNull() ? fallback : value.asText(fallback);
        return text == null ? "" : text.trim();
    }

    @FunctionalInterface
    public interface ValidationErrorFactory {
        IllegalArgumentException apply(ValidationError error);
    }

    public record ValidationError(String messageKey, String defaultMessage, Object... args) {

        static ValidationError targetFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.caseWhen.targetFieldRequired",
                    "Case when config targetField is required.");
        }

        static ValidationError casesRequired() {
            return new ValidationError(
                    "pipeline.validation.caseWhen.casesRequired",
                    "Case when config cases must contain at least one item.");
        }

        static ValidationError conditionRequired() {
            return new ValidationError(
                    "pipeline.validation.caseWhen.conditionRequired",
                    "Case when config condition is required.");
        }

        static ValidationError valueRequired() {
            return new ValidationError(
                    "pipeline.validation.caseWhen.valueRequired",
                    "Case when config case value or expression is required.");
        }

        static ValidationError unsupportedDefaultMode(String value) {
            return new ValidationError(
                    "pipeline.validation.caseWhen.unsupportedDefaultMode",
                    "Case when config defaultMode has unsupported value: " + value,
                    value);
        }
    }

    private enum DefaultMode {
        NONE,
        VALUE,
        EXPRESSION,
        LEGACY
    }
}
