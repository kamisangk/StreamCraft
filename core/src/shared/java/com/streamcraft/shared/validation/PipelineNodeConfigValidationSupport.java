package com.streamcraft.shared.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.aggregation.AggregateConfigParser;
import com.streamcraft.shared.casewhen.CaseWhenConfigParser;
import com.streamcraft.shared.dataquality.DataQualityConfigParser;
import com.streamcraft.shared.deduplication.DeduplicateConfigParser;
import com.streamcraft.shared.eval.EvalConfigParser;
import com.streamcraft.shared.explode.ExplodeConfigParser;
import com.streamcraft.shared.expression.SafeExpressionSupport;
import com.streamcraft.shared.flatten.FlattenConfigParser;
import com.streamcraft.shared.lookup.LookupEnrichConfigParser;
import com.streamcraft.shared.lookupjoin.LookupJoinConfigParser;
import com.streamcraft.shared.maskhash.MaskHashConfigParser;
import com.streamcraft.shared.pattern.GrokPatternSupport;
import com.streamcraft.shared.route.RouteConfigParser;
import com.streamcraft.shared.streamjoin.StreamJoinConfigParser;
import com.streamcraft.shared.timederive.TimeDeriveConfigParser;
import java.util.Set;

public final class PipelineNodeConfigValidationSupport {

    private static final Set<String> SUPPORTED_CONSUME_MODES = Set.of("earliest", "latest", "committed");
    private static final Set<String> SUPPORTED_FORMATS = Set.of("JSON", "TEXT");
    private static final Set<String> SUPPORTED_TRANSFORM_SERDE_FORMATS = Set.of("JSON", "KV", "CSV", "XML");
    private static final Set<String> SUPPORTED_CUSTOM_CODE_LANGUAGES = Set.of("JAVA");
    private static final Set<String> SUPPORTED_CUSTOM_CODE_COMPILE_PATTERNS = Set.of("SOURCE_CODE");
    private static final Set<String> SUPPORTED_CUSTOM_CODE_ERROR_STRATEGIES =
            Set.of("KEEP_ORIGINAL", "SKIP", "FAIL");
    private static final Set<String> SUPPORTED_CAST_TARGET_TYPES =
            Set.of("STRING", "INT", "INTEGER", "LONG", "DOUBLE", "FLOAT", "BOOLEAN");

    private PipelineNodeConfigValidationSupport() {
    }

    public static void validateTransformConfig(
            String operator,
            JsonNode config,
            ValidationErrorFactory error) {
        JsonNode safeConfig = config == null ? null : config;
        switch (operator) {
            case "DESERIALIZE" -> validateDeserialize(safeConfig, error);
            case "SERIALIZE" -> validateSerialize(safeConfig, error);
            case "FILTER" -> SafeExpressionSupport.validate(
                    pathText(safeConfig, "condition"), "Filter condition");
            case "GROK" -> validateGrok(safeConfig, error);
            case "CAST" -> validateCast(safeConfig, error);
            case "RENAME" -> validateRename(safeConfig, error);
            case "EVAL" -> EvalConfigParser.parseValidated(safeConfig, validationError -> error.apply(adapt(validationError)));
            case "AGGREGATE" -> AggregateConfigParser.parseValidated(
                    safeConfig, validationError -> error.apply(adapt(validationError)));
            case "DEDUPLICATE" -> DeduplicateConfigParser.parseValidated(
                    safeConfig, validationError -> error.apply(adapt(validationError)));
            case "LOOKUP_ENRICH" -> LookupEnrichConfigParser.parseValidated(
                    safeConfig, validationError -> error.apply(adapt(validationError)));
            case "LOOKUP_JOIN" -> LookupJoinConfigParser.parseValidated(
                    safeConfig, validationError -> error.apply(adapt(validationError)));
            case "STREAM_JOIN" -> StreamJoinConfigParser.parseValidated(
                    safeConfig, validationError -> error.apply(adapt(validationError)));
            case "FLATTEN" -> FlattenConfigParser.parseValidated(
                    safeConfig, validationError -> error.apply(adapt(validationError)));
            case "EXPLODE" -> ExplodeConfigParser.parseValidated(
                    safeConfig, validationError -> error.apply(adapt(validationError)));
            case "DATA_QUALITY" -> DataQualityConfigParser.parseValidated(
                    safeConfig, validationError -> error.apply(adapt(validationError)));
            case "TIME_DERIVE" -> TimeDeriveConfigParser.parseValidated(
                    safeConfig, validationError -> error.apply(adapt(validationError)));
            case "MASK_HASH" -> MaskHashConfigParser.parseValidated(
                    safeConfig, validationError -> error.apply(adapt(validationError)));
            case "CASE_WHEN" -> CaseWhenConfigParser.parseValidated(
                    safeConfig, validationError -> error.apply(adapt(validationError)));
            case "ROUTE" -> RouteConfigParser.parseValidated(
                    safeConfig, validationError -> error.apply(adapt(validationError)));
            case "CUSTOM_CODE" -> validateCustomCode(safeConfig, error);
            default -> {
            }
        }
    }

    private static void validateDeserialize(JsonNode config, ValidationErrorFactory error) {
        requireText(config, "field", error);
        requireText(config, "targetField", error);
        String format = validateTransformSerdeFormat(config, error);
        if ("CSV".equals(format)) {
            requireNonEmptyArray(config, "fieldNames", error);
        }
    }

    private static void validateSerialize(JsonNode config, ValidationErrorFactory error) {
        requireNonEmptyArray(config, "sourceFields", error);
        requireText(config, "targetField", error);
        validateTransformSerdeFormat(config, error);
    }

    private static void validateGrok(JsonNode config, ValidationErrorFactory error) {
        requireText(config, "inputField", error);
        requireText(config, "outputField", error);
        GrokPatternSupport.validate(pathText(config, "pattern"), "Grok pattern");
    }

    private static void validateCast(JsonNode config, ValidationErrorFactory error) {
        requireText(config, "inputField", error);
        requireText(config, "outputField", error);
        validateCastTargetType(pathText(config, "targetType"), error);
    }

    private static void validateRename(JsonNode config, ValidationErrorFactory error) {
        JsonNode mapping = path(config, "mapping");
        if (mapping == null || !mapping.isObject() || !mapping.fields().hasNext()) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.config.mappingRequired",
                    "Node config mapping must contain at least one field mapping."));
        }
        mapping.fields().forEachRemaining(entry -> {
            if (entry.getKey() == null || entry.getKey().isBlank()) {
                throw error.apply(new ValidationError(
                        "pipeline.validation.config.mappingSourceFieldRequired",
                        "Node config mapping source field is required."));
            }
            if (entry.getValue() == null || entry.getValue().asText().isBlank()) {
                throw error.apply(new ValidationError(
                        "pipeline.validation.config.mappingTargetFieldRequired",
                        "Node config mapping target field is required."));
            }
        });
    }

    private static void validateCustomCode(JsonNode config, ValidationErrorFactory error) {
        validateAllowedValue(
                pathText(config, "language", "JAVA"),
                SUPPORTED_CUSTOM_CODE_LANGUAGES,
                "language",
                "pipeline.validation.language.javaOnly",
                error,
                "Node config language must be JAVA.");
        validateAllowedValue(
                pathText(config, "compilePattern", "SOURCE_CODE"),
                SUPPORTED_CUSTOM_CODE_COMPILE_PATTERNS,
                "compilePattern",
                "pipeline.validation.compilePattern.sourceCodeOnly",
                error,
                "Node config compilePattern must be SOURCE_CODE.");
        requireText(config, "className", error);
        requireText(config, "sourceCode", error);
        validateAllowedValue(
                pathText(config, "errorStrategy", "KEEP_ORIGINAL"),
                SUPPORTED_CUSTOM_CODE_ERROR_STRATEGIES,
                "errorStrategy",
                "pipeline.validation.errorStrategy.oneOf",
                error,
                "Node config errorStrategy must be one of: KEEP_ORIGINAL, SKIP, FAIL.");
    }

    private static String validateTransformSerdeFormat(JsonNode config, ValidationErrorFactory error) {
        String format = requireText(config, "format", error);
        if (!SUPPORTED_TRANSFORM_SERDE_FORMATS.contains(format)) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.format.oneOf",
                    "Node config format must be one of: " + String.join(", ", SUPPORTED_TRANSFORM_SERDE_FORMATS),
                    String.join(", ", SUPPORTED_TRANSFORM_SERDE_FORMATS)));
        }
        return format;
    }

    private static void validateCastTargetType(
            String targetType,
            ValidationErrorFactory error) {
        String normalized = normalize(targetType);
        if (!SUPPORTED_CAST_TARGET_TYPES.contains(normalized)) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.targetType.oneOf",
                    "Node config targetType must be one of: STRING, INT, INTEGER, LONG, DOUBLE, FLOAT, BOOLEAN"));
        }
    }

    private static void validateAllowedValue(
            String value,
            Set<String> allowedValues,
            String fieldName,
            String messageKey,
            ValidationErrorFactory error,
            String defaultMessage) {
        if (!allowedValues.contains(normalize(value))) {
            throw error.apply(new ValidationError(
                    messageKey,
                    defaultMessage,
                    fieldName,
                    String.join(", ", allowedValues)));
        }
    }

    private static String requireText(
            JsonNode config,
            String fieldName,
            ValidationErrorFactory error) {
        String value = pathText(config, fieldName);
        if (value.isBlank()) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.config.missingField",
                    "Node config field is required: " + fieldName,
                    fieldName));
        }
        return value;
    }

    private static void requireNonEmptyArray(
            JsonNode config,
            String fieldName,
            ValidationErrorFactory error) {
        JsonNode value = path(config, fieldName);
        if (value == null || !value.isArray() || value.isEmpty()) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.config.missingField",
                    "Node config field is required: " + fieldName,
                    fieldName));
        }
    }

    private static String pathText(JsonNode config, String fieldName) {
        return pathText(config, fieldName, null);
    }

    private static String pathText(JsonNode config, String fieldName, String fallback) {
        JsonNode value = path(config, fieldName);
        if (value == null || value.isNull() || value.isMissingNode()) {
            return fallback == null ? "" : fallback;
        }
        return value.asText(fallback == null ? "" : fallback);
    }

    private static JsonNode path(JsonNode config, String fieldName) {
        return config == null ? null : config.path(fieldName);
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase();
    }

    private static ValidationError adapt(
            com.streamcraft.shared.eval.EvalConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    private static ValidationError adapt(
            com.streamcraft.shared.aggregation.AggregateConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    private static ValidationError adapt(
            com.streamcraft.shared.deduplication.DeduplicateConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    private static ValidationError adapt(
            com.streamcraft.shared.lookup.LookupEnrichConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    private static ValidationError adapt(
            com.streamcraft.shared.lookupjoin.LookupJoinConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    private static ValidationError adapt(
            com.streamcraft.shared.streamjoin.StreamJoinConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    private static ValidationError adapt(
            com.streamcraft.shared.flatten.FlattenConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    private static ValidationError adapt(
            com.streamcraft.shared.explode.ExplodeConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    private static ValidationError adapt(
            com.streamcraft.shared.dataquality.DataQualityConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    private static ValidationError adapt(
            com.streamcraft.shared.timederive.TimeDeriveConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    private static ValidationError adapt(
            com.streamcraft.shared.maskhash.MaskHashConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    private static ValidationError adapt(
            com.streamcraft.shared.casewhen.CaseWhenConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    private static ValidationError adapt(
            com.streamcraft.shared.route.RouteConfigParser.ValidationError error) {
        return new ValidationError(error.messageKey(), error.defaultMessage(), error.args());
    }

    public record ValidationError(String messageKey, String defaultMessage, Object... args) {
    }

    @FunctionalInterface
    public interface ValidationErrorFactory {
        IllegalArgumentException apply(ValidationError error);
    }
}
