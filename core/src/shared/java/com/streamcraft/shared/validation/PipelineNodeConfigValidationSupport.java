package com.streamcraft.shared.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.shared.aggregation.AggregateConfigParser;
import com.streamcraft.shared.casewhen.CaseWhenConfigParser;
import com.streamcraft.shared.dataquality.DataQualityConfigParser;
import com.streamcraft.shared.deduplication.DeduplicateConfigParser;
import com.streamcraft.shared.elasticsearch.ElasticsearchSinkConfigParser;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfigParser;
import com.streamcraft.shared.eval.EvalConfigParser;
import com.streamcraft.shared.explode.ExplodeConfigParser;
import com.streamcraft.shared.expression.SafeExpressionSupport;
import com.streamcraft.shared.file.HdfsFileSinkConfigParser;
import com.streamcraft.shared.file.HdfsFileSourceConfigParser;
import com.streamcraft.shared.flatten.FlattenConfigParser;
import com.streamcraft.shared.influxdb.InfluxDbSinkConfigParser;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfigParser;
import com.streamcraft.shared.jdbc.JdbcSinkConfigParser;
import com.streamcraft.shared.jdbc.JdbcSourceConfigParser;
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
    private static final Set<String> SUPPORTED_AUTH_TYPES = Set.of("NONE", "SASL_PLAIN", "SASL_SCRAM");
    private static final Set<String> SUPPORTED_SCRAM_MECHANISMS = Set.of("SCRAM-SHA-256", "SCRAM-SHA-512");
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

    public static void validateSourceConfig(
            String operator,
            JsonNode config,
            ValidationErrorFactory error) {
        switch (operator) {
            case "KAFKA_SOURCE" -> validateKafkaSource(config, error);
            case "JDBC_SOURCE" -> JdbcSourceConfigParser.parseValidated(
                    config, validationError -> error.apply(new ValidationError(
                            validationError.messageKey(),
                            validationError.defaultMessage(),
                            validationError.args())));
            case "ELASTICSEARCH_SOURCE" -> ElasticsearchSourceConfigParser.parseValidated(
                    config, validationError -> error.apply(new ValidationError(
                            validationError.messageKey(),
                            validationError.defaultMessage(),
                            validationError.args())));
            case "INFLUXDB_SOURCE" -> InfluxDbSourceConfigParser.parseValidated(
                    config, validationError -> error.apply(new ValidationError(
                            validationError.messageKey(),
                            validationError.defaultMessage(),
                            validationError.args())));
            case "HDFS_FILE_SOURCE" -> HdfsFileSourceConfigParser.parseValidated(
                    config, validationError -> error.apply(new ValidationError(
                            validationError.messageKey(),
                            validationError.defaultMessage(),
                            validationError.args())));
            default -> throw error.apply(new ValidationError(
                    "pipeline.validation.source.unsupported",
                    "Unsupported source operator: " + operator,
                    operator));
        }
    }

    public static void validateSinkConfig(
            String operator,
            JsonNode config,
            ValidationErrorFactory error) {
        switch (operator) {
            case "KAFKA_SINK" -> validateKafkaSink(config, error);
            case "JDBC_SINK" -> JdbcSinkConfigParser.parseValidated(
                    config, validationError -> error.apply(new ValidationError(
                            validationError.messageKey(),
                            validationError.defaultMessage(),
                            validationError.args())));
            case "ELASTICSEARCH_SINK" -> ElasticsearchSinkConfigParser.parseValidated(
                    config, validationError -> error.apply(new ValidationError(
                            validationError.messageKey(),
                            validationError.defaultMessage(),
                            validationError.args())));
            case "INFLUXDB_SINK" -> InfluxDbSinkConfigParser.parseValidated(
                    config, validationError -> error.apply(new ValidationError(
                            validationError.messageKey(),
                            validationError.defaultMessage(),
                            validationError.args())));
            case "HDFS_FILE_SINK" -> HdfsFileSinkConfigParser.parseValidated(
                    config, validationError -> error.apply(new ValidationError(
                            validationError.messageKey(),
                            validationError.defaultMessage(),
                            validationError.args())));
            default -> throw error.apply(new ValidationError(
                    "pipeline.validation.sink.unsupported",
                    "Unsupported sink operator: " + operator,
                    operator));
        }
    }

    public static void validatePreviewSource(
            JsonNode config,
            ObjectMapper objectMapper,
            ValidationErrorFactory error) {
        String format = validateFormat(pathText(config, "format", "JSON"), error);
        JsonNode sampleData = path(config, "sampleData");
        if (sampleData == null || !sampleData.isArray()) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.preview.sampleDataStringArray",
                    "Preview requires Kafka Source sampleData to be a string array."));
        }
        for (JsonNode item : sampleData) {
            if (!item.isTextual()) {
                throw error.apply(new ValidationError(
                        "pipeline.validation.preview.sampleDataStringArray",
                        "Preview requires Kafka Source sampleData to be a string array."));
            }
            if ("JSON".equals(format)) {
                validatePreviewJsonSample(item.asText(), objectMapper, error);
            }
        }
    }

    public static void validatePreviewSink(
            String operator,
            JsonNode config,
            ValidationErrorFactory error) {
        if (!"KAFKA_SINK".equals(operator)) {
            return;
        }
        if (config == null || config.isMissingNode() || config.isNull() || !config.hasNonNull("format")) {
            return;
        }
        String format = validateFormat(pathText(config, "format"), error);
        if ("TEXT".equals(format)) {
            requireText(config, "messageField", error);
        }
    }

    private static void validateKafkaSource(JsonNode config, ValidationErrorFactory error) {
        requireText(config, "bootstrapServers", error);
        JsonNode topics = path(config, "topics");
        if (topics == null || !topics.isArray() || topics.isEmpty()) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.kafkaSource.topicRequired",
                    "Kafka Source must contain at least one topic."));
        }
        requireText(config, "groupId", error);
        validateConsumeMode(requireText(config, "consumeMode", error), error);
        validateKafkaAuth(config, "Kafka Source", error);
        validateFormat(requireText(config, "format", error), error);
    }

    private static void validateKafkaSink(JsonNode config, ValidationErrorFactory error) {
        requireText(config, "bootstrapServers", error);
        requireText(config, "topic", error);
        requireText(config, "deliveryGuarantee", error);
        validateKafkaAuth(config, "Kafka Sink", error);
        String format = validateFormat(requireText(config, "format", error), error);
        if ("TEXT".equals(format)) {
            requireText(config, "messageField", error);
        }
    }

    private static void validateKafkaAuth(
            JsonNode config,
            String nodeLabel,
            ValidationErrorFactory error) {
        String authType = pathText(config, "authType");
        if (authType.isBlank()) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.auth.required",
                    "Node config field is required: authType",
                    nodeLabel));
        }
        if (!SUPPORTED_AUTH_TYPES.contains(authType)) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.auth.oneOf",
                    nodeLabel + " authType must be one of: " + joinValues(SUPPORTED_AUTH_TYPES) + ".",
                    nodeLabel,
                    joinValues(SUPPORTED_AUTH_TYPES)));
        }
        switch (authType) {
            case "NONE" -> {
                return;
            }
            case "SASL_PLAIN" -> {
                requireAuthField(config, nodeLabel, "username", authType, error);
                requireAuthField(config, nodeLabel, "password", authType, error);
            }
            case "SASL_SCRAM" -> {
                requireAuthField(config, nodeLabel, "username", authType, error);
                requireAuthField(config, nodeLabel, "password", authType, error);
                String scramMechanism = requireAuthField(config, nodeLabel, "scramMechanism", authType, error);
                if (!SUPPORTED_SCRAM_MECHANISMS.contains(scramMechanism)) {
                    throw error.apply(new ValidationError(
                            "pipeline.validation.scram.oneOf",
                            nodeLabel + " scramMechanism must be one of: "
                                    + joinValues(SUPPORTED_SCRAM_MECHANISMS) + ".",
                            nodeLabel,
                            joinValues(SUPPORTED_SCRAM_MECHANISMS)));
                }
            }
            default -> throw error.apply(new ValidationError(
                    "pipeline.validation.auth.oneOf",
                    nodeLabel + " authType must be one of: " + joinValues(SUPPORTED_AUTH_TYPES) + ".",
                    nodeLabel,
                    joinValues(SUPPORTED_AUTH_TYPES)));
        }
    }

    private static String requireAuthField(
            JsonNode config,
            String nodeLabel,
            String fieldName,
            String authType,
            ValidationErrorFactory error) {
        String value = pathText(config, fieldName);
        if (value.isBlank()) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.auth.fieldRequired",
                    nodeLabel + " " + fieldName + " is required when authType is " + authType + ".",
                    nodeLabel,
                    fieldName,
                    authType));
        }
        return value;
    }

    private static void validateConsumeMode(String consumeMode, ValidationErrorFactory error) {
        if (!SUPPORTED_CONSUME_MODES.contains(consumeMode)) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.consumeMode.oneOf",
                    "Node config consumeMode must be one of: " + joinValues(SUPPORTED_CONSUME_MODES),
                    joinValues(SUPPORTED_CONSUME_MODES)));
        }
    }

    private static String validateFormat(String format, ValidationErrorFactory error) {
        if (!SUPPORTED_FORMATS.contains(format)) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.format.oneOf",
                    "Node config format must be one of: JSON, TEXT",
                    "JSON, TEXT"));
        }
        return format;
    }

    private static void validatePreviewJsonSample(
            String sample,
            ObjectMapper objectMapper,
            ValidationErrorFactory error) {
        try {
            JsonNode json = objectMapper.readTree(sample);
            if (json == null || !json.isObject()) {
                throw error.apply(new ValidationError(
                        "pipeline.validation.preview.jsonObject",
                        "Preview JSON sample must be a JSON object."));
            }
        } catch (java.io.IOException exception) {
            throw error.apply(new ValidationError(
                    "pipeline.validation.preview.jsonObject",
                    "Preview JSON sample must be a JSON object.",
                    exception));
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

    private static String joinValues(Set<String> values) {
        return values.stream().sorted().collect(java.util.stream.Collectors.joining(", "));
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

    public record ValidationError(
            String messageKey,
            String defaultMessage,
            Throwable cause,
            Object... args) {

        public ValidationError(String messageKey, String defaultMessage, Object... args) {
            this(messageKey, defaultMessage, null, args);
        }
    }

    @FunctionalInterface
    public interface ValidationErrorFactory {
        IllegalArgumentException apply(ValidationError error);
    }
}
