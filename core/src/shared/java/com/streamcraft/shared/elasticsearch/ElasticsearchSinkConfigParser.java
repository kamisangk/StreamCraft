package com.streamcraft.shared.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.elasticsearch.ElasticsearchSinkConfig.AuthType;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ElasticsearchSinkConfigParser {

    private static final Pattern SAFE_HOST = Pattern.compile("https?://[A-Za-z0-9._:-]+/?");
    private static final Pattern SAFE_INDEX_LITERAL = Pattern.compile("[a-zA-Z0-9_.*,+-]+");
    private static final Pattern INDEX_PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_.]*)}");
    private static final Pattern SAFE_FIELD = Pattern.compile("_id|[A-Za-z_][A-Za-z0-9_.]*");

    private ElasticsearchSinkConfigParser() {
    }

    public static ElasticsearchSinkConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static ElasticsearchSinkConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        List<String> hosts = strings(safeConfig, "hosts").stream()
                .map(ElasticsearchSinkConfigParser::normalizeHost)
                .toList();
        if (hosts.isEmpty()) {
            throw error.apply(ValidationError.required("hosts"));
        }
        hosts.forEach(host -> validateHost(host, error));

        String index = text(safeConfig, "index", "");
        if (index.isBlank()) {
            throw error.apply(ValidationError.required("index"));
        }
        validateIndex(index, error);

        String indexType = text(safeConfig, "indexType", "");
        if (!indexType.isBlank()) {
            validateIndexType(indexType, error);
        }

        List<String> primaryKeys = strings(safeConfig, "primaryKeys");
        validateFields(primaryKeys, "primary key field", error);
        List<String> fields = strings(safeConfig, "fields");
        validateFields(fields, "field", error);

        int maxBatchSize = intValue(safeConfig, "maxBatchSize", ElasticsearchSinkConfig.DEFAULT_MAX_BATCH_SIZE);
        if (maxBatchSize <= 0) {
            throw error.apply(ValidationError.positive("maxBatchSize"));
        }
        long flushIntervalMillis = longValue(
                safeConfig,
                "flushIntervalMillis",
                ElasticsearchSinkConfig.DEFAULT_FLUSH_INTERVAL_MILLIS);
        if (flushIntervalMillis <= 0) {
            throw error.apply(ValidationError.positive("flushIntervalMillis"));
        }
        int maxRetryCount = intValue(safeConfig, "maxRetryCount", ElasticsearchSinkConfig.DEFAULT_MAX_RETRY_COUNT);
        if (maxRetryCount < 0) {
            throw error.apply(ValidationError.nonNegative("maxRetryCount"));
        }

        AuthType authType = parseAuthType(text(safeConfig, "authType", "NONE"), error);
        String username = text(safeConfig, "username", "");
        String password = text(safeConfig, "password", "");
        String apiKeyId = text(safeConfig, "apiKeyId", "");
        String apiKey = text(safeConfig, "apiKey", "");
        String apiKeyEncoded = text(safeConfig, "apiKeyEncoded", "");
        validateAuth(authType, username, password, apiKeyId, apiKey, apiKeyEncoded, error);

        return new ElasticsearchSinkConfig(
                hosts,
                index,
                indexType,
                primaryKeys,
                text(safeConfig, "keyDelimiter", ""),
                fields,
                maxBatchSize,
                flushIntervalMillis,
                maxRetryCount,
                authType,
                username,
                password,
                apiKeyId,
                apiKey,
                apiKeyEncoded);
    }

    private static String normalizeHost(String host) {
        if (host.startsWith("http://") || host.startsWith("https://")) {
            return host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
        }
        return "http://" + host;
    }

    private static void validateHost(String host, ValidationErrorFactory error) {
        if (!SAFE_HOST.matcher(host).matches()) {
            throw error.apply(ValidationError.invalidHost(host));
        }
    }

    private static void validateIndex(String index, ValidationErrorFactory error) {
        int placeholders = 0;
        Matcher matcher = INDEX_PLACEHOLDER.matcher(index);
        while (matcher.find()) {
            placeholders++;
            validateField(matcher.group(1), "index placeholder", error);
        }
        String withoutPlaceholders = INDEX_PLACEHOLDER.matcher(index).replaceAll("");
        if (withoutPlaceholders.isBlank() && placeholders == 0) {
            throw error.apply(ValidationError.invalidIndex(index));
        }
        if (!withoutPlaceholders.isBlank() && !SAFE_INDEX_LITERAL.matcher(withoutPlaceholders).matches()) {
            throw error.apply(ValidationError.invalidIndex(index));
        }
        if (withoutPlaceholders.contains("$") || withoutPlaceholders.contains("{") || withoutPlaceholders.contains("}")) {
            throw error.apply(ValidationError.invalidIndex(index));
        }
    }

    private static void validateIndexType(String indexType, ValidationErrorFactory error) {
        if (!SAFE_INDEX_LITERAL.matcher(indexType).matches()) {
            throw error.apply(ValidationError.invalidIndexType(indexType));
        }
    }

    private static void validateFields(List<String> fields, String label, ValidationErrorFactory error) {
        Set<String> seen = new java.util.LinkedHashSet<>();
        for (String field : fields) {
            validateField(field, label, error);
            if (!seen.add(field)) {
                throw error.apply(ValidationError.duplicateField(label, field));
            }
        }
    }

    private static void validateField(String field, String label, ValidationErrorFactory error) {
        if (!SAFE_FIELD.matcher(field).matches()) {
            throw error.apply(ValidationError.invalidField(label, field));
        }
    }

    private static void validateAuth(
            AuthType authType,
            String username,
            String password,
            String apiKeyId,
            String apiKey,
            String apiKeyEncoded,
            ValidationErrorFactory error) {
        if (authType == AuthType.BASIC && (username.isBlank() || password.isBlank())) {
            throw error.apply(ValidationError.basicAuthRequired());
        }
        if (authType == AuthType.API_KEY && (apiKeyId.isBlank() || apiKey.isBlank())) {
            throw error.apply(ValidationError.apiKeyRequired());
        }
        if (authType == AuthType.API_KEY_ENCODED && apiKeyEncoded.isBlank()) {
            throw error.apply(ValidationError.apiKeyEncodedRequired());
        }
    }

    private static AuthType parseAuthType(String value, ValidationErrorFactory error) {
        String normalized = value == null || value.isBlank() ? "NONE" : value.trim().toUpperCase(Locale.ROOT);
        try {
            return AuthType.valueOf(normalized);
        } catch (Exception exception) {
            throw error.apply(ValidationError.unsupportedValue("authType", value));
        }
    }

    private static List<String> strings(JsonNode config, String fieldName) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || value.isMissingNode() || value.isNull()) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        if (value.isArray()) {
            for (JsonNode item : value) {
                String text = item.asText("").trim();
                if (!text.isBlank()) {
                    values.add(text);
                }
            }
            return List.copyOf(values);
        }
        for (String item : value.asText("").split(",", -1)) {
            String text = item.trim();
            if (!text.isBlank()) {
                values.add(text);
            }
        }
        return List.copyOf(values);
    }

    private static String text(JsonNode config, String fieldName, String fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        String text = value == null || value.isMissingNode() || value.isNull() ? fallback : value.asText(fallback);
        return text == null ? "" : text.trim();
    }

    private static long longValue(JsonNode config, String fieldName, long fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        return value == null || value.isMissingNode() || value.isNull() ? fallback : value.asLong(fallback);
    }

    private static int intValue(JsonNode config, String fieldName, int fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        return value == null || value.isMissingNode() || value.isNull() ? fallback : value.asInt(fallback);
    }

    @FunctionalInterface
    public interface ValidationErrorFactory {
        IllegalArgumentException apply(ValidationError error);
    }

    public record ValidationError(String messageKey, String defaultMessage, Object... args) {

        static ValidationError required(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSink.required",
                    "Elasticsearch Sink config " + fieldName + " is required.",
                    fieldName);
        }

        static ValidationError invalidHost(String host) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSink.invalidHost",
                    "Elasticsearch Sink config host is invalid: " + host,
                    host);
        }

        static ValidationError invalidIndex(String index) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSink.invalidIndex",
                    "Elasticsearch Sink config index is invalid: " + index,
                    index);
        }

        static ValidationError invalidIndexType(String indexType) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSink.invalidIndexType",
                    "Elasticsearch Sink config indexType is invalid: " + indexType,
                    indexType);
        }

        static ValidationError invalidField(String label, String field) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSink.invalidField",
                    "Elasticsearch Sink config " + label + " is invalid: " + field,
                    label,
                    field);
        }

        static ValidationError duplicateField(String label, String field) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSink.duplicateField",
                    "Elasticsearch Sink config " + label + " must be unique: " + field,
                    label,
                    field);
        }

        static ValidationError positive(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSink.positive",
                    "Elasticsearch Sink config " + fieldName + " must be greater than 0.",
                    fieldName);
        }

        static ValidationError nonNegative(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSink.nonNegative",
                    "Elasticsearch Sink config " + fieldName + " must be greater than or equal to 0.",
                    fieldName);
        }

        static ValidationError basicAuthRequired() {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSink.basicAuthRequired",
                    "Elasticsearch Sink config username and password are required when authType is BASIC.");
        }

        static ValidationError apiKeyRequired() {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSink.apiKeyRequired",
                    "Elasticsearch Sink config apiKeyId and apiKey are required when authType is API_KEY.");
        }

        static ValidationError apiKeyEncodedRequired() {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSink.apiKeyEncodedRequired",
                    "Elasticsearch Sink config apiKeyEncoded is required when authType is API_KEY_ENCODED.");
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSink.unsupportedValue",
                    "Elasticsearch Sink config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
