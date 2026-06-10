package com.streamcraft.shared.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig.AuthType;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig.CursorType;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig.ReadMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.regex.Pattern;

public final class ElasticsearchSourceConfigParser {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern SAFE_INDEX = Pattern.compile("[A-Za-z0-9_.*,+-]+");
    private static final Pattern SAFE_FIELD = Pattern.compile("_id|[A-Za-z_][A-Za-z0-9_.]*");
    private static final Pattern SAFE_SCROLL_TIME = Pattern.compile("[1-9][0-9]*(ms|s|m|h)");

    private ElasticsearchSourceConfigParser() {
    }

    public static ElasticsearchSourceConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static ElasticsearchSourceConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        List<String> hosts = strings(safeConfig, "hosts");
        if (hosts.isEmpty()) {
            throw error.apply(ValidationError.required("hosts"));
        }
        hosts.forEach(host -> validateHost(host, error));

        String index = text(safeConfig, "index", "");
        if (index.isBlank()) {
            throw error.apply(ValidationError.required("index"));
        }
        if (!SAFE_INDEX.matcher(index).matches()) {
            throw error.apply(ValidationError.invalidIndex(index));
        }

        List<String> sourceFields = strings(safeConfig, "source");
        sourceFields.forEach(field -> validateField(field, "source field", error));
        JsonNode query = safeConfig == null ? null : safeConfig.path("query");
        String queryJson = "{}";
        if (query != null && !query.isMissingNode() && !query.isNull()) {
            if (!query.isObject()) {
                throw error.apply(ValidationError.queryMustBeObject());
            }
            queryJson = compact(query);
        }

        ReadMode readMode = parseEnum(text(safeConfig, "readMode", "FULL"), ReadMode.class, "readMode", error);
        String cursorField = text(safeConfig, "cursorField", "");
        if (readMode == ReadMode.INCREMENTAL) {
            if (cursorField.isBlank()) {
                throw error.apply(ValidationError.cursorFieldRequired());
            }
            validateField(cursorField, "cursorField", error);
        } else if (!cursorField.isBlank()) {
            validateField(cursorField, "cursorField", error);
        }

        int scrollSize = intValue(safeConfig, "scrollSize", ElasticsearchSourceConfig.DEFAULT_SCROLL_SIZE);
        if (scrollSize <= 0) {
            throw error.apply(ValidationError.positive("scrollSize"));
        }
        String scrollTime = text(safeConfig, "scrollTime", ElasticsearchSourceConfig.DEFAULT_SCROLL_TIME);
        if (scrollTime.isBlank()) {
            scrollTime = ElasticsearchSourceConfig.DEFAULT_SCROLL_TIME;
        }
        if (!SAFE_SCROLL_TIME.matcher(scrollTime).matches()) {
            throw error.apply(ValidationError.invalidScrollTime(scrollTime));
        }
        long pollIntervalMillis = longValue(
                safeConfig, "pollIntervalMillis", ElasticsearchSourceConfig.DEFAULT_POLL_INTERVAL_MILLIS);
        if (pollIntervalMillis <= 0) {
            throw error.apply(ValidationError.positive("pollIntervalMillis"));
        }
        int maxPolls = intValue(safeConfig, "maxPolls", 0);
        if (maxPolls < 0) {
            throw error.apply(ValidationError.nonNegative("maxPolls"));
        }

        String idField = text(safeConfig, "idField", "");
        if (!idField.isBlank()) {
            validateField(idField, "idField", error);
        }
        String timestampField = text(safeConfig, "timestampField", "");
        if (!timestampField.isBlank()) {
            validateField(timestampField, "timestampField", error);
        }

        AuthType authType = parseEnum(text(safeConfig, "authType", "NONE"), AuthType.class, "authType", error);
        String username = text(safeConfig, "username", "");
        String password = text(safeConfig, "password", "");
        String apiKey = text(safeConfig, "apiKey", "");
        if (authType == AuthType.BASIC && (username.isBlank() || password.isBlank())) {
            throw error.apply(ValidationError.basicAuthRequired());
        }
        if (authType == AuthType.API_KEY && apiKey.isBlank()) {
            throw error.apply(ValidationError.apiKeyRequired());
        }

        return new ElasticsearchSourceConfig(
                hosts,
                index,
                sourceFields,
                queryJson,
                readMode,
                cursorField,
                parseEnum(text(safeConfig, "cursorType", "STRING"), CursorType.class, "cursorType", error),
                text(safeConfig, "initialCursorValue", ""),
                pollIntervalMillis,
                scrollSize,
                scrollTime,
                maxPolls,
                idField,
                timestampField,
                authType,
                username,
                password,
                apiKey);
    }

    private static void validateHost(String host, ValidationErrorFactory error) {
        if (!(host.startsWith("http://") || host.startsWith("https://"))) {
            throw error.apply(ValidationError.invalidHost(host));
        }
    }

    private static void validateField(String field, String label, ValidationErrorFactory error) {
        if (!SAFE_FIELD.matcher(field).matches()) {
            throw error.apply(ValidationError.invalidField(label, field));
        }
    }

    private static String compact(JsonNode node) {
        try {
            return OBJECT_MAPPER.writeValueAsString(node);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
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

        static ValidationError required(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSource.required",
                    "Elasticsearch Source config " + fieldName + " is required.",
                    fieldName);
        }

        static ValidationError invalidHost(String host) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSource.invalidHost",
                    "Elasticsearch Source config host must start with http:// or https://: " + host,
                    host);
        }

        static ValidationError invalidIndex(String index) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSource.invalidIndex",
                    "Elasticsearch Source config index is invalid: " + index,
                    index);
        }

        static ValidationError invalidField(String label, String field) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSource.invalidField",
                    "Elasticsearch Source config " + label + " is invalid: " + field,
                    label,
                    field);
        }

        static ValidationError queryMustBeObject() {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSource.queryMustBeObject",
                    "Elasticsearch Source config query must be a JSON object.");
        }

        static ValidationError cursorFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSource.cursorFieldRequired",
                    "Elasticsearch Source config cursorField is required for INCREMENTAL read mode.");
        }

        static ValidationError invalidScrollTime(String scrollTime) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSource.invalidScrollTime",
                    "Elasticsearch Source config scrollTime is invalid: " + scrollTime,
                    scrollTime);
        }

        static ValidationError positive(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSource.positive",
                    "Elasticsearch Source config " + fieldName + " must be greater than 0.",
                    fieldName);
        }

        static ValidationError nonNegative(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSource.nonNegative",
                    "Elasticsearch Source config " + fieldName + " must be greater than or equal to 0.",
                    fieldName);
        }

        static ValidationError basicAuthRequired() {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSource.basicAuthRequired",
                    "Elasticsearch Source config username and password are required when authType is BASIC.");
        }

        static ValidationError apiKeyRequired() {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSource.apiKeyRequired",
                    "Elasticsearch Source config apiKey is required when authType is API_KEY.");
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.elasticsearchSource.unsupportedValue",
                    "Elasticsearch Source config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
