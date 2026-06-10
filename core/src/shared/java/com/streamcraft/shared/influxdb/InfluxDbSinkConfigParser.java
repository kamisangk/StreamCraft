package com.streamcraft.shared.influxdb;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class InfluxDbSinkConfigParser {

    private static final Pattern SAFE_DATABASE = Pattern.compile("[A-Za-z0-9_.$-]+");
    private static final Pattern SAFE_FIELD = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern SAFE_MEASUREMENT_LITERAL = Pattern.compile("[A-Za-z0-9_.$,+-]+");
    private static final Pattern MEASUREMENT_PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)}");
    private static final Set<String> SUPPORTED_PRECISIONS = Set.of("n", "u", "us", "ms", "s", "m", "h");

    private InfluxDbSinkConfigParser() {
    }

    public static InfluxDbSinkConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static InfluxDbSinkConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        String url = normalizeUrl(text(safeConfig, "url", ""));
        if (url.isBlank()) {
            throw error.apply(ValidationError.required("url"));
        }
        validateUrl(url, error);

        String database = text(safeConfig, "database", "");
        if (database.isBlank()) {
            throw error.apply(ValidationError.required("database"));
        }
        if (!SAFE_DATABASE.matcher(database).matches()) {
            throw error.apply(ValidationError.invalidDatabase(database));
        }

        String measurement = text(safeConfig, "measurement", "");
        if (measurement.isBlank()) {
            throw error.apply(ValidationError.required("measurement"));
        }
        validateMeasurement(measurement, error);

        String keyTime = text(safeConfig, "keyTime", InfluxDbSinkConfig.DEFAULT_KEY_TIME);
        if (keyTime.isBlank()) {
            keyTime = InfluxDbSinkConfig.DEFAULT_KEY_TIME;
        }
        validateField(keyTime, "keyTime", error);

        List<String> keyTags = strings(safeConfig, "keyTags");
        validateFields(keyTags, "key tag", error);
        List<String> fields = strings(safeConfig, "fields");
        validateFields(fields, "field", error);

        int batchSize = intValue(safeConfig, "batchSize", InfluxDbSinkConfig.DEFAULT_BATCH_SIZE);
        if (batchSize <= 0) {
            throw error.apply(ValidationError.positive("batchSize"));
        }
        int maxRetries = intValue(safeConfig, "maxRetries", InfluxDbSinkConfig.DEFAULT_MAX_RETRIES);
        if (maxRetries < 0) {
            throw error.apply(ValidationError.nonNegative("maxRetries"));
        }
        long retryBackoffMultiplierMillis = longValue(
                safeConfig,
                "retryBackoffMultiplierMillis",
                InfluxDbSinkConfig.DEFAULT_RETRY_BACKOFF_MULTIPLIER_MILLIS);
        if (retryBackoffMultiplierMillis <= 0) {
            throw error.apply(ValidationError.positive("retryBackoffMultiplierMillis"));
        }
        long maxRetryBackoffMillis = longValue(
                safeConfig,
                "maxRetryBackoffMillis",
                InfluxDbSinkConfig.DEFAULT_MAX_RETRY_BACKOFF_MILLIS);
        if (maxRetryBackoffMillis <= 0) {
            throw error.apply(ValidationError.positive("maxRetryBackoffMillis"));
        }
        int connectTimeoutMillis = intValue(
                safeConfig, "connectTimeoutMillis", InfluxDbSinkConfig.DEFAULT_CONNECT_TIMEOUT_MILLIS);
        if (connectTimeoutMillis <= 0) {
            throw error.apply(ValidationError.positive("connectTimeoutMillis"));
        }
        long flushIntervalMillis = longValue(
                safeConfig,
                "flushIntervalMillis",
                InfluxDbSinkConfig.DEFAULT_FLUSH_INTERVAL_MILLIS);
        if (flushIntervalMillis <= 0) {
            throw error.apply(ValidationError.positive("flushIntervalMillis"));
        }
        String precision = normalizePrecision(text(safeConfig, "precision", InfluxDbSinkConfig.DEFAULT_PRECISION));
        if (!SUPPORTED_PRECISIONS.contains(precision)) {
            throw error.apply(ValidationError.unsupportedValue("precision", precision));
        }

        return new InfluxDbSinkConfig(
                url,
                database,
                measurement,
                keyTime,
                keyTags,
                fields,
                batchSize,
                maxRetries,
                retryBackoffMultiplierMillis,
                maxRetryBackoffMillis,
                connectTimeoutMillis,
                flushIntervalMillis,
                precision,
                text(safeConfig, "username", ""),
                text(safeConfig, "password", ""));
    }

    private static String normalizeUrl(String url) {
        String cleanUrl = url == null ? "" : url.trim();
        if (cleanUrl.isBlank()) {
            return "";
        }
        if (!cleanUrl.startsWith("http://") && !cleanUrl.startsWith("https://")) {
            cleanUrl = "http://" + cleanUrl;
        }
        return cleanUrl.endsWith("/") ? cleanUrl.substring(0, cleanUrl.length() - 1) : cleanUrl;
    }

    private static void validateUrl(String url, ValidationErrorFactory error) {
        try {
            URI uri = URI.create(url);
            if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) || uri.getHost() == null) {
                throw error.apply(ValidationError.invalidUrl(url));
            }
        } catch (IllegalArgumentException exception) {
            throw error.apply(ValidationError.invalidUrl(url));
        }
    }

    private static void validateMeasurement(String measurement, ValidationErrorFactory error) {
        Matcher matcher = MEASUREMENT_PLACEHOLDER.matcher(measurement);
        while (matcher.find()) {
            validateField(matcher.group(1), "measurement placeholder", error);
        }
        String literal = MEASUREMENT_PLACEHOLDER.matcher(measurement).replaceAll("");
        if (!literal.isBlank() && !SAFE_MEASUREMENT_LITERAL.matcher(literal).matches()) {
            throw error.apply(ValidationError.invalidMeasurement(measurement));
        }
        if (literal.contains("$") || literal.contains("{") || literal.contains("}")) {
            throw error.apply(ValidationError.invalidMeasurement(measurement));
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

    private static String normalizePrecision(String precision) {
        String normalized = precision == null || precision.isBlank()
                ? InfluxDbSinkConfig.DEFAULT_PRECISION
                : precision.trim().toLowerCase(Locale.ROOT);
        return "us".equals(normalized) ? "u" : normalized;
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
                    "pipeline.validation.influxDbSink.required",
                    "InfluxDB Sink config " + fieldName + " is required.",
                    fieldName);
        }

        static ValidationError invalidUrl(String url) {
            return new ValidationError(
                    "pipeline.validation.influxDbSink.invalidUrl",
                    "InfluxDB Sink config url is invalid: " + url,
                    url);
        }

        static ValidationError invalidDatabase(String database) {
            return new ValidationError(
                    "pipeline.validation.influxDbSink.invalidDatabase",
                    "InfluxDB Sink config database is invalid: " + database,
                    database);
        }

        static ValidationError invalidMeasurement(String measurement) {
            return new ValidationError(
                    "pipeline.validation.influxDbSink.invalidMeasurement",
                    "InfluxDB Sink config measurement is invalid: " + measurement,
                    measurement);
        }

        static ValidationError invalidField(String label, String field) {
            return new ValidationError(
                    "pipeline.validation.influxDbSink.invalidField",
                    "InfluxDB Sink config " + label + " is invalid: " + field,
                    label,
                    field);
        }

        static ValidationError duplicateField(String label, String field) {
            return new ValidationError(
                    "pipeline.validation.influxDbSink.duplicateField",
                    "InfluxDB Sink config " + label + " must be unique: " + field,
                    label,
                    field);
        }

        static ValidationError positive(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.influxDbSink.positive",
                    "InfluxDB Sink config " + fieldName + " must be greater than 0.",
                    fieldName);
        }

        static ValidationError nonNegative(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.influxDbSink.nonNegative",
                    "InfluxDB Sink config " + fieldName + " must be greater than or equal to 0.",
                    fieldName);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.influxDbSink.unsupportedValue",
                    "InfluxDB Sink config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
