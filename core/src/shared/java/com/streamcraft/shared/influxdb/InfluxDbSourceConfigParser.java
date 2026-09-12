package com.streamcraft.shared.influxdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfig.CursorType;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfig.ReadMode;
import java.net.URI;
import java.util.Locale;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

public final class InfluxDbSourceConfigParser {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern SAFE_DATABASE = Pattern.compile("[A-Za-z0-9_.$-]+");
    private static final Pattern SAFE_FIELD = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern SAFE_MEASUREMENT = Pattern.compile("[A-Za-z_][A-Za-z0-9_.-]*");
    private static final Set<String> SUPPORTED_EPOCHS = Set.of("n", "u", "us", "ms", "s", "m", "h");

    private InfluxDbSourceConfigParser() {
    }

    public static InfluxDbSourceConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static InfluxDbSourceConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
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

        String sql = text(safeConfig, "sql", "");
        if (sql.isBlank()) {
            throw error.apply(ValidationError.required("sql"));
        }
        validateSql(sql, error);

        String schemaJson = compactSchema(safeConfig == null ? null : safeConfig.path("schema"), error);
        String epoch = text(safeConfig, "epoch", InfluxDbSourceConfig.DEFAULT_EPOCH);
        validateEpoch(epoch, error);

        int queryTimeoutSeconds = intValue(
                safeConfig, "queryTimeoutSeconds", InfluxDbSourceConfig.DEFAULT_QUERY_TIMEOUT_SECONDS);
        if (queryTimeoutSeconds <= 0) {
            throw error.apply(ValidationError.positive("queryTimeoutSeconds"));
        }
        int connectTimeoutMillis = intValue(
                safeConfig, "connectTimeoutMillis", InfluxDbSourceConfig.DEFAULT_CONNECT_TIMEOUT_MILLIS);
        if (connectTimeoutMillis <= 0) {
            throw error.apply(ValidationError.positive("connectTimeoutMillis"));
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

        long pollIntervalMillis = longValue(
                safeConfig, "pollIntervalMillis", InfluxDbSourceConfig.DEFAULT_POLL_INTERVAL_MILLIS);
        if (pollIntervalMillis <= 0) {
            throw error.apply(ValidationError.positive("pollIntervalMillis"));
        }
        int maxPolls = intValue(safeConfig, "maxPolls", 0);
        if (maxPolls < 0) {
            throw error.apply(ValidationError.nonNegative("maxPolls"));
        }
        String timestampField = text(safeConfig, "timestampField", "");
        if (!timestampField.isBlank()) {
            validateField(timestampField, "timestampField", error);
        }
        List<String> splitMeasurements = strings(safeConfig, "splitMeasurements");
        if (splitMeasurements.isEmpty()) splitMeasurements = strings(safeConfig, "measurements");
        for (String measurement : splitMeasurements) {
            if (!SAFE_MEASUREMENT.matcher(measurement).matches()) {
                throw error.apply(ValidationError.invalidMeasurement(measurement));
            }
        }
        int splitCount = intValue(safeConfig, "splitCount", 1);
        if (splitCount <= 0) throw error.apply(ValidationError.positive("splitCount"));
        String splitField = text(safeConfig, "splitField", timestampField.isBlank() ? cursorField : timestampField);
        String splitStart = text(safeConfig, "splitStart", "");
        String splitEnd = text(safeConfig, "splitEnd", "");
        long splitOverlapMillis = longValue(safeConfig, "splitOverlapMillis", 0L);
        if (splitOverlapMillis < 0) throw error.apply(ValidationError.nonNegative("splitOverlapMillis"));
        if (splitCount > 1 && splitMeasurements.isEmpty() && (splitStart.isBlank() || splitEnd.isBlank())) {
            throw error.apply(ValidationError.splitBoundsRequired());
        }
        if (splitCount > 1 && splitMeasurements.isEmpty() && splitField.isBlank()) {
            throw error.apply(ValidationError.splitFieldRequired());
        }
        if (splitCount > 1 && readMode != ReadMode.FULL) {
            throw error.apply(ValidationError.splitRequiresFullRead());
        }
        if (!splitField.isBlank()) validateField(splitField, "splitField", error);

        String idField = text(safeConfig, "idField", "");
        if (!idField.isBlank()) {
            validateField(idField, "idField", error);
        }
        if (readMode == ReadMode.INCREMENTAL && !idField.isBlank() && sql.contains("${cursor}")) {
            throw error.apply(ValidationError.scalarCursorTemplateDoesNotSupportIdField());
        }
        return new InfluxDbSourceConfig(
                url,
                database,
                sql,
                schemaJson,
                normalizeEpoch(epoch),
                queryTimeoutSeconds,
                connectTimeoutMillis,
                readMode,
                cursorField,
                parseEnum(text(safeConfig, "cursorType", "STRING"), CursorType.class, "cursorType", error),
                text(safeConfig, "initialCursorValue", ""),
                pollIntervalMillis,
                maxPolls,
                idField,
                timestampField,
                text(safeConfig, "username", ""),
                text(safeConfig, "password", ""),
                splitMeasurements,
                splitCount,
                splitField,
                splitStart,
                splitEnd,
                splitOverlapMillis);
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

    private static void validateSql(String sql, ValidationErrorFactory error) {
        String normalized = sql.trim().toLowerCase(Locale.ROOT);
        if (!normalized.startsWith("select ")) {
            throw error.apply(ValidationError.sqlMustBeSelect());
        }
        if (sql.contains(";")) {
            throw error.apply(ValidationError.sqlMustBeSingleStatement());
        }
    }

    private static String compactSchema(JsonNode schema, ValidationErrorFactory error) {
        if (schema == null || schema.isMissingNode() || schema.isNull()) {
            return "{}";
        }
        if (!schema.isObject()) {
            throw error.apply(ValidationError.schemaMustBeObject());
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(schema);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static void validateEpoch(String epoch, ValidationErrorFactory error) {
        if (!SUPPORTED_EPOCHS.contains(normalizeEpoch(epoch))) {
            throw error.apply(ValidationError.unsupportedValue("epoch", epoch));
        }
    }

    private static String normalizeEpoch(String epoch) {
        String normalized = epoch == null || epoch.isBlank()
                ? InfluxDbSourceConfig.DEFAULT_EPOCH
                : epoch.trim().toLowerCase(Locale.ROOT);
        return "us".equals(normalized) ? "u" : normalized;
    }

    private static void validateField(String field, String label, ValidationErrorFactory error) {
        if (!SAFE_FIELD.matcher(field).matches()) {
            throw error.apply(ValidationError.invalidField(label, field));
        }
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

    private static List<String> strings(JsonNode config, String fieldName) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || value.isMissingNode() || value.isNull()) return List.of();
        List<String> values = new ArrayList<>();
        if (value.isArray()) value.forEach(item -> { String v = item.asText("").trim(); if (!v.isBlank()) values.add(v); });
        else for (String item : value.asText("").split(",", -1)) { String v = item.trim(); if (!v.isBlank()) values.add(v); }
        return List.copyOf(values);
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
                    "pipeline.validation.influxDbSource.required",
                    "InfluxDB Source config " + fieldName + " is required.",
                    fieldName);
        }

        static ValidationError invalidUrl(String url) {
            return new ValidationError(
                    "pipeline.validation.influxDbSource.invalidUrl",
                    "InfluxDB Source config url is invalid: " + url,
                    url);
        }

        static ValidationError invalidDatabase(String database) {
            return new ValidationError(
                    "pipeline.validation.influxDbSource.invalidDatabase",
                    "InfluxDB Source config database is invalid: " + database,
                    database);
        }

        static ValidationError sqlMustBeSelect() {
            return new ValidationError(
                    "pipeline.validation.influxDbSource.sqlMustBeSelect",
                    "InfluxDB Source config sql must be a SELECT statement.");
        }

        static ValidationError sqlMustBeSingleStatement() {
            return new ValidationError(
                    "pipeline.validation.influxDbSource.sqlMustBeSingleStatement",
                    "InfluxDB Source config sql must be a single SELECT statement without semicolons.");
        }

        static ValidationError schemaMustBeObject() {
            return new ValidationError(
                    "pipeline.validation.influxDbSource.schemaMustBeObject",
                    "InfluxDB Source config schema must be a JSON object.");
        }

        static ValidationError cursorFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.influxDbSource.cursorFieldRequired",
                    "InfluxDB Source config cursorField is required for INCREMENTAL read mode.");
        }

        static ValidationError scalarCursorTemplateDoesNotSupportIdField() {
            return new ValidationError(
                    "pipeline.validation.influxDbSource.scalarCursorTemplateDoesNotSupportIdField",
                    "InfluxDB Source config sql ${cursor} placeholder cannot be used with idField.");
        }

        static ValidationError invalidField(String label, String field) {
            return new ValidationError(
                    "pipeline.validation.influxDbSource.invalidField",
                    "InfluxDB Source config " + label + " is invalid: " + field,
                    label,
                    field);
        }

        static ValidationError positive(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.influxDbSource.positive",
                    "InfluxDB Source config " + fieldName + " must be greater than 0.",
                    fieldName);
        }

        static ValidationError nonNegative(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.influxDbSource.nonNegative",
                    "InfluxDB Source config " + fieldName + " must be greater than or equal to 0.",
                    fieldName);
        }

        static ValidationError splitBoundsRequired() {
            return new ValidationError("pipeline.validation.influxDbSource.splitBoundsRequired",
                    "InfluxDB Source splitStart and splitEnd are required when splitCount is greater than 1.");
        }

        static ValidationError splitFieldRequired() {
            return new ValidationError("pipeline.validation.influxDbSource.splitFieldRequired",
                    "InfluxDB Source splitField is required for time range splitting.");
        }

        static ValidationError splitRequiresFullRead() {
            return new ValidationError("pipeline.validation.influxDbSource.splitRequiresFullRead",
                    "InfluxDB Source splitting is supported only for FULL read mode.");
        }

        static ValidationError invalidMeasurement(String measurement) {
            return new ValidationError("pipeline.validation.influxDbSource.invalidMeasurement",
                    "InfluxDB Source measurement is invalid: " + measurement, measurement);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.influxDbSource.unsupportedValue",
                    "InfluxDB Source config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
