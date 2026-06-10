package com.streamcraft.shared.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.jdbc.JdbcSourceConfig.CursorType;
import com.streamcraft.shared.jdbc.JdbcSourceConfig.ReadMode;
import java.util.Locale;
import java.util.function.Function;
import java.util.regex.Pattern;

public final class JdbcSourceConfigParser {

    private static final Pattern SAFE_CURSOR_FIELD = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern SAFE_TABLE_PATH = Pattern.compile("[A-Za-z0-9_.$`\"]+");

    private JdbcSourceConfigParser() {
    }

    public static JdbcSourceConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static JdbcSourceConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        String url = text(safeConfig, "url", "");
        if (url.isBlank()) {
            throw error.apply(ValidationError.required("url"));
        }
        String driver = text(safeConfig, "driver", "");
        if (driver.isBlank()) {
            throw error.apply(ValidationError.required("driver"));
        }

        String query = text(safeConfig, "query", "");
        String tablePath = text(safeConfig, "tablePath", "");
        if (query.isBlank() && tablePath.isBlank()) {
            throw error.apply(ValidationError.queryOrTableRequired());
        }
        if (!query.isBlank()) {
            validateQuery(query, error);
        }
        if (!tablePath.isBlank() && !SAFE_TABLE_PATH.matcher(tablePath).matches()) {
            throw error.apply(ValidationError.invalidTablePath(tablePath));
        }

        ReadMode readMode = parseEnum(text(safeConfig, "readMode", "FULL"), ReadMode.class, "readMode", error);
        String cursorField = text(safeConfig, "cursorField", "");
        if (readMode == ReadMode.INCREMENTAL) {
            if (cursorField.isBlank()) {
                throw error.apply(ValidationError.cursorFieldRequired());
            }
            validateCursorField(cursorField, error);
        } else if (!cursorField.isBlank()) {
            validateCursorField(cursorField, error);
        }

        long pollIntervalMillis = longValue(
                safeConfig, "pollIntervalMillis", JdbcSourceConfig.DEFAULT_POLL_INTERVAL_MILLIS);
        if (pollIntervalMillis <= 0) {
            throw error.apply(ValidationError.positive("pollIntervalMillis"));
        }
        int fetchSize = intValue(safeConfig, "fetchSize", JdbcSourceConfig.DEFAULT_FETCH_SIZE);
        if (fetchSize <= 0) {
            throw error.apply(ValidationError.positive("fetchSize"));
        }
        int maxPolls = intValue(safeConfig, "maxPolls", 0);
        if (maxPolls < 0) {
            throw error.apply(ValidationError.nonNegative("maxPolls"));
        }

        return new JdbcSourceConfig(
                url,
                driver,
                text(safeConfig, "username", ""),
                text(safeConfig, "password", ""),
                query,
                tablePath,
                readMode,
                cursorField,
                parseEnum(text(safeConfig, "cursorType", "STRING"), CursorType.class, "cursorType", error),
                text(safeConfig, "initialCursorValue", ""),
                pollIntervalMillis,
                fetchSize,
                maxPolls,
                text(safeConfig, "idField", ""),
                text(safeConfig, "timestampField", ""));
    }

    private static void validateQuery(String query, ValidationErrorFactory error) {
        String normalizedQuery = query.trim().toLowerCase(Locale.ROOT);
        if (!normalizedQuery.startsWith("select ")) {
            throw error.apply(ValidationError.queryMustBeSelect());
        }
        if (query.contains(";")) {
            throw error.apply(ValidationError.queryMustBeSingleStatement());
        }
    }

    private static void validateCursorField(String cursorField, ValidationErrorFactory error) {
        if (!SAFE_CURSOR_FIELD.matcher(cursorField).matches()) {
            throw error.apply(ValidationError.invalidCursorField(cursorField));
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
                    "pipeline.validation.jdbcSource.required",
                    "JDBC Source config " + fieldName + " is required.",
                    fieldName);
        }

        static ValidationError queryOrTableRequired() {
            return new ValidationError(
                    "pipeline.validation.jdbcSource.queryOrTableRequired",
                    "JDBC Source config query or tablePath is required.");
        }

        static ValidationError queryMustBeSelect() {
            return new ValidationError(
                    "pipeline.validation.jdbcSource.queryMustBeSelect",
                    "JDBC Source config query must be a SELECT statement.");
        }

        static ValidationError queryMustBeSingleStatement() {
            return new ValidationError(
                    "pipeline.validation.jdbcSource.queryMustBeSingleStatement",
                    "JDBC Source config query must be a single SELECT statement without semicolons.");
        }

        static ValidationError invalidTablePath(String tablePath) {
            return new ValidationError(
                    "pipeline.validation.jdbcSource.invalidTablePath",
                    "JDBC Source config tablePath is invalid: " + tablePath,
                    tablePath);
        }

        static ValidationError cursorFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.jdbcSource.cursorFieldRequired",
                    "JDBC Source config cursorField is required for INCREMENTAL read mode.");
        }

        static ValidationError invalidCursorField(String cursorField) {
            return new ValidationError(
                    "pipeline.validation.jdbcSource.invalidCursorField",
                    "JDBC Source config cursorField is invalid: " + cursorField,
                    cursorField);
        }

        static ValidationError positive(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.jdbcSource.positive",
                    "JDBC Source config " + fieldName + " must be greater than 0.",
                    fieldName);
        }

        static ValidationError nonNegative(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.jdbcSource.nonNegative",
                    "JDBC Source config " + fieldName + " must be greater than or equal to 0.",
                    fieldName);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.jdbcSource.unsupportedValue",
                    "JDBC Source config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
