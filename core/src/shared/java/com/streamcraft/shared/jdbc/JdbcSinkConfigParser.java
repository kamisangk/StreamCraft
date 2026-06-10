package com.streamcraft.shared.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.jdbc.JdbcSinkConfig.WriteMode;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

public final class JdbcSinkConfigParser {

    private static final Pattern SAFE_FIELD = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern SAFE_TABLE_PATH = Pattern.compile("[A-Za-z0-9_.$`\"]+");

    private JdbcSinkConfigParser() {
    }

    public static JdbcSinkConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static JdbcSinkConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        String url = text(safeConfig, "url", "");
        if (url.isBlank()) {
            throw error.apply(ValidationError.required("url"));
        }
        String driver = text(safeConfig, "driver", "");
        if (driver.isBlank()) {
            throw error.apply(ValidationError.required("driver"));
        }
        String tablePath = text(safeConfig, "tablePath", "");
        if (tablePath.isBlank()) {
            throw error.apply(ValidationError.required("tablePath"));
        }
        if (!SAFE_TABLE_PATH.matcher(tablePath).matches()) {
            throw error.apply(ValidationError.invalidTablePath(tablePath));
        }

        WriteMode writeMode = parseEnum(text(safeConfig, "writeMode", "INSERT"), WriteMode.class, "writeMode", error);
        List<String> fields = strings(safeConfig, "fields");
        if (fields.isEmpty()) {
            throw error.apply(ValidationError.fieldsRequired());
        }
        validateFields(fields, "field", error);

        List<String> keyFields = strings(safeConfig, "keyFields");
        if (!keyFields.isEmpty()) {
            validateFields(keyFields, "keyField", error);
            Set<String> fieldSet = Set.copyOf(fields);
            for (String keyField : keyFields) {
                if (!fieldSet.contains(keyField)) {
                    throw error.apply(ValidationError.keyFieldNotInFields(keyField));
                }
            }
        }
        if (writeMode == WriteMode.UPSERT) {
            if (keyFields.isEmpty()) {
                throw error.apply(ValidationError.keyFieldsRequired());
            }
            if (fields.size() == keyFields.size()) {
                throw error.apply(ValidationError.upsertValueFieldsRequired());
            }
        }

        int batchSize = intValue(safeConfig, "batchSize", JdbcSinkConfig.DEFAULT_BATCH_SIZE);
        if (batchSize <= 0) {
            throw error.apply(ValidationError.positive("batchSize"));
        }
        long flushIntervalMillis = longValue(
                safeConfig, "flushIntervalMillis", JdbcSinkConfig.DEFAULT_FLUSH_INTERVAL_MILLIS);
        if (flushIntervalMillis <= 0) {
            throw error.apply(ValidationError.positive("flushIntervalMillis"));
        }

        return new JdbcSinkConfig(
                url,
                driver,
                text(safeConfig, "username", ""),
                text(safeConfig, "password", ""),
                tablePath,
                writeMode,
                fields,
                keyFields,
                batchSize,
                flushIntervalMillis);
    }

    private static void validateFields(List<String> fields, String fieldLabel, ValidationErrorFactory error) {
        Set<String> seen = new LinkedHashSet<>();
        for (String field : fields) {
            if (!SAFE_FIELD.matcher(field).matches()) {
                throw error.apply(ValidationError.invalidField(field));
            }
            if (!seen.add(field)) {
                throw error.apply(ValidationError.duplicateField(fieldLabel, field));
            }
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
                    "pipeline.validation.jdbcSink.required",
                    "JDBC Sink config " + fieldName + " is required.",
                    fieldName);
        }

        static ValidationError fieldsRequired() {
            return new ValidationError(
                    "pipeline.validation.jdbcSink.fieldsRequired",
                    "JDBC Sink config fields must contain at least one field.");
        }

        static ValidationError keyFieldsRequired() {
            return new ValidationError(
                    "pipeline.validation.jdbcSink.keyFieldsRequired",
                    "JDBC Sink config keyFields is required for UPSERT write mode.");
        }

        static ValidationError upsertValueFieldsRequired() {
            return new ValidationError(
                    "pipeline.validation.jdbcSink.upsertValueFieldsRequired",
                    "JDBC Sink config UPSERT mode requires at least one non-key field.");
        }

        static ValidationError invalidTablePath(String tablePath) {
            return new ValidationError(
                    "pipeline.validation.jdbcSink.invalidTablePath",
                    "JDBC Sink config tablePath is invalid: " + tablePath,
                    tablePath);
        }

        static ValidationError invalidField(String field) {
            return new ValidationError(
                    "pipeline.validation.jdbcSink.invalidField",
                    "JDBC Sink config field is invalid: " + field,
                    field);
        }

        static ValidationError duplicateField(String fieldLabel, String field) {
            return new ValidationError(
                    "pipeline.validation.jdbcSink.duplicateField",
                    "JDBC Sink config " + fieldLabel + " must be unique: " + field,
                    fieldLabel,
                    field);
        }

        static ValidationError keyFieldNotInFields(String keyField) {
            return new ValidationError(
                    "pipeline.validation.jdbcSink.keyFieldNotInFields",
                    "JDBC Sink config keyField must be included in fields: " + keyField,
                    keyField);
        }

        static ValidationError positive(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.jdbcSink.positive",
                    "JDBC Sink config " + fieldName + " must be greater than 0.",
                    fieldName);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.jdbcSink.unsupportedValue",
                    "JDBC Sink config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
