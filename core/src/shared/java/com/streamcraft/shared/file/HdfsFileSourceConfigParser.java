package com.streamcraft.shared.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.shared.file.HdfsFileSourceConfig.ReadMode;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public final class HdfsFileSourceConfigParser {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern SAFE_FIELD = Pattern.compile("[A-Za-z_][A-Za-z0-9_.-]*");
    private static final Set<String> SUPPORTED_DEFAULT_FS_SCHEMES = Set.of("hdfs", "viewfs", "file");

    private HdfsFileSourceConfigParser() {
    }

    public static HdfsFileSourceConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static HdfsFileSourceConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        String defaultFs = text(safeConfig, "fs.defaultFS", "");
        if (defaultFs.isBlank()) {
            throw error.apply(ValidationError.required("fs.defaultFS"));
        }
        validateDefaultFs(defaultFs, error);

        String path = text(safeConfig, "path", "");
        if (path.isBlank()) {
            throw error.apply(ValidationError.required("path"));
        }
        validatePath(path, "path", error);

        String format = text(safeConfig, "file_format_type", "");
        if (format.isBlank()) {
            throw error.apply(ValidationError.required("file_format_type"));
        }
        HdfsFileFormatType fileFormatType = parseEnum(format, HdfsFileFormatType.class, "file_format_type", error);

        String schemaJson = compactSchema(safeConfig == null ? null : safeConfig.path("schema"), error);
        List<String> readColumns = strings(safeConfig, "read_columns");
        validateFields(readColumns, "read_columns", error);

        String fieldDelimiter = unescape(rawText(
                safeConfig, "field_delimiter", HdfsFileSourceConfig.DEFAULT_FIELD_DELIMITER));
        String rowDelimiter = unescape(rawText(
                safeConfig, "row_delimiter", HdfsFileSourceConfig.DEFAULT_ROW_DELIMITER));
        int skipHeaderRowNumber = intValue(safeConfig, "skip_header_row_number", 0);
        if (skipHeaderRowNumber < 0) {
            throw error.apply(ValidationError.nonNegative("skip_header_row_number"));
        }
        boolean csvUseHeaderLine = bool(safeConfig, "csv_use_header_line", false);
        String encoding = text(safeConfig, "encoding", HdfsFileSourceConfig.DEFAULT_ENCODING);
        validateEncoding(encoding, error);
        String compressCodec = text(safeConfig, "compress_codec", HdfsFileSourceConfig.DEFAULT_COMPRESS_CODEC);
        if (compressCodec.isBlank()) {
            compressCodec = HdfsFileSourceConfig.DEFAULT_COMPRESS_CODEC;
        }
        boolean parsePartitionFromPath = bool(safeConfig, "parse_partition_from_path", false);
        String fileFilterPattern = text(safeConfig, "file_filter_pattern", "");
        validateRegex(fileFilterPattern, "file_filter_pattern", error);

        ReadMode readMode = parseEnum(text(safeConfig, "readMode", "FULL"), ReadMode.class, "readMode", error);
        long pollIntervalMillis = longValue(
                safeConfig, "pollIntervalMillis", HdfsFileSourceConfig.DEFAULT_POLL_INTERVAL_MILLIS);
        if (pollIntervalMillis <= 0) {
            throw error.apply(ValidationError.positive("pollIntervalMillis"));
        }
        int maxPolls = intValue(safeConfig, "maxPolls", 0);
        if (maxPolls < 0) {
            throw error.apply(ValidationError.nonNegative("maxPolls"));
        }

        String idField = text(safeConfig, "idField", "");
        validateOptionalField(idField, "idField", error);
        String timestampField = text(safeConfig, "timestampField", "");
        validateOptionalField(timestampField, "timestampField", error);
        String hdfsSitePath = text(safeConfig, "hdfs_site_path", "");
        validateOptionalPath(hdfsSitePath, "hdfs_site_path", error);
        String kerberosPrincipal = text(safeConfig, "kerberos_principal", "");
        String kerberosKeytabPath = text(safeConfig, "kerberos_keytab_path", "");
        validateOptionalPath(kerberosKeytabPath, "kerberos_keytab_path", error);
        if (!kerberosPrincipal.isBlank() && kerberosKeytabPath.isBlank()) {
            throw error.apply(ValidationError.required("kerberos_keytab_path"));
        }
        if (!kerberosKeytabPath.isBlank() && kerberosPrincipal.isBlank()) {
            throw error.apply(ValidationError.required("kerberos_principal"));
        }

        return new HdfsFileSourceConfig(
                defaultFs,
                path,
                fileFormatType,
                schemaJson,
                readColumns,
                fieldDelimiter,
                rowDelimiter,
                skipHeaderRowNumber,
                csvUseHeaderLine,
                encoding,
                compressCodec,
                parsePartitionFromPath,
                fileFilterPattern,
                readMode,
                pollIntervalMillis,
                maxPolls,
                idField,
                timestampField,
                hdfsSitePath,
                kerberosPrincipal,
                kerberosKeytabPath);
    }

    private static String compactSchema(JsonNode schema, ValidationErrorFactory error) {
        if (schema == null || schema.isMissingNode() || schema.isNull()) {
            return "{}";
        }
        if (!schema.isObject()) {
            throw error.apply(ValidationError.schemaMustBeObject());
        }
        try {
            validateFields(schemaFieldNames(schema), "schema.fields", error);
            return OBJECT_MAPPER.writeValueAsString(schema);
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static List<String> schemaFieldNames(JsonNode schema) {
        JsonNode fields = schema.path("fields");
        if (!fields.isObject()) {
            fields = schema;
        }
        if (!fields.isObject()) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        fields.fieldNames().forEachRemaining(names::add);
        return names;
    }

    private static void validateDefaultFs(String defaultFs, ValidationErrorFactory error) {
        try {
            URI uri = URI.create(defaultFs);
            if (uri.getScheme() == null
                    || !SUPPORTED_DEFAULT_FS_SCHEMES.contains(uri.getScheme().toLowerCase(Locale.ROOT))) {
                throw error.apply(ValidationError.invalidUri("fs.defaultFS", defaultFs));
            }
        } catch (IllegalArgumentException exception) {
            throw error.apply(ValidationError.invalidUri("fs.defaultFS", defaultFs));
        }
    }

    private static void validatePath(String path, String fieldName, ValidationErrorFactory error) {
        if (path.contains("\n") || path.contains("\r")) {
            throw error.apply(ValidationError.invalidPath(fieldName, path));
        }
    }

    private static void validateOptionalPath(String path, String fieldName, ValidationErrorFactory error) {
        if (!path.isBlank()) {
            validatePath(path, fieldName, error);
        }
    }

    private static void validateFields(List<String> fields, String label, ValidationErrorFactory error) {
        Set<String> seen = new LinkedHashSet<>();
        for (String field : fields) {
            validateOptionalField(field, label, error);
            if (!seen.add(field)) {
                throw error.apply(ValidationError.duplicateField(label, field));
            }
        }
    }

    private static void validateOptionalField(String field, String label, ValidationErrorFactory error) {
        if (!field.isBlank() && !SAFE_FIELD.matcher(field).matches()) {
            throw error.apply(ValidationError.invalidField(label, field));
        }
    }

    private static void validateRegex(String pattern, String fieldName, ValidationErrorFactory error) {
        if (pattern.isBlank()) {
            return;
        }
        try {
            Pattern.compile(pattern);
        } catch (PatternSyntaxException exception) {
            throw error.apply(ValidationError.invalidField(fieldName, pattern));
        }
    }

    private static void validateEncoding(String encoding, ValidationErrorFactory error) {
        try {
            Charset.forName(encoding);
        } catch (Exception exception) {
            throw error.apply(ValidationError.unsupportedValue("encoding", encoding));
        }
    }

    private static String unescape(String value) {
        return value
                .replace("\\001", "\001")
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\t", "\t");
    }

    private static String text(JsonNode config, String fieldName, String fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        String text = value == null || value.isMissingNode() || value.isNull() ? fallback : value.asText(fallback);
        return text == null ? "" : text.trim();
    }

    private static String rawText(JsonNode config, String fieldName, String fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        String text = value == null || value.isMissingNode() || value.isNull() ? fallback : value.asText(fallback);
        return text == null ? "" : text;
    }

    private static long longValue(JsonNode config, String fieldName, long fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        return value == null || value.isMissingNode() || value.isNull() ? fallback : value.asLong(fallback);
    }

    private static int intValue(JsonNode config, String fieldName, int fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        return value == null || value.isMissingNode() || value.isNull() ? fallback : value.asInt(fallback);
    }

    private static boolean bool(JsonNode config, String fieldName, boolean fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        return value == null || value.isMissingNode() || value.isNull() ? fallback : value.asBoolean(fallback);
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
                    "pipeline.validation.hdfsFileSource.required",
                    "HDFS File Source config " + fieldName + " is required.",
                    fieldName);
        }

        static ValidationError invalidUri(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSource.invalidUri",
                    "HDFS File Source config " + fieldName + " is invalid: " + value,
                    fieldName,
                    value);
        }

        static ValidationError invalidPath(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSource.invalidPath",
                    "HDFS File Source config " + fieldName + " is invalid: " + value,
                    fieldName,
                    value);
        }

        static ValidationError schemaMustBeObject() {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSource.schemaMustBeObject",
                    "HDFS File Source config schema must be a JSON object.");
        }

        static ValidationError invalidField(String label, String field) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSource.invalidField",
                    "HDFS File Source config " + label + " is invalid: " + field,
                    label,
                    field);
        }

        static ValidationError duplicateField(String label, String field) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSource.duplicateField",
                    "HDFS File Source config " + label + " must be unique: " + field,
                    label,
                    field);
        }

        static ValidationError positive(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSource.positive",
                    "HDFS File Source config " + fieldName + " must be greater than 0.",
                    fieldName);
        }

        static ValidationError nonNegative(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSource.nonNegative",
                    "HDFS File Source config " + fieldName + " must be greater than or equal to 0.",
                    fieldName);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSource.unsupportedValue",
                    "HDFS File Source config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
