package com.streamcraft.shared.file;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

public final class HdfsFileSinkConfigParser {

    private static final Pattern SAFE_FIELD = Pattern.compile("[A-Za-z_][A-Za-z0-9_.-]*");
    private static final Set<String> SUPPORTED_DEFAULT_FS_SCHEMES = Set.of("hdfs", "viewfs", "file");

    private HdfsFileSinkConfigParser() {
    }

    public static HdfsFileSinkConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static HdfsFileSinkConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
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

        String tmpPath = text(safeConfig, "tmp_path", HdfsFileSinkConfig.DEFAULT_TMP_PATH);
        if (tmpPath.isBlank()) {
            tmpPath = HdfsFileSinkConfig.DEFAULT_TMP_PATH;
        }
        validatePath(tmpPath, "tmp_path", error);

        List<String> sinkColumns = strings(safeConfig, "sink_columns");
        validateFields(sinkColumns, "sink_columns", error);
        List<String> partitionBy = strings(safeConfig, "partition_by");
        validateFields(partitionBy, "partition_by", error);
        String partitionDirExpression = text(safeConfig, "partition_dir_expression", "");
        boolean partitionFieldWriteInFile = bool(safeConfig, "is_partition_field_write_in_file", true);
        boolean customFilename = bool(safeConfig, "custom_filename", false);
        String fileNameExpression = text(
                safeConfig, "file_name_expression", HdfsFileSinkConfig.DEFAULT_FILE_NAME_EXPRESSION);
        if (fileNameExpression.isBlank()) {
            fileNameExpression = HdfsFileSinkConfig.DEFAULT_FILE_NAME_EXPRESSION;
        }
        String filenameTimeFormat = text(
                safeConfig, "filename_time_format", HdfsFileSinkConfig.DEFAULT_FILENAME_TIME_FORMAT);
        if (filenameTimeFormat.isBlank()) {
            filenameTimeFormat = HdfsFileSinkConfig.DEFAULT_FILENAME_TIME_FORMAT;
        }

        int batchSize = intValue(safeConfig, "batch_size", HdfsFileSinkConfig.DEFAULT_BATCH_SIZE);
        if (batchSize <= 0) {
            throw error.apply(ValidationError.positive("batchSize"));
        }
        long flushIntervalMillis = longValue(
                safeConfig, "flushIntervalMillis", HdfsFileSinkConfig.DEFAULT_FLUSH_INTERVAL_MILLIS);
        if (flushIntervalMillis <= 0) {
            throw error.apply(ValidationError.positive("flushIntervalMillis"));
        }
        String fieldDelimiter = unescape(rawText(
                safeConfig, "field_delimiter", HdfsFileSinkConfig.DEFAULT_FIELD_DELIMITER));
        String rowDelimiter = unescape(rawText(safeConfig, "row_delimiter", HdfsFileSinkConfig.DEFAULT_ROW_DELIMITER));
        boolean csvUseHeaderLine = bool(safeConfig, "csv_use_header_line", false);
        String encoding = text(safeConfig, "encoding", HdfsFileSinkConfig.DEFAULT_ENCODING);
        validateEncoding(encoding, error);
        String compressCodec = text(safeConfig, "compress_codec", HdfsFileSinkConfig.DEFAULT_COMPRESS_CODEC);
        if (compressCodec.isBlank()) {
            compressCodec = HdfsFileSinkConfig.DEFAULT_COMPRESS_CODEC;
        }

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

        return new HdfsFileSinkConfig(
                defaultFs,
                path,
                tmpPath,
                fileFormatType,
                sinkColumns,
                partitionBy,
                partitionDirExpression,
                partitionFieldWriteInFile,
                customFilename,
                fileNameExpression,
                filenameTimeFormat,
                batchSize,
                flushIntervalMillis,
                fieldDelimiter,
                rowDelimiter,
                csvUseHeaderLine,
                encoding,
                compressCodec,
                hdfsSitePath,
                kerberosPrincipal,
                kerberosKeytabPath);
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
            if (!SAFE_FIELD.matcher(field).matches()) {
                throw error.apply(ValidationError.invalidField(label, field));
            }
            if (!seen.add(field)) {
                throw error.apply(ValidationError.duplicateField(label, field));
            }
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
                    "pipeline.validation.hdfsFileSink.required",
                    "HDFS File Sink config " + fieldName + " is required.",
                    fieldName);
        }

        static ValidationError invalidUri(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSink.invalidUri",
                    "HDFS File Sink config " + fieldName + " is invalid: " + value,
                    fieldName,
                    value);
        }

        static ValidationError invalidPath(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSink.invalidPath",
                    "HDFS File Sink config " + fieldName + " is invalid: " + value,
                    fieldName,
                    value);
        }

        static ValidationError invalidField(String label, String field) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSink.invalidField",
                    "HDFS File Sink config " + label + " is invalid: " + field,
                    label,
                    field);
        }

        static ValidationError duplicateField(String label, String field) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSink.duplicateField",
                    "HDFS File Sink config " + label + " must be unique: " + field,
                    label,
                    field);
        }

        static ValidationError positive(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSink.positive",
                    "HDFS File Sink config " + fieldName + " must be greater than 0.",
                    fieldName);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.hdfsFileSink.unsupportedValue",
                    "HDFS File Sink config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
