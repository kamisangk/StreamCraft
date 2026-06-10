package com.streamcraft.shared.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public record HdfsFileSourceConfig(
        String defaultFs,
        String path,
        HdfsFileFormatType fileFormatType,
        String schemaJson,
        List<String> readColumns,
        String fieldDelimiter,
        String rowDelimiter,
        int skipHeaderRowNumber,
        boolean csvUseHeaderLine,
        String encoding,
        String compressCodec,
        boolean parsePartitionFromPath,
        String fileFilterPattern,
        ReadMode readMode,
        long pollIntervalMillis,
        int maxPolls,
        String idField,
        String timestampField,
        String hdfsSitePath,
        String kerberosPrincipal,
        String kerberosKeytabPath) implements Serializable {

    public static final long DEFAULT_POLL_INTERVAL_MILLIS = 5_000L;
    public static final String DEFAULT_FIELD_DELIMITER = "\001";
    public static final String DEFAULT_ROW_DELIMITER = "\n";
    public static final String DEFAULT_ENCODING = "UTF-8";
    public static final String DEFAULT_COMPRESS_CODEC = "none";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public HdfsFileSourceConfig {
        defaultFs = defaultFs == null ? "" : defaultFs.trim();
        path = path == null ? "" : path.trim();
        schemaJson = schemaJson == null || schemaJson.isBlank() ? "{}" : schemaJson;
        readColumns = readColumns == null ? List.of() : List.copyOf(readColumns);
        fieldDelimiter = fieldDelimiter == null ? DEFAULT_FIELD_DELIMITER : fieldDelimiter;
        rowDelimiter = rowDelimiter == null ? DEFAULT_ROW_DELIMITER : rowDelimiter;
        encoding = encoding == null || encoding.isBlank() ? DEFAULT_ENCODING : encoding.trim();
        compressCodec = compressCodec == null || compressCodec.isBlank() ? DEFAULT_COMPRESS_CODEC : compressCodec.trim();
        fileFilterPattern = fileFilterPattern == null ? "" : fileFilterPattern.trim();
        readMode = readMode == null ? ReadMode.FULL : readMode;
        idField = idField == null ? "" : idField.trim();
        timestampField = timestampField == null ? "" : timestampField.trim();
        hdfsSitePath = hdfsSitePath == null ? "" : hdfsSitePath.trim();
        kerberosPrincipal = kerberosPrincipal == null ? "" : kerberosPrincipal.trim();
        kerberosKeytabPath = kerberosKeytabPath == null ? "" : kerberosKeytabPath.trim();
    }

    public List<String> fieldNames() {
        List<String> schemaFields = schemaFieldNames();
        return schemaFields.isEmpty() ? readColumns : schemaFields;
    }

    private List<String> schemaFieldNames() {
        try {
            JsonNode schema = OBJECT_MAPPER.readTree(schemaJson);
            JsonNode fields = schema.path("fields");
            if (!fields.isObject()) {
                fields = schema;
            }
            if (!fields.isObject()) {
                return List.of();
            }
            List<String> names = new ArrayList<>();
            fields.fieldNames().forEachRemaining(names::add);
            return List.copyOf(names);
        } catch (Exception exception) {
            throw new IllegalStateException("HDFS File Source schemaJson is not valid JSON.", exception);
        }
    }

    public enum ReadMode {
        FULL,
        INCREMENTAL
    }
}
