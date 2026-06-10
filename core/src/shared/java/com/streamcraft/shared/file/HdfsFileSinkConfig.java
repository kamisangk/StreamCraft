package com.streamcraft.shared.file;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.List;

public record HdfsFileSinkConfig(
        String defaultFs,
        String path,
        String tmpPath,
        HdfsFileFormatType fileFormatType,
        List<String> sinkColumns,
        List<String> partitionBy,
        String partitionDirExpression,
        boolean partitionFieldWriteInFile,
        boolean customFilename,
        String fileNameExpression,
        String filenameTimeFormat,
        int batchSize,
        long flushIntervalMillis,
        String fieldDelimiter,
        String rowDelimiter,
        boolean csvUseHeaderLine,
        String encoding,
        String compressCodec,
        String hdfsSitePath,
        String kerberosPrincipal,
        String kerberosKeytabPath) implements Serializable {

    public static final int DEFAULT_BATCH_SIZE = 1_000;
    public static final long DEFAULT_FLUSH_INTERVAL_MILLIS = 5_000L;
    public static final String DEFAULT_TMP_PATH = "/tmp/streamcraft/hdfs-file";
    public static final String DEFAULT_FIELD_DELIMITER = "\001";
    public static final String DEFAULT_ROW_DELIMITER = "\n";
    public static final String DEFAULT_ENCODING = "UTF-8";
    public static final String DEFAULT_COMPRESS_CODEC = "none";
    public static final String DEFAULT_FILE_NAME_EXPRESSION = "part-${now}";
    public static final String DEFAULT_FILENAME_TIME_FORMAT = "yyyyMMddHHmmss";

    public HdfsFileSinkConfig {
        defaultFs = defaultFs == null ? "" : defaultFs.trim();
        path = path == null ? "" : path.trim();
        tmpPath = tmpPath == null || tmpPath.isBlank() ? DEFAULT_TMP_PATH : tmpPath.trim();
        sinkColumns = sinkColumns == null ? List.of() : List.copyOf(sinkColumns);
        partitionBy = partitionBy == null ? List.of() : List.copyOf(partitionBy);
        partitionDirExpression = partitionDirExpression == null ? "" : partitionDirExpression.trim();
        fileNameExpression = fileNameExpression == null || fileNameExpression.isBlank()
                ? DEFAULT_FILE_NAME_EXPRESSION
                : fileNameExpression.trim();
        filenameTimeFormat = filenameTimeFormat == null || filenameTimeFormat.isBlank()
                ? DEFAULT_FILENAME_TIME_FORMAT
                : filenameTimeFormat.trim();
        fieldDelimiter = fieldDelimiter == null ? DEFAULT_FIELD_DELIMITER : fieldDelimiter;
        rowDelimiter = rowDelimiter == null ? DEFAULT_ROW_DELIMITER : rowDelimiter;
        encoding = encoding == null || encoding.isBlank() ? DEFAULT_ENCODING : encoding.trim();
        compressCodec = compressCodec == null || compressCodec.isBlank() ? DEFAULT_COMPRESS_CODEC : compressCodec.trim();
        hdfsSitePath = hdfsSitePath == null ? "" : hdfsSitePath.trim();
        kerberosPrincipal = kerberosPrincipal == null ? "" : kerberosPrincipal.trim();
        kerberosKeytabPath = kerberosKeytabPath == null ? "" : kerberosKeytabPath.trim();
    }

    public List<String> writableColumns() {
        if (partitionFieldWriteInFile || partitionBy.isEmpty()) {
            return sinkColumns;
        }
        LinkedHashSet<String> columns = new LinkedHashSet<>(sinkColumns);
        partitionBy.forEach(columns::remove);
        return List.copyOf(columns);
    }
}
