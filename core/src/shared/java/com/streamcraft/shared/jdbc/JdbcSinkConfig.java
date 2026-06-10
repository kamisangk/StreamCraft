package com.streamcraft.shared.jdbc;

import java.io.Serializable;
import java.util.List;

public record JdbcSinkConfig(
        String url,
        String driver,
        String username,
        String password,
        String tablePath,
        WriteMode writeMode,
        List<String> fields,
        List<String> keyFields,
        int batchSize,
        long flushIntervalMillis) implements Serializable {

    public static final int DEFAULT_BATCH_SIZE = 500;
    public static final long DEFAULT_FLUSH_INTERVAL_MILLIS = 5_000L;

    public JdbcSinkConfig {
        url = clean(url);
        driver = clean(driver);
        username = clean(username);
        password = password == null ? "" : password;
        tablePath = clean(tablePath);
        writeMode = writeMode == null ? WriteMode.INSERT : writeMode;
        fields = fields == null ? List.of() : List.copyOf(fields);
        keyFields = keyFields == null ? List.of() : List.copyOf(keyFields);
        batchSize = batchSize <= 0 ? DEFAULT_BATCH_SIZE : batchSize;
        flushIntervalMillis = flushIntervalMillis <= 0 ? DEFAULT_FLUSH_INTERVAL_MILLIS : flushIntervalMillis;
    }

    public List<String> nonKeyFields() {
        return fields.stream()
                .filter(field -> !keyFields.contains(field))
                .toList();
    }

    private static String clean(String value) {
        return value == null ? "" : value.trim();
    }

    public enum WriteMode {
        INSERT,
        UPSERT
    }
}
