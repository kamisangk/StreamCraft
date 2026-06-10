package com.streamcraft.shared.influxdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;

public record InfluxDbSourceConfig(
        String url,
        String database,
        String sql,
        String schemaJson,
        String epoch,
        int queryTimeoutSeconds,
        int connectTimeoutMillis,
        ReadMode readMode,
        String cursorField,
        CursorType cursorType,
        String initialCursorValue,
        long pollIntervalMillis,
        int maxPolls,
        String idField,
        String timestampField,
        String username,
        String password) implements Serializable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 30;
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 10_000;
    public static final long DEFAULT_POLL_INTERVAL_MILLIS = 5_000L;
    public static final String DEFAULT_EPOCH = "ms";

    public InfluxDbSourceConfig {
        url = clean(url);
        database = clean(database);
        sql = clean(sql);
        schemaJson = clean(schemaJson);
        if (schemaJson.isBlank()) {
            schemaJson = "{}";
        }
        epoch = clean(epoch);
        if (epoch.isBlank()) {
            epoch = DEFAULT_EPOCH;
        }
        queryTimeoutSeconds = queryTimeoutSeconds <= 0 ? DEFAULT_QUERY_TIMEOUT_SECONDS : queryTimeoutSeconds;
        connectTimeoutMillis = connectTimeoutMillis <= 0 ? DEFAULT_CONNECT_TIMEOUT_MILLIS : connectTimeoutMillis;
        readMode = readMode == null ? ReadMode.FULL : readMode;
        cursorField = clean(cursorField);
        cursorType = cursorType == null ? CursorType.STRING : cursorType;
        initialCursorValue = clean(initialCursorValue);
        pollIntervalMillis = pollIntervalMillis <= 0 ? DEFAULT_POLL_INTERVAL_MILLIS : pollIntervalMillis;
        maxPolls = Math.max(0, maxPolls);
        idField = clean(idField);
        timestampField = clean(timestampField);
        username = clean(username);
        password = password == null ? "" : password;
    }

    public JsonNode schema() {
        try {
            return OBJECT_MAPPER.readTree(schemaJson);
        } catch (Exception exception) {
            throw new IllegalStateException("InfluxDB Source schemaJson is not valid JSON.", exception);
        }
    }

    private static String clean(String value) {
        return value == null ? "" : value.trim();
    }

    public enum ReadMode {
        FULL,
        INCREMENTAL
    }

    public enum CursorType {
        STRING,
        LONG,
        DOUBLE,
        TIMESTAMP
    }
}
