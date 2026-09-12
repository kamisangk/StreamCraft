package com.streamcraft.shared.jdbc;

import java.io.Serializable;

public record JdbcSourceConfig(
        String url,
        String driver,
        String username,
        String password,
        String query,
        String tablePath,
        ReadMode readMode,
        String cursorField,
        CursorType cursorType,
        String initialCursorValue,
        long pollIntervalMillis,
        int fetchSize,
        int maxPolls,
        String idField,
        String timestampField,
        int splitCount,
        String splitColumn,
        String splitLowerBound,
        String splitUpperBound) implements Serializable {

    public static final long DEFAULT_POLL_INTERVAL_MILLIS = 5_000L;
    public static final int DEFAULT_FETCH_SIZE = 1_000;

    public JdbcSourceConfig {
        url = clean(url);
        driver = clean(driver);
        username = clean(username);
        password = password == null ? "" : password;
        query = clean(query);
        tablePath = clean(tablePath);
        readMode = readMode == null ? ReadMode.FULL : readMode;
        cursorField = clean(cursorField);
        cursorType = cursorType == null ? CursorType.STRING : cursorType;
        initialCursorValue = clean(initialCursorValue);
        pollIntervalMillis = pollIntervalMillis <= 0 ? DEFAULT_POLL_INTERVAL_MILLIS : pollIntervalMillis;
        fetchSize = fetchSize <= 0 ? DEFAULT_FETCH_SIZE : fetchSize;
        maxPolls = Math.max(0, maxPolls);
        idField = clean(idField);
        timestampField = clean(timestampField);
        splitCount = splitCount <= 0 ? 1 : splitCount;
        splitColumn = clean(splitColumn);
        splitLowerBound = clean(splitLowerBound);
        splitUpperBound = clean(splitUpperBound);
    }

    /** Compatibility constructor for configurations created before range splitting was introduced. */
    public JdbcSourceConfig(
            String url,
            String driver,
            String username,
            String password,
            String query,
            String tablePath,
            ReadMode readMode,
            String cursorField,
            CursorType cursorType,
            String initialCursorValue,
            long pollIntervalMillis,
            int fetchSize,
            int maxPolls,
            String idField,
            String timestampField) {
        this(url, driver, username, password, query, tablePath, readMode, cursorField, cursorType,
                initialCursorValue, pollIntervalMillis, fetchSize, maxPolls, idField, timestampField,
                1, "", "", "");
    }

    public boolean rangeSplittingEnabled() {
        return splitCount > 1 && !splitColumn.isBlank()
                && !splitLowerBound.isBlank() && !splitUpperBound.isBlank()
                && readMode == ReadMode.FULL
                && (cursorType == CursorType.LONG || cursorType == CursorType.DOUBLE);
    }

    public String resolvedQuery() {
        if (!query.isBlank()) {
            return query;
        }
        return "select * from " + tablePath;
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
