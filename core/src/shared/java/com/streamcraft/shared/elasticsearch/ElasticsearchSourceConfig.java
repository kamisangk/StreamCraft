package com.streamcraft.shared.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.List;

public record ElasticsearchSourceConfig(
        List<String> hosts,
        String index,
        List<String> sourceFields,
        String queryJson,
        ReadMode readMode,
        String cursorField,
        CursorType cursorType,
        String initialCursorValue,
        long pollIntervalMillis,
        int scrollSize,
        String scrollTime,
        int maxPolls,
        String idField,
        String timestampField,
        AuthType authType,
        String username,
        String password,
        String apiKey) implements Serializable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final long DEFAULT_POLL_INTERVAL_MILLIS = 5_000L;
    public static final int DEFAULT_SCROLL_SIZE = 100;
    public static final String DEFAULT_SCROLL_TIME = "1m";

    public ElasticsearchSourceConfig {
        hosts = hosts == null ? List.of() : List.copyOf(hosts);
        index = clean(index);
        sourceFields = sourceFields == null ? List.of() : List.copyOf(sourceFields);
        queryJson = clean(queryJson);
        if (queryJson.isBlank()) {
            queryJson = "{}";
        }
        readMode = readMode == null ? ReadMode.FULL : readMode;
        cursorField = clean(cursorField);
        cursorType = cursorType == null ? CursorType.STRING : cursorType;
        initialCursorValue = clean(initialCursorValue);
        pollIntervalMillis = pollIntervalMillis <= 0 ? DEFAULT_POLL_INTERVAL_MILLIS : pollIntervalMillis;
        scrollSize = scrollSize <= 0 ? DEFAULT_SCROLL_SIZE : scrollSize;
        scrollTime = clean(scrollTime);
        if (scrollTime.isBlank()) {
            scrollTime = DEFAULT_SCROLL_TIME;
        }
        maxPolls = Math.max(0, maxPolls);
        idField = clean(idField);
        timestampField = clean(timestampField);
        authType = authType == null ? AuthType.NONE : authType;
        username = clean(username);
        password = password == null ? "" : password;
        apiKey = clean(apiKey);
    }

    public JsonNode query() {
        try {
            return OBJECT_MAPPER.readTree(queryJson);
        } catch (Exception exception) {
            throw new IllegalStateException("Elasticsearch Source queryJson is not valid JSON.", exception);
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

    public enum AuthType {
        NONE,
        BASIC,
        API_KEY
    }
}
