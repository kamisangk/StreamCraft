package com.streamcraft.shared.elasticsearch;

import java.io.Serializable;
import java.util.List;

public record ElasticsearchSinkConfig(
        List<String> hosts,
        String index,
        String indexType,
        List<String> primaryKeys,
        String keyDelimiter,
        List<String> fields,
        int maxBatchSize,
        long flushIntervalMillis,
        int maxRetryCount,
        AuthType authType,
        String username,
        String password,
        String apiKeyId,
        String apiKey,
        String apiKeyEncoded) implements Serializable {

    public static final int DEFAULT_MAX_BATCH_SIZE = 10;
    public static final long DEFAULT_FLUSH_INTERVAL_MILLIS = 5_000L;
    public static final int DEFAULT_MAX_RETRY_COUNT = 3;
    public static final String DEFAULT_KEY_DELIMITER = "_";

    public ElasticsearchSinkConfig {
        hosts = hosts == null ? List.of() : List.copyOf(hosts);
        index = clean(index);
        indexType = clean(indexType);
        primaryKeys = primaryKeys == null ? List.of() : List.copyOf(primaryKeys);
        keyDelimiter = clean(keyDelimiter);
        if (keyDelimiter.isEmpty()) {
            keyDelimiter = DEFAULT_KEY_DELIMITER;
        }
        fields = fields == null ? List.of() : List.copyOf(fields);
        maxBatchSize = maxBatchSize <= 0 ? DEFAULT_MAX_BATCH_SIZE : maxBatchSize;
        flushIntervalMillis = flushIntervalMillis <= 0 ? DEFAULT_FLUSH_INTERVAL_MILLIS : flushIntervalMillis;
        maxRetryCount = Math.max(0, maxRetryCount);
        authType = authType == null ? AuthType.NONE : authType;
        username = clean(username);
        password = password == null ? "" : password;
        apiKeyId = clean(apiKeyId);
        apiKey = clean(apiKey);
        apiKeyEncoded = clean(apiKeyEncoded);
    }

    private static String clean(String value) {
        return value == null ? "" : value.trim();
    }

    public enum AuthType {
        NONE,
        BASIC,
        API_KEY,
        API_KEY_ENCODED
    }
}
