package com.streamcraft.shared.influxdb;

import java.io.Serializable;
import java.util.List;

public record InfluxDbSinkConfig(
        String url,
        String database,
        String measurement,
        String keyTime,
        List<String> keyTags,
        List<String> fields,
        int batchSize,
        int maxRetries,
        long retryBackoffMultiplierMillis,
        long maxRetryBackoffMillis,
        int connectTimeoutMillis,
        long flushIntervalMillis,
        String precision,
        String username,
        String password) implements Serializable {

    public static final String DEFAULT_KEY_TIME = "time";
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final long DEFAULT_RETRY_BACKOFF_MULTIPLIER_MILLIS = 100L;
    public static final long DEFAULT_MAX_RETRY_BACKOFF_MILLIS = 1_000L;
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 10_000;
    public static final long DEFAULT_FLUSH_INTERVAL_MILLIS = 5_000L;
    public static final String DEFAULT_PRECISION = "ms";

    public InfluxDbSinkConfig {
        url = clean(url);
        database = clean(database);
        measurement = clean(measurement);
        keyTime = clean(keyTime);
        if (keyTime.isBlank()) {
            keyTime = DEFAULT_KEY_TIME;
        }
        keyTags = keyTags == null ? List.of() : List.copyOf(keyTags);
        fields = fields == null ? List.of() : List.copyOf(fields);
        batchSize = batchSize <= 0 ? DEFAULT_BATCH_SIZE : batchSize;
        maxRetries = Math.max(0, maxRetries);
        retryBackoffMultiplierMillis = retryBackoffMultiplierMillis <= 0
                ? DEFAULT_RETRY_BACKOFF_MULTIPLIER_MILLIS
                : retryBackoffMultiplierMillis;
        maxRetryBackoffMillis = maxRetryBackoffMillis <= 0
                ? DEFAULT_MAX_RETRY_BACKOFF_MILLIS
                : maxRetryBackoffMillis;
        connectTimeoutMillis = connectTimeoutMillis <= 0 ? DEFAULT_CONNECT_TIMEOUT_MILLIS : connectTimeoutMillis;
        flushIntervalMillis = flushIntervalMillis <= 0 ? DEFAULT_FLUSH_INTERVAL_MILLIS : flushIntervalMillis;
        precision = clean(precision);
        if (precision.isBlank()) {
            precision = DEFAULT_PRECISION;
        }
        username = clean(username);
        password = password == null ? "" : password;
    }

    private static String clean(String value) {
        return value == null ? "" : value.trim();
    }
}
