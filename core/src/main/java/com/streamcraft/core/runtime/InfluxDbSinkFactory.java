package com.streamcraft.core.runtime;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.shared.influxdb.InfluxDbSinkConfig;
import com.streamcraft.shared.influxdb.InfluxDbSinkConfigParser;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

public class InfluxDbSinkFactory {

    public void attach(DataStream<DataEntity> stream, PipelineNode sinkNode) {
        InfluxDbSinkConfig config = InfluxDbSinkConfigParser.parse(
                sinkNode.config(),
                IllegalArgumentException::new);
        stream.addSink(new InfluxDbSinkFunction(config))
                .name(sinkNode.name());
    }

    /**
     * Checkpoint-aware legacy sink with at-least-once delivery semantics.
     * InfluxDB's HTTP write is not atomically coupled to Flink checkpoints.
     */
    static final class InfluxDbSinkFunction extends RichSinkFunction<DataEntity>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;
        private static final Pattern MEASUREMENT_PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)}");

        private final InfluxDbSinkConfig config;
        private transient List<DataEntity> buffer;
        private transient Map<Long, List<DataEntity>> checkpointSnapshots;
        private transient HttpClient httpClient;
        private transient long lastFlushAt;
        private transient Counter recordsFlushed;
        private transient Counter checkpointFlushes;
        private transient Counter writeRetries;
        private transient Counter writeFailures;

        InfluxDbSinkFunction(InfluxDbSinkConfig config) {
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) {
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(config.connectTimeoutMillis()))
                    .build();
            buffer = new ArrayList<>();
            checkpointSnapshots = new HashMap<>();
            lastFlushAt = System.currentTimeMillis();
            try {
                recordsFlushed = getRuntimeContext().getMetricGroup().counter("records_flushed");
                checkpointFlushes = getRuntimeContext().getMetricGroup().counter("checkpoint_flushes");
                writeRetries = getRuntimeContext().getMetricGroup().counter("write_retries");
                writeFailures = getRuntimeContext().getMetricGroup().counter("write_failures");
            } catch (RuntimeException ignored) {
                // Direct unit tests and local construction have no task metric group.
            }
        }

        @Override
        public void invoke(DataEntity value, Context context) throws Exception {
            synchronized (this) {
                buffer.add(value);
                long now = System.currentTimeMillis();
                if (buffer.size() >= config.batchSize() || now - lastFlushAt >= config.flushIntervalMillis()) {
                    flush();
                }
            }
        }

        @Override
        public void close() throws Exception {
            synchronized (this) {
                if (buffer != null) {
                    flush();
                }
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            // The live buffer is intentionally replayed from the upstream checkpoint.
        }

        @Override
        public synchronized void snapshotState(FunctionSnapshotContext context) {
            if (buffer == null || checkpointSnapshots == null) {
                return;
            }
            checkpointSnapshots.put(context.getCheckpointId(), List.copyOf(buffer));
        }

        @Override
        public synchronized void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (checkpointSnapshots == null) {
                return;
            }
            List<DataEntity> snapshot = checkpointSnapshots.get(checkpointId);
            if (snapshot == null) {
                return;
            }
            List<DataEntity> recordsToFlush = recordsStillBuffered(snapshot);
            if (!recordsToFlush.isEmpty()) {
                try {
                    executeWriteWithRetry(lineProtocol(recordsToFlush));
                } catch (Exception exception) {
                    increment(writeFailures);
                    throw exception;
                }
                removeByIdentity(recordsToFlush);
                increment(recordsFlushed, recordsToFlush.size());
            }
            increment(checkpointFlushes);
            checkpointSnapshots.entrySet().removeIf(entry -> entry.getKey() <= checkpointId);
            lastFlushAt = System.currentTimeMillis();
        }

        @Override
        public synchronized void notifyCheckpointAborted(long checkpointId) {
            if (checkpointSnapshots != null) {
                checkpointSnapshots.remove(checkpointId);
            }
        }

        synchronized int bufferedRecordCount() {
            return buffer == null ? 0 : buffer.size();
        }

        private void flush() throws Exception {
            if (buffer.isEmpty()) {
                return;
            }
            List<DataEntity> records = List.copyOf(buffer);
            try {
                executeWriteWithRetry(lineProtocol(records));
            } catch (Exception exception) {
                increment(writeFailures);
                throw exception;
            }
            buffer.clear();
            increment(recordsFlushed, records.size());
            lastFlushAt = System.currentTimeMillis();
        }

        private List<DataEntity> recordsStillBuffered(List<DataEntity> snapshot) {
            List<DataEntity> available = new ArrayList<>(buffer);
            List<DataEntity> recordsToFlush = new ArrayList<>();
            for (DataEntity snapshotRecord : snapshot) {
                for (int index = 0; index < available.size(); index++) {
                    if (available.get(index) == snapshotRecord) {
                        recordsToFlush.add(snapshotRecord);
                        available.remove(index);
                        break;
                    }
                }
            }
            return recordsToFlush;
        }

        private void removeByIdentity(List<DataEntity> records) {
            for (DataEntity record : records) {
                for (int index = 0; index < buffer.size(); index++) {
                    if (buffer.get(index) == record) {
                        buffer.remove(index);
                        break;
                    }
                }
            }
        }

        private static void increment(Counter counter) {
            if (counter != null) {
                counter.inc();
            }
        }

        private static void increment(Counter counter, int amount) {
            if (counter != null) {
                counter.inc(amount);
            }
        }

        private String lineProtocol(List<DataEntity> records) {
            StringBuilder body = new StringBuilder();
            for (DataEntity record : records) {
                body.append(line(record)).append('\n');
            }
            return body.toString();
        }

        private String line(DataEntity record) {
            Map<String, Object> fields = record.fields();
            StringBuilder line = new StringBuilder();
            line.append(escapeMeasurement(resolveMeasurement(fields)));
            appendTags(line, fields);
            line.append(' ');
            line.append(fieldSet(fields));
            line.append(' ');
            line.append(timestamp(record, fields));
            return line.toString();
        }

        private String resolveMeasurement(Map<String, Object> fields) {
            Matcher matcher = MEASUREMENT_PLACEHOLDER.matcher(config.measurement());
            StringBuffer resolved = new StringBuffer();
            while (matcher.find()) {
                Object value = fields.get(matcher.group(1));
                if (value == null) {
                    throw new IllegalArgumentException(
                            "InfluxDB Sink measurement placeholder field is null or missing: " + matcher.group(1));
                }
                matcher.appendReplacement(resolved, Matcher.quoteReplacement(String.valueOf(value)));
            }
            matcher.appendTail(resolved);
            return resolved.toString();
        }

        private void appendTags(StringBuilder line, Map<String, Object> fields) {
            for (String tag : config.keyTags()) {
                Object value = fields.get(tag);
                if (value == null) {
                    continue;
                }
                line.append(',')
                        .append(escapeKey(tag))
                        .append('=')
                        .append(escapeTagValue(String.valueOf(value)));
            }
        }

        private String fieldSet(Map<String, Object> fields) {
            Map<String, Object> values = new LinkedHashMap<>();
            if (config.fields().isEmpty()) {
                fields.forEach((field, value) -> {
                    if (!config.keyTime().equals(field) && !config.keyTags().contains(field) && value != null) {
                        values.put(field, value);
                    }
                });
            } else {
                for (String field : config.fields()) {
                    Object value = fields.get(field);
                    if (value != null) {
                        values.put(field, value);
                    }
                }
            }
            if (values.isEmpty()) {
                throw new IllegalArgumentException("InfluxDB Sink record does not contain writable fields.");
            }
            return values.entrySet().stream()
                    .map(entry -> escapeKey(entry.getKey()) + "=" + fieldValue(entry.getValue()))
                    .reduce((left, right) -> left + "," + right)
                    .orElseThrow();
        }

        private String fieldValue(Object value) {
            if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
                return value + "i";
            }
            if (value instanceof Float || value instanceof Double) {
                return String.valueOf(value);
            }
            if (value instanceof Boolean) {
                return String.valueOf(value);
            }
            return "\"" + String.valueOf(value).replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }

        private long timestamp(DataEntity record, Map<String, Object> fields) {
            Object value = fields.get(config.keyTime());
            long millis = record.timestamp();
            if (value instanceof Number number) {
                millis = number.longValue();
            } else if (value instanceof String text && !text.isBlank()) {
                millis = parseTimestamp(text, record.timestamp());
            }
            return convertMillisToPrecision(millis, config.precision());
        }

        private void executeWriteWithRetry(String body) throws Exception {
            int maxAttempts = config.maxRetries() + 1;
            Exception lastFailure = null;
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    executeWrite(body);
                    return;
                } catch (Exception exception) {
                    lastFailure = exception;
                    if (attempt == maxAttempts || !isRetryable(exception)) {
                        throw exception;
                    }
                    increment(writeRetries);
                    Thread.sleep(Math.min(config.retryBackoffMultiplierMillis() * attempt,
                            config.maxRetryBackoffMillis()));
                }
            }
            if (lastFailure != null) {
                throw lastFailure;
            }
        }

        private boolean isRetryable(Exception exception) {
            return exception instanceof InfluxWriteException writeException
                    && (writeException.statusCode() == 429 || writeException.statusCode() >= 500);
        }

        private void executeWrite(String body) throws Exception {
            HttpRequest request = HttpRequest.newBuilder(writeUri())
                    .header("Content-Type", "text/plain; charset=utf-8")
                    .timeout(Duration.ofMillis(config.connectTimeoutMillis()))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new InfluxWriteException(response.statusCode(), response.body());
            }
        }

        private static final class InfluxWriteException extends IllegalStateException {

            private final int statusCode;

            private InfluxWriteException(int statusCode, String body) {
                super("InfluxDB Sink write failed with status " + statusCode + ": " + body);
                this.statusCode = statusCode;
            }

            private int statusCode() {
                return statusCode;
            }
        }

        private URI writeUri() {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("db", config.database());
            params.put("precision", config.precision());
            if (!config.username().isBlank()) {
                params.put("u", config.username());
            }
            if (!config.password().isBlank()) {
                params.put("p", config.password());
            }
            return URI.create(config.url() + "/write?" + encodedParams(params));
        }
    }

    private static long parseTimestamp(String value, long fallback) {
        try {
            return Instant.parse(value).toEpochMilli();
        } catch (Exception ignored) {
            try {
                return Timestamp.valueOf(value).getTime();
            } catch (Exception ignoredAgain) {
                return fallback;
            }
        }
    }

    private static long convertMillisToPrecision(long millis, String precision) {
        return switch (precision) {
            case "n" -> millis * 1_000_000L;
            case "u" -> millis * 1_000L;
            case "s" -> millis / 1_000L;
            case "m" -> millis / 60_000L;
            case "h" -> millis / 3_600_000L;
            case "ms" -> millis;
            default -> millis;
        };
    }

    private static String escapeMeasurement(String value) {
        return value.replace("\\", "\\\\")
                .replace(" ", "\\ ")
                .replace(",", "\\,");
    }

    private static String escapeKey(String value) {
        return escapeMeasurement(value).replace("=", "\\=");
    }

    private static String escapeTagValue(String value) {
        return escapeKey(value);
    }

    private static String encodedParams(Map<String, String> params) {
        return params.entrySet().stream()
                .map(entry -> encoded(entry.getKey()) + "=" + encoded(entry.getValue()))
                .reduce((left, right) -> left + "&" + right)
                .orElse("");
    }

    private static String encoded(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
