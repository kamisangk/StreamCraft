package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfig;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfig.CursorType;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfig.ReadMode;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfigParser;
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
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;

public class InfluxDbSourceFactory {

    public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
        InfluxDbSourceConfig config = InfluxDbSourceConfigParser.parse(
                sourceNode.config(),
                IllegalArgumentException::new);
        return env.addSource(new InfluxDbSourceFunction(config))
                .name(sourceNode.name())
                .setParallelism(1);
    }

    private static final class InfluxDbSourceFunction extends RichParallelSourceFunction<DataEntity> {

        private static final long serialVersionUID = 1L;

        private final InfluxDbSourceConfig config;
        private volatile boolean running = true;
        private transient HttpClient httpClient;
        private transient ObjectMapper objectMapper;

        private InfluxDbSourceFunction(InfluxDbSourceConfig config) {
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) {
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(config.connectTimeoutMillis()))
                    .build();
            objectMapper = new ObjectMapper();
        }

        @Override
        public void run(SourceContext<DataEntity> sourceContext) throws Exception {
            if (config.readMode() == ReadMode.FULL) {
                collectRows(sourceContext, executeQuery(config.sql()));
                return;
            }
            runIncremental(sourceContext);
        }

        private void runIncremental(SourceContext<DataEntity> sourceContext) throws Exception {
            Object cursor = coerceCursor(config.initialCursorValue(), config.cursorType());
            int polls = 0;
            while (running && (config.maxPolls() == 0 || polls < config.maxPolls())) {
                Object nextCursor = collectRows(sourceContext, executeQuery(incrementalSql(cursor)));
                if (nextCursor != null) {
                    cursor = nextCursor;
                }
                polls++;
                if (running && (config.maxPolls() == 0 || polls < config.maxPolls())) {
                    sleepIfRunning(config.pollIntervalMillis());
                }
            }
        }

        private Object collectRows(SourceContext<DataEntity> sourceContext, List<Map<String, Object>> rows) {
            Object lastCursor = null;
            for (Map<String, Object> fields : rows) {
                if (!running) {
                    return lastCursor;
                }
                Object cursorValue = config.cursorField().isBlank() ? null : fields.get(config.cursorField());
                if (!config.cursorField().isBlank() && cursorValue == null) {
                    continue;
                }
                synchronized (sourceContext.getCheckpointLock()) {
                    sourceContext.collect(new DataEntity(
                            resolveId(fields),
                            resolveTimestamp(fields),
                            fields,
                            headers(fields)));
                }
                if (cursorValue != null) {
                    lastCursor = cursorValue;
                }
            }
            return lastCursor;
        }

        private List<Map<String, Object>> executeQuery(String sql) throws Exception {
            HttpRequest request = HttpRequest.newBuilder(queryUri(sql))
                    .timeout(Duration.ofSeconds(config.queryTimeoutSeconds()))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException(
                        "InfluxDB Source query failed with status " + response.statusCode() + ": " + response.body());
            }
            return parseRows(response.body());
        }

        private URI queryUri(String sql) {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("db", config.database());
            params.put("q", sql);
            params.put("epoch", config.epoch());
            if (!config.username().isBlank()) {
                params.put("u", config.username());
            }
            if (!config.password().isBlank()) {
                params.put("p", config.password());
            }
            return URI.create(config.url() + "/query?" + encodedParams(params));
        }

        private List<Map<String, Object>> parseRows(String body) throws Exception {
            JsonNode root = objectMapper.readTree(body);
            List<Map<String, Object>> rows = new ArrayList<>();
            for (JsonNode result : root.path("results")) {
                String resultError = result.path("error").asText("");
                if (!resultError.isBlank()) {
                    throw new IllegalStateException("InfluxDB Source query error: " + resultError);
                }
                for (JsonNode series : result.path("series")) {
                    List<String> columns = columnNames(series.path("columns"));
                    Map<String, Object> tags = tags(series.path("tags"));
                    for (JsonNode valueRow : series.path("values")) {
                        Map<String, Object> fields = new LinkedHashMap<>();
                        fields.put("_measurement", series.path("name").asText(""));
                        fields.putAll(tags);
                        for (int i = 0; i < columns.size() && i < valueRow.size(); i++) {
                            fields.put(columns.get(i), objectMapper.convertValue(valueRow.get(i), Object.class));
                        }
                        rows.add(fields);
                    }
                }
            }
            return rows;
        }

        private List<String> columnNames(JsonNode columns) {
            List<String> names = new ArrayList<>();
            for (JsonNode column : columns) {
                String name = column.asText("");
                if (!name.isBlank()) {
                    names.add(name);
                }
            }
            return names;
        }

        @SuppressWarnings("unchecked")
        private Map<String, Object> tags(JsonNode tags) {
            if (!tags.isObject()) {
                return Map.of();
            }
            return new LinkedHashMap<>(objectMapper.convertValue(tags, Map.class));
        }

        private String incrementalSql(Object cursor) {
            if (cursor == null) {
                return config.sql();
            }
            String literal = cursorLiteral(cursor, config.cursorType());
            if (config.sql().contains("${cursor}")) {
                return config.sql().replace("${cursor}", literal);
            }
            return appendCursorPredicate(config.sql(), config.cursorField() + " > " + literal);
        }

        private String resolveId(Map<String, Object> fields) {
            if (!config.idField().isBlank()) {
                Object value = fields.get(config.idField());
                if (value != null) {
                    return String.valueOf(value);
                }
            }
            Object measurement = fields.get("_measurement");
            Object timestamp = fields.getOrDefault(timestampField(), "");
            if (measurement != null && timestamp != null) {
                return measurement + ":" + timestamp + ":" + UUID.randomUUID();
            }
            return UUID.randomUUID().toString();
        }

        private long resolveTimestamp(Map<String, Object> fields) {
            Object value = fields.get(timestampField());
            if (value instanceof Number number) {
                return convertEpochToMillis(number.longValue(), config.epoch());
            }
            if (value instanceof String text && !text.isBlank()) {
                try {
                    return Instant.parse(text).toEpochMilli();
                } catch (Exception ignored) {
                    try {
                        return Timestamp.valueOf(text).getTime();
                    } catch (Exception ignoredAgain) {
                        return System.currentTimeMillis();
                    }
                }
            }
            return System.currentTimeMillis();
        }

        private String timestampField() {
            return config.timestampField().isBlank() ? "time" : config.timestampField();
        }

        private Map<String, String> headers(Map<String, Object> fields) {
            Map<String, String> headers = new HashMap<>();
            headers.put("source", "influxdb");
            headers.put("readMode", config.readMode().name());
            headers.put("database", config.database());
            Object measurement = fields.get("_measurement");
            if (measurement != null) {
                headers.put("measurement", String.valueOf(measurement));
            }
            if (!config.cursorField().isBlank()) {
                headers.put("cursorField", config.cursorField());
            }
            return headers;
        }

        private void sleepIfRunning(long millis) throws InterruptedException {
            long remaining = millis;
            while (running && remaining > 0) {
                long chunk = Math.min(remaining, 200L);
                Thread.sleep(chunk);
                remaining -= chunk;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void close() {
            running = false;
        }
    }

    private static String appendCursorPredicate(String sql, String predicate) {
        String lower = sql.toLowerCase(Locale.ROOT);
        int insertAt = sql.length();
        for (String token : List.of(" group by ", " order by ", " limit ", " slimit ", " tz(")) {
            int index = lower.indexOf(token);
            if (index >= 0 && index < insertAt) {
                insertAt = index;
            }
        }
        String head = sql.substring(0, insertAt).trim();
        String tail = sql.substring(insertAt);
        String separator = lower.substring(0, insertAt).contains(" where ") ? " and " : " where ";
        return head + separator + predicate + tail;
    }

    private static String cursorLiteral(Object cursor, CursorType cursorType) {
        return switch (cursorType) {
            case LONG, DOUBLE -> String.valueOf(cursor);
            case TIMESTAMP, STRING -> "'" + String.valueOf(cursor).replace("'", "\\'") + "'";
        };
    }

    private static Object coerceCursor(String value, CursorType cursorType) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return switch (cursorType) {
            case LONG -> Long.parseLong(value);
            case DOUBLE -> Double.parseDouble(value);
            case TIMESTAMP -> coerceTimestamp(value).toInstant().toString();
            case STRING -> value;
        };
    }

    private static Timestamp coerceTimestamp(String value) {
        try {
            return Timestamp.from(Instant.parse(value));
        } catch (Exception ignored) {
            return Timestamp.valueOf(value);
        }
    }

    private static long convertEpochToMillis(long value, String epoch) {
        return switch (epoch) {
            case "n" -> value / 1_000_000L;
            case "u" -> value / 1_000L;
            case "s" -> value * 1_000L;
            case "m" -> value * 60_000L;
            case "h" -> value * 3_600_000L;
            case "ms" -> value;
            default -> value;
        };
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
