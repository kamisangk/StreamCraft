package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig.AuthType;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig.CursorType;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig.ReadMode;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfigParser;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSourceFactory.class);

    public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
        ElasticsearchSourceConfig config = ElasticsearchSourceConfigParser.parse(
                sourceNode.config(),
                IllegalArgumentException::new);
        return env.addSource(new ElasticsearchSourceFunction(config))
                .name(sourceNode.name())
                .setParallelism(1);
    }

    private static final class ElasticsearchSourceFunction extends RichParallelSourceFunction<DataEntity> {

        private static final long serialVersionUID = 1L;

        private final ElasticsearchSourceConfig config;
        private volatile boolean running = true;
        private transient HttpClient httpClient;
        private transient ObjectMapper objectMapper;

        private ElasticsearchSourceFunction(ElasticsearchSourceConfig config) {
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) {
            httpClient = HttpClient.newHttpClient();
            objectMapper = new ObjectMapper();
        }

        @Override
        public void run(SourceContext<DataEntity> sourceContext) throws Exception {
            if (config.readMode() == ReadMode.FULL) {
                runFull(sourceContext);
                return;
            }
            runIncremental(sourceContext);
        }

        private void runFull(SourceContext<DataEntity> sourceContext) throws Exception {
            SearchPage page = search(config.query(), null);
            while (running && !page.hits().isEmpty()) {
                collectHits(sourceContext, page.hits());
                if (page.scrollId().isBlank()) {
                    return;
                }
                page = scroll(page.scrollId());
            }
        }

        private void runIncremental(SourceContext<DataEntity> sourceContext) throws Exception {
            Object cursor = coerceInitialCursor(config.initialCursorValue(), config.cursorType());
            int polls = 0;
            while (running && (config.maxPolls() == 0 || polls < config.maxPolls())) {
                Object nextCursor = runIncrementalPoll(sourceContext, cursor);
                if (nextCursor != null) {
                    cursor = nextCursor;
                }
                polls++;
                if (running && (config.maxPolls() == 0 || polls < config.maxPolls())) {
                    sleepIfRunning(config.pollIntervalMillis());
                }
            }
        }

        private Object runIncrementalPoll(SourceContext<DataEntity> sourceContext, Object cursor) throws Exception {
            SearchPage page = search(incrementalQuery(config.query(), cursor), config.cursorField());
            Object lastCursor = null;
            while (running && !page.hits().isEmpty()) {
                Object pageCursor = collectHits(sourceContext, page.hits());
                if (pageCursor != null) {
                    lastCursor = pageCursor;
                }
                if (page.scrollId().isBlank()) {
                    return lastCursor;
                }
                page = scroll(page.scrollId());
            }
            return lastCursor;
        }

        private SearchPage search(JsonNode query, String sortField) throws Exception {
            ObjectNode body = objectMapper.createObjectNode();
            body.put("size", config.scrollSize());
            body.set("query", effectiveQuery(query));
            appendSourceFilter(body);
            if (sortField != null && !sortField.isBlank()) {
                ArrayNode sort = body.putArray("sort");
                ObjectNode sortItem = objectMapper.createObjectNode();
                sortItem.put(sortField, "asc");
                sort.add(sortItem);
            }
            String url = firstHost() + "/" + config.index() + "/_search?scroll=" + encoded(config.scrollTime());
            return executeSearch(url, body);
        }

        private SearchPage scroll(String scrollId) throws Exception {
            ObjectNode body = objectMapper.createObjectNode();
            body.put("scroll", config.scrollTime());
            body.put("scroll_id", scrollId);
            return executeSearch(firstHost() + "/_search/scroll", body);
        }

        private SearchPage executeSearch(String url, JsonNode body) throws Exception {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body)));
            applyAuth(builder);
            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException(
                        "Elasticsearch Source request failed with status " + response.statusCode() + ": "
                                + response.body());
            }
            JsonNode root = objectMapper.readTree(response.body());
            List<JsonNode> hits = root.path("hits").path("hits").isArray()
                    ? objectMapper.convertValue(root.path("hits").path("hits"), objectMapper.getTypeFactory()
                            .constructCollectionType(List.class, JsonNode.class))
                    : List.of();
            return new SearchPage(root.path("_scroll_id").asText(""), hits);
        }

        private Object collectHits(SourceContext<DataEntity> sourceContext, List<JsonNode> hits) {
            Object lastCursor = null;
            for (JsonNode hit : hits) {
                if (!running) {
                    return lastCursor;
                }
                Map<String, Object> fields = sourceFields(hit);
                if (config.sourceFields().isEmpty() || config.sourceFields().contains("_id")) {
                    fields.put("_id", hit.path("_id").asText(""));
                }
                Object cursorValue = config.cursorField().isBlank()
                        ? null
                        : resolveField(fields, config.cursorField());
                if (!config.cursorField().isBlank() && cursorValue == null) {
                    LOG.warn("Skipping Elasticsearch Source document because cursorField '{}' is null.",
                            config.cursorField());
                    continue;
                }
                synchronized (sourceContext.getCheckpointLock()) {
                    sourceContext.collect(new DataEntity(
                            resolveId(hit, fields),
                            resolveTimestamp(fields),
                            fields,
                            headers(hit)));
                }
                if (cursorValue != null) {
                    lastCursor = cursorValue;
                }
            }
            return lastCursor;
        }

        @SuppressWarnings("unchecked")
        private Map<String, Object> sourceFields(JsonNode hit) {
            JsonNode source = hit.path("_source");
            if (!source.isObject()) {
                return new LinkedHashMap<>();
            }
            return new LinkedHashMap<>(objectMapper.convertValue(source, Map.class));
        }

        private String resolveId(JsonNode hit, Map<String, Object> fields) {
            if (!config.idField().isBlank()) {
                Object idValue = resolveField(fields, config.idField());
                if (idValue != null) {
                    return String.valueOf(idValue);
                }
            }
            String hitId = hit.path("_id").asText("");
            return hitId.isBlank() ? UUID.randomUUID().toString() : hitId;
        }

        private long resolveTimestamp(Map<String, Object> fields) {
            if (config.timestampField().isBlank()) {
                return System.currentTimeMillis();
            }
            Object value = resolveField(fields, config.timestampField());
            if (value instanceof Number number) {
                return number.longValue();
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

        private Map<String, String> headers(JsonNode hit) {
            Map<String, String> headers = new HashMap<>();
            headers.put("source", "elasticsearch");
            headers.put("readMode", config.readMode().name());
            headers.put("index", hit.path("_index").asText(config.index()));
            if (!config.cursorField().isBlank()) {
                headers.put("cursorField", config.cursorField());
            }
            return headers;
        }

        private JsonNode incrementalQuery(JsonNode baseQuery, Object cursor) {
            if (cursor == null) {
                return effectiveQuery(baseQuery);
            }
            ObjectNode bool = objectMapper.createObjectNode();
            ArrayNode must = bool.putObject("bool").putArray("must");
            must.add(effectiveQuery(baseQuery));
            ObjectNode range = objectMapper.createObjectNode();
            ObjectNode fieldRange = range.putObject("range").putObject(config.cursorField());
            fieldRange.set("gt", objectMapper.valueToTree(cursor));
            must.add(range);
            return bool;
        }

        private JsonNode effectiveQuery(JsonNode query) {
            if (query == null || query.isMissingNode() || query.isNull()
                    || (query.isObject() && query.isEmpty())) {
                return objectMapper.createObjectNode().set("match_all", objectMapper.createObjectNode());
            }
            return query;
        }

        private void appendSourceFilter(ObjectNode body) {
            List<String> filteredSourceFields = config.sourceFields().stream()
                    .filter(field -> !"_id".equals(field))
                    .toList();
            if (filteredSourceFields.isEmpty()) {
                return;
            }
            ArrayNode source = body.putArray("_source");
            filteredSourceFields.forEach(source::add);
        }

        private void applyAuth(HttpRequest.Builder builder) {
            if (config.authType() == AuthType.BASIC) {
                String token = Base64.getEncoder().encodeToString(
                        (config.username() + ":" + config.password()).getBytes(StandardCharsets.UTF_8));
                builder.header("Authorization", "Basic " + token);
                return;
            }
            if (config.authType() == AuthType.API_KEY) {
                builder.header("Authorization", "ApiKey " + config.apiKey());
            }
        }

        private String firstHost() {
            String host = config.hosts().get(0);
            return host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
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

    private record SearchPage(String scrollId, List<JsonNode> hits) {
    }

    @SuppressWarnings("unchecked")
    private static Object resolveField(Map<String, Object> fields, String fieldName) {
        if (fields == null || fieldName == null || fieldName.isBlank()) {
            return null;
        }
        if (fields.containsKey(fieldName)) {
            return fields.get(fieldName);
        }
        Object current = fields;
        for (String segment : fieldName.split("\\.", -1)) {
            if (segment.isEmpty()) {
                return null;
            }
            if (!(current instanceof Map<?, ?> currentMap)) {
                return null;
            }
            current = ((Map<String, Object>) currentMap).get(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private static Object coerceInitialCursor(String value, CursorType cursorType) {
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

    private static String encoded(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
