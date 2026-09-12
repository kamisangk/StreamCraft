package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig.AuthType;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig.CursorType;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig.ReadMode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ElasticsearchSourceReader implements SourceReader<DataEntity, ElasticsearchSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSourceReader.class);

    private final SourceReaderContext context;
    private final ElasticsearchSourceConfig config;
    private final Object lock = new Object();
    private final ArrayDeque<EsRecord> records = new ArrayDeque<>();
    private final ArrayDeque<ElasticsearchSourceSplit> pendingSplits = new ArrayDeque<>();
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private ElasticsearchSourceSplit split;
    private boolean started;
    private boolean exhausted;
    private boolean noMoreSplits;
    private boolean splitFinished;
    private int polls;
    private long nextPollAtMillis;
    private String activeScrollId = "";

    ElasticsearchSourceReader(SourceReaderContext context, ElasticsearchSourceConfig config) {
        this.context = context;
        this.config = config;
    }

    @Override
    public void start() {
        started = true;
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<DataEntity> output) throws Exception {
        synchronized (lock) {
            if (!started) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            activateNextSplit();
            if (split == null) return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
            if (splitFinished && records.isEmpty()) {
                split = null;
                activateNextSplit();
                if (split == null) return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
            }
            if (!records.isEmpty()) {
                emitOne(output);
                return records.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
            }
            if (exhausted) {
                return InputStatus.END_OF_INPUT;
            }
            if (config.readMode() == ReadMode.INCREMENTAL && config.maxPolls() > 0 && polls >= config.maxPolls()) {
                exhausted = true;
                return InputStatus.END_OF_INPUT;
            }
            if (System.currentTimeMillis() < nextPollAtMillis) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            loadBatch();
            if (!records.isEmpty()) {
                emitOne(output);
                return records.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
            }
            if (config.readMode() == ReadMode.FULL || (config.maxPolls() > 0 && polls >= config.maxPolls())) {
                exhausted = true;
                return InputStatus.END_OF_INPUT;
            }
            nextPollAtMillis = System.currentTimeMillis() + config.pollIntervalMillis();
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    @Override
    public List<ElasticsearchSourceSplit> snapshotState(long checkpointId) {
        synchronized (lock) {
            List<ElasticsearchSourceSplit> snapshot = new ArrayList<>(1 + pendingSplits.size());
            if (split != null && !splitFinished) snapshot.add(split);
            snapshot.addAll(pendingSplits);
            return snapshot;
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        synchronized (lock) {
            if (!records.isEmpty() || exhausted || split == null || System.currentTimeMillis() >= nextPollAtMillis) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Void> future = new CompletableFuture<>();
            CompletableFuture.delayedExecutor(
                    nextPollAtMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .execute(() -> future.complete(null));
            return future;
        }
    }

    @Override
    public void addSplits(List<ElasticsearchSourceSplit> splits) {
        synchronized (lock) {
            for (ElasticsearchSourceSplit candidate : splits) {
                candidate.state().validateFor(
                        "Elasticsearch Source", config.cursorType().name(), config.cursorField(), config.idField());
                pendingSplits.addLast(candidate);
            }
            activateNextSplit();
            exhausted = false;
            splitFinished = false;
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void close() {
        synchronized (lock) {
            exhausted = true;
            split = null;
            pendingSplits.clear();
            if (!activeScrollId.isBlank()) {
                clearScroll(activeScrollId);
                activeScrollId = "";
            }
        }
    }

    private void loadBatch() throws Exception {
        ElasticsearchSourceSplit current = split;
        SearchPage page = search(config.readMode() == ReadMode.FULL
                ? effectiveQuery(config.query())
                : incrementalQuery(effectiveQuery(config.query()), current.state()));
        String activeScroll = page.scrollId();
        activeScrollId = activeScroll;
        long pageIndex = current.pageIndex();
        int pagesRead = 0;
        try {
            while (!page.hits().isEmpty()) {
                appendHits(page.hits());
                pageIndex++;
                pagesRead++;
                if (config.maxPolls() > 0 && pagesRead >= config.maxPolls()) {
                    break;
                }
                if (page.scrollId().isBlank()) {
                    break;
                }
                activeScroll = page.scrollId();
                page = scroll(page.scrollId());
            }
        } finally {
            if (!activeScroll.isBlank()) {
                clearScroll(activeScroll);
            }
            activeScrollId = "";
        }
        split = new ElasticsearchSourceSplit(
                current.splitId(), current.state(), "", pageIndex);
        if (config.readMode() == ReadMode.FULL) splitFinished = true;
        polls++;
    }

    private void activateNextSplit() {
        if (split == null && !pendingSplits.isEmpty()) {
            split = pendingSplits.removeFirst();
            splitFinished = false;
            polls = 0;
            nextPollAtMillis = 0L;
        }
    }

    private SearchPage search(JsonNode query) throws Exception {
        ObjectNode body = objectMapper.createObjectNode();
        body.put("size", config.scrollSize());
        body.set("query", query);
        appendSlice(body);
        appendSourceFilter(body);
        appendSort(body);
        return executeSearch(firstHost() + "/" + config.index() + "/_search?scroll=" + encoded(config.scrollTime()), body);
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
                    "Elasticsearch Source request failed with status " + response.statusCode() + ": " + response.body());
        }
        JsonNode root = objectMapper.readTree(response.body());
        List<JsonNode> hits = new ArrayList<>();
        JsonNode hitArray = root.path("hits").path("hits");
        if (hitArray.isArray()) {
            hitArray.forEach(hits::add);
        }
        return new SearchPage(root.path("_scroll_id").asText(""), hits);
    }

    private void appendHits(List<JsonNode> hits) {
        for (JsonNode hit : hits) {
            Map<String, Object> fields = sourceFields(hit);
            if (config.sourceFields().isEmpty() || config.sourceFields().contains("_id")) {
                fields.put("_id", hit.path("_id").asText(""));
            }
            Object cursor = config.cursorField().isBlank() ? null : resolveField(fields, config.cursorField());
            if (!config.cursorField().isBlank() && cursor == null) {
                LOG.warn("Skipping Elasticsearch Source document because cursorField '{}' is null.", config.cursorField());
                continue;
            }
            Object uniqueId = config.idField().isBlank() ? null : resolveField(fields, config.idField());
            if (!config.idField().isBlank() && uniqueId == null && "_id".equals(config.idField())) {
                uniqueId = hit.path("_id").asText("");
            }
            if (!config.idField().isBlank() && (uniqueId == null || String.valueOf(uniqueId).isBlank())) {
                LOG.warn("Skipping Elasticsearch Source document because idField '{}' is null.", config.idField());
                continue;
            }
            records.add(new EsRecord(
                    new DataEntity(resolveId(hit, fields), resolveTimestamp(fields), fields, headers(hit)),
                    cursor == null ? null : String.valueOf(cursor),
                    uniqueId == null ? null : String.valueOf(uniqueId)));
        }
    }

    private void emitOne(ReaderOutput<DataEntity> output) {
        EsRecord record = records.removeFirst();
        output.collect(record.entity());
        if (config.readMode() == ReadMode.INCREMENTAL && record.cursorValue() != null) {
            split = new ElasticsearchSourceSplit(
                    split.splitId(),
                    new IncrementalSourceState(
                            config.cursorType().name(),
                            config.cursorField(),
                            record.cursorValue(),
                            config.idField().isBlank() ? null : config.idField(),
                            config.idField().isBlank() ? null : record.uniqueId()),
                    "",
                    split.pageIndex());
        }
    }

    private JsonNode incrementalQuery(JsonNode baseQuery, IncrementalSourceState state) {
        if (state.cursorValue() == null) {
            return baseQuery;
        }
        Object cursor = coerceCursor(state.cursorValue(), config.cursorType());
        if (state.uniqueIdValue() == null || config.idField().isBlank()) {
            ObjectNode bool = objectMapper.createObjectNode();
            ArrayNode must = bool.putObject("bool").putArray("must");
            must.add(baseQuery);
            ObjectNode range = objectMapper.createObjectNode().putObject("range").putObject(config.cursorField());
            range.set("gt", objectMapper.valueToTree(cursor));
            must.add(range);
            return bool;
        }
        ObjectNode bool = objectMapper.createObjectNode();
        ObjectNode outer = bool.putObject("bool");
        ArrayNode must = outer.putArray("must");
        must.add(baseQuery);
        ArrayNode should = outer.putArray("should");
        ObjectNode greaterCursor = objectMapper.createObjectNode().putObject("range").putObject(config.cursorField());
        greaterCursor.set("gt", objectMapper.valueToTree(cursor));
        should.add(greaterCursor);
        ObjectNode equalAndGreaterId = objectMapper.createObjectNode();
        ArrayNode tieMust = equalAndGreaterId.putObject("bool").putArray("must");
        ObjectNode equalCursor = objectMapper.createObjectNode().putObject("term");
        equalCursor.set(config.cursorField(), objectMapper.valueToTree(cursor));
        tieMust.add(equalCursor);
        ObjectNode greaterId = objectMapper.createObjectNode().putObject("range").putObject(config.idField());
        greaterId.put("gt", state.uniqueIdValue());
        tieMust.add(greaterId);
        should.add(equalAndGreaterId);
        outer.put("minimum_should_match", 1);
        return bool;
    }

    private JsonNode effectiveQuery(JsonNode query) {
        if (query == null || query.isMissingNode() || query.isNull() || (query.isObject() && query.isEmpty())) {
            return objectMapper.createObjectNode().set("match_all", objectMapper.createObjectNode());
        }
        return query;
    }

    private void appendSort(ObjectNode body) {
        if (config.cursorField().isBlank()) {
            return;
        }
        ArrayNode sort = body.putArray("sort");
        sort.add(objectMapper.createObjectNode().put(config.cursorField(), "asc"));
        if (!config.idField().isBlank()) {
            sort.add(objectMapper.createObjectNode().put(config.idField(), "asc"));
        }
    }

    private void appendSourceFilter(ObjectNode body) {
        List<String> filtered = new ArrayList<>(config.sourceFields().stream()
                .filter(field -> !"_id".equals(field))
                .toList());
        addRequiredField(filtered, config.cursorField());
        addRequiredField(filtered, config.idField());
        if (filtered.isEmpty()) {
            return;
        }
        ArrayNode source = body.putArray("_source");
        filtered.forEach(source::add);
    }

    private void appendSlice(ObjectNode body) {
        if (split == null || split.sliceMax() <= 1) return;
        ObjectNode slice = body.putObject("slice");
        slice.put("id", split.sliceId());
        slice.put("max", split.sliceMax());
    }

    private static void addRequiredField(List<String> fields, String field) {
        if (field != null && !field.isBlank() && !"_id".equals(field) && !fields.contains(field)) {
            fields.add(field);
        }
    }

    private void clearScroll(String scrollId) {
        try {
            ObjectNode body = objectMapper.createObjectNode();
            body.putArray("scroll_id").add(scrollId);
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(firstHost() + "/_search/scroll"))
                    .header("Content-Type", "application/json")
                    .method("DELETE", HttpRequest.BodyPublishers.ofString(body.toString()));
            applyAuth(builder);
            httpClient.send(builder.build(), HttpResponse.BodyHandlers.discarding());
        } catch (Exception exception) {
            LOG.debug("Unable to clear Elasticsearch scroll context", exception);
        }
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
            Object value = resolveField(fields, config.idField());
            if (value != null) {
                return String.valueOf(value);
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

    private void applyAuth(HttpRequest.Builder builder) {
        if (config.authType() == AuthType.BASIC) {
            builder.header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
                    (config.username() + ":" + config.password()).getBytes(StandardCharsets.UTF_8)));
        } else if (config.authType() == AuthType.API_KEY) {
            builder.header("Authorization", "ApiKey " + config.apiKey());
        }
    }

    private String firstHost() {
        String host = config.hosts().get(0);
        return host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
    }

    private static Object resolveField(Map<String, Object> fields, String fieldName) {
        if (fields == null || fieldName == null || fieldName.isBlank()) {
            return null;
        }
        if (fields.containsKey(fieldName)) {
            return fields.get(fieldName);
        }
        Object current = fields;
        for (String segment : fieldName.split("\\.", -1)) {
            if (!(current instanceof Map<?, ?> currentMap)) {
                return null;
            }
            current = currentMap.get(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private static Object coerceCursor(String value, CursorType type) {
        return switch (type) {
            case LONG -> Long.parseLong(value);
            case DOUBLE -> Double.parseDouble(value);
            case TIMESTAMP -> {
                try {
                    yield Timestamp.from(Instant.parse(value));
                } catch (Exception exception) {
                    yield Timestamp.from(Instant.ofEpochMilli(Long.parseLong(value)));
                }
            }
            case STRING -> value;
        };
    }

    private static String encoded(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private record SearchPage(String scrollId, List<JsonNode> hits) {
    }

    private record EsRecord(DataEntity entity, String cursorValue, String uniqueId) {
    }
}
