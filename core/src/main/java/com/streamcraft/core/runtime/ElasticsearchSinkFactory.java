package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.shared.elasticsearch.ElasticsearchSinkConfig;
import com.streamcraft.shared.elasticsearch.ElasticsearchSinkConfig.AuthType;
import com.streamcraft.shared.elasticsearch.ElasticsearchSinkConfigParser;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

public class ElasticsearchSinkFactory {

    public void attach(DataStream<DataEntity> stream, PipelineNode sinkNode) {
        ElasticsearchSinkConfig config = ElasticsearchSinkConfigParser.parse(
                sinkNode.config(),
                IllegalArgumentException::new);
        stream.addSink(new ElasticsearchSinkFunction(config))
                .name(sinkNode.name());
    }

    /**
     * Checkpoint-aware legacy sink with at-least-once delivery semantics.
     * Elasticsearch bulk responses are handled per input record.
     * Writes finish before a checkpoint snapshot returns; aborted checkpoints
     * may expose writes that are replayed after recovery.
     */
    static final class ElasticsearchSinkFunction extends RichSinkFunction<DataEntity>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;
        private static final Pattern INDEX_PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_.]*)}");

        private final ElasticsearchSinkConfig config;
        private transient List<DataEntity> buffer;
        private transient HttpClient httpClient;
        private transient ObjectMapper objectMapper;
        private transient long lastFlushAt;
        private transient Counter recordsFlushed;
        private transient Counter checkpointFlushes;
        private transient Counter writeRetries;
        private transient Counter writeFailures;

        ElasticsearchSinkFunction(ElasticsearchSinkConfig config) {
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) {
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            objectMapper = new ObjectMapper();
            buffer = new ArrayList<>();
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
                if (buffer.size() >= config.maxBatchSize() || now - lastFlushAt >= config.flushIntervalMillis()) {
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
            // Snapshots flush durably; records after the snapshot are replayed upstream.
        }

        @Override
        public synchronized void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (buffer == null) {
                return;
            }
            flush();
            increment(checkpointFlushes);
        }

        @Override
        public synchronized void notifyCheckpointComplete(long checkpointId) {
            // Durability is established before the snapshot is acknowledged.
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
                executeBulkWithRetry(records);
            } catch (Exception exception) {
                increment(writeFailures);
                throw exception;
            }
            buffer.clear();
            increment(recordsFlushed, records.size());
            lastFlushAt = System.currentTimeMillis();
        }

        private String bulkBody(List<DataEntity> records) {
            StringBuilder body = new StringBuilder();
            try {
                for (DataEntity record : records) {
                    body.append(objectMapper.writeValueAsString(action(record))).append('\n');
                    body.append(objectMapper.writeValueAsString(document(record))).append('\n');
                }
            } catch (java.io.IOException exception) {
                throw new ElasticsearchBulkResponseException(
                        "Elasticsearch Sink record cannot be serialized to bulk JSON.",
                        exception);
            }
            return body.toString();
        }

        private ObjectNode action(DataEntity record) {
            ObjectNode root = objectMapper.createObjectNode();
            ObjectNode index = root.putObject("index");
            index.put("_index", resolveIndex(record.fields()));
            if (!config.indexType().isBlank()) {
                index.put("_type", config.indexType());
            }
            String documentId = documentId(record);
            if (!documentId.isBlank()) {
                index.put("_id", documentId);
            }
            return root;
        }

        private ObjectNode document(DataEntity record) {
            Map<String, Object> fields = record.fields();
            if (config.fields().isEmpty()) {
                return objectMapper.valueToTree(fields);
            }
            ObjectNode document = objectMapper.createObjectNode();
            for (String field : config.fields()) {
                Object value = resolveField(fields, field);
                if (value != null) {
                    document.set(field, objectMapper.valueToTree(value));
                }
            }
            return document;
        }

        private String resolveIndex(Map<String, Object> fields) {
            Matcher matcher = INDEX_PLACEHOLDER.matcher(config.index());
            StringBuffer resolved = new StringBuffer();
            while (matcher.find()) {
                Object value = resolveField(fields, matcher.group(1));
                if (value == null) {
                    throw new IllegalArgumentException(
                            "Elasticsearch Sink index placeholder field is null or missing: " + matcher.group(1));
                }
                matcher.appendReplacement(resolved, Matcher.quoteReplacement(String.valueOf(value)));
            }
            matcher.appendTail(resolved);
            return resolved.toString().toLowerCase(Locale.ROOT);
        }

        private String documentId(DataEntity record) {
            if (config.primaryKeys().isEmpty()) {
                return "";
            }
            List<String> values = new ArrayList<>();
            for (String primaryKey : config.primaryKeys()) {
                Object value = resolveField(record.fields(), primaryKey);
                if (value == null) {
                    throw new IllegalArgumentException(
                            "Elasticsearch Sink primary key field is null or missing: " + primaryKey);
                }
                values.add(String.valueOf(value));
            }
            return String.join(config.keyDelimiter(), values);
        }

        private void executeBulkWithRetry(List<DataEntity> records) throws Exception {
            List<DataEntity> pending = List.copyOf(records);
            int maxAttempts = config.maxRetryCount() + 1;
            Exception lastFailure = null;
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    List<DataEntity> retryRecords = executeBulk(pending);
                    if (retryRecords.isEmpty()) {
                        return;
                    }
                    pending = retryRecords;
                    lastFailure = new ElasticsearchBulkResponseException(
                            "Elasticsearch Sink bulk response contains retryable item failures.");
                } catch (Exception exception) {
                    lastFailure = exception;
                    if (attempt == maxAttempts || !isRetryable(exception)) {
                        throw exception;
                    }
                }
                increment(writeRetries);
                Thread.sleep(Math.min(1_000L * attempt, 5_000L));
            }
            if (lastFailure != null) {
                throw lastFailure;
            }
        }

        private boolean isRetryable(Exception exception) {
            if (exception instanceof ElasticsearchBulkException bulkException) {
                int status = bulkException.statusCode();
                return status == 429 || status >= 500;
            }
            return exception instanceof java.io.IOException;
        }

        private List<DataEntity> executeBulk(List<DataEntity> records) throws Exception {
            String body = bulkBody(records);
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(firstHost() + "/_bulk"))
                    .header("Content-Type", "application/x-ndjson")
                    .POST(HttpRequest.BodyPublishers.ofString(body));
            applyAuth(builder);
            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new ElasticsearchBulkException(
                        response.statusCode(),
                        "Elasticsearch Sink bulk request failed with status " + response.statusCode() + ": "
                                + response.body());
            }
            JsonNode root;
            try {
                root = objectMapper.readTree(response.body());
            } catch (java.io.IOException exception) {
                throw malformedResponse("response is not valid JSON", exception);
            }
            if (root == null || !root.isObject()) {
                throw malformedResponse("response must be a JSON object");
            }
            JsonNode items = root.get("items");
            if (items == null || !items.isArray()) {
                throw malformedResponse("response is missing an items array");
            }
            if (items.size() != records.size()) {
                throw malformedResponse(
                        "response item count " + items.size() + " does not match request record count " + records.size());
            }
            List<DataEntity> retryRecords = new ArrayList<>();
            for (int index = 0; index < records.size(); index++) {
                JsonNode item = items.get(index);
                if (item == null || !item.isObject() || item.size() != 1) {
                    throw malformedResponse("item " + index + " must contain exactly one operation object");
                }
                String operationName = item.fieldNames().next();
                if (!Set.of("index", "create", "update", "delete").contains(operationName)) {
                    throw malformedResponse("item " + index + " has unsupported operation: " + operationName);
                }
                JsonNode operation = item.get(operationName);
                if (operation == null || !operation.isObject()) {
                    throw malformedResponse("item " + index + " operation is not an object");
                }
                JsonNode statusNode = operation.get("status");
                if (statusNode == null || !statusNode.isIntegralNumber()) {
                    throw malformedResponse("item " + index + " is missing a numeric status");
                }
                int status = statusNode.intValue();
                if (status < 100 || status > 599) {
                    throw malformedResponse("item " + index + " has invalid status " + status);
                }
                if (status >= 200 && status < 300) {
                    continue;
                }
                if (status == 429 || status >= 500) {
                    retryRecords.add(records.get(index));
                    continue;
                }
                throw new ElasticsearchBulkResponseException(
                        "Elasticsearch Sink bulk item " + index + " failed with status " + status + ": " + operation);
            }
            return retryRecords;
        }

        private ElasticsearchBulkResponseException malformedResponse(String detail) {
            return malformedResponse(detail, null);
        }

        private ElasticsearchBulkResponseException malformedResponse(String detail, Throwable cause) {
            return new ElasticsearchBulkResponseException(
                    "Elasticsearch Sink bulk response is invalid: " + detail,
                    cause);
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

        private static final class ElasticsearchBulkException extends IllegalStateException {

            private final int statusCode;

            private ElasticsearchBulkException(int statusCode, String message) {
                super(message);
                this.statusCode = statusCode;
            }

            private int statusCode() {
                return statusCode;
            }
        }

        private static final class ElasticsearchBulkResponseException extends IllegalStateException {

            private ElasticsearchBulkResponseException(String message) {
                this(message, null);
            }

            private ElasticsearchBulkResponseException(String message, Throwable cause) {
                super(message, cause);
            }
        }

        private void applyAuth(HttpRequest.Builder builder) {
            if (config.authType() == AuthType.BASIC) {
                String token = Base64.getEncoder().encodeToString(
                        (config.username() + ":" + config.password()).getBytes(StandardCharsets.UTF_8));
                builder.header("Authorization", "Basic " + token);
                return;
            }
            if (config.authType() == AuthType.API_KEY) {
                String token = Base64.getEncoder().encodeToString(
                        (config.apiKeyId() + ":" + config.apiKey()).getBytes(StandardCharsets.UTF_8));
                builder.header("Authorization", "ApiKey " + token);
                return;
            }
            if (config.authType() == AuthType.API_KEY_ENCODED) {
                builder.header("Authorization", "ApiKey " + config.apiKeyEncoded());
            }
        }

        private String firstHost() {
            return config.hosts().get(0);
        }
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
}
