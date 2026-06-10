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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.common.functions.OpenContext;
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

    private static final class ElasticsearchSinkFunction extends RichSinkFunction<DataEntity> {

        private static final long serialVersionUID = 1L;
        private static final Pattern INDEX_PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_.]*)}");

        private final ElasticsearchSinkConfig config;
        private final List<DataEntity> buffer = new ArrayList<>();
        private transient HttpClient httpClient;
        private transient ObjectMapper objectMapper;
        private transient long lastFlushAt;

        private ElasticsearchSinkFunction(ElasticsearchSinkConfig config) {
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) {
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            objectMapper = new ObjectMapper();
            lastFlushAt = System.currentTimeMillis();
        }

        @Override
        public void invoke(DataEntity value, Context context) throws Exception {
            buffer.add(value);
            long now = System.currentTimeMillis();
            if (buffer.size() >= config.maxBatchSize() || now - lastFlushAt >= config.flushIntervalMillis()) {
                flush();
            }
        }

        @Override
        public void close() throws Exception {
            flush();
        }

        private void flush() throws Exception {
            if (buffer.isEmpty()) {
                return;
            }
            String bulkBody = bulkBody(buffer);
            executeBulkWithRetry(bulkBody);
            buffer.clear();
            lastFlushAt = System.currentTimeMillis();
        }

        private String bulkBody(List<DataEntity> records) throws Exception {
            StringBuilder body = new StringBuilder();
            for (DataEntity record : records) {
                body.append(objectMapper.writeValueAsString(action(record))).append('\n');
                body.append(objectMapper.writeValueAsString(document(record))).append('\n');
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

        private void executeBulkWithRetry(String body) throws Exception {
            int maxAttempts = config.maxRetryCount() + 1;
            Exception lastFailure = null;
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    executeBulk(body);
                    return;
                } catch (Exception exception) {
                    lastFailure = exception;
                    if (attempt == maxAttempts || !isRetryable(exception)) {
                        throw exception;
                    }
                    Thread.sleep(Math.min(1_000L * attempt, 5_000L));
                }
            }
            if (lastFailure != null) {
                throw lastFailure;
            }
        }

        private boolean isRetryable(Exception exception) {
            String message = exception.getMessage();
            return message == null || !message.contains("status 4");
        }

        private void executeBulk(String body) throws Exception {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(firstHost() + "/_bulk"))
                    .header("Content-Type", "application/x-ndjson")
                    .POST(HttpRequest.BodyPublishers.ofString(body));
            applyAuth(builder);
            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException(
                        "Elasticsearch Sink bulk request failed with status " + response.statusCode() + ": "
                                + response.body());
            }
            JsonNode root = objectMapper.readTree(response.body());
            if (root.path("errors").asBoolean(false)) {
                throw new IllegalStateException("Elasticsearch Sink bulk request contains item errors: " + response.body());
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
