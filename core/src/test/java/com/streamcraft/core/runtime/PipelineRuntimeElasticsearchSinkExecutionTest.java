package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class PipelineRuntimeElasticsearchSinkExecutionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void elasticsearchSinkWritesBulkIndexActionsWithPrimaryKeysAndDynamicIndex() throws Exception {
        try (FakeElasticsearchServer server = new FakeElasticsearchServer()) {
            execute(definitionWithElasticsearchSink(server.baseUrl()), "elasticsearch-sink-bulk-test");

            assertEquals(List.of("/_bulk"), server.requestPaths());
            String body = server.requestBodies().get(0);
            assertTrue(body.contains("{\"index\":{\"_index\":\"orders-cn\",\"_id\":\"1\"}}"));
            assertTrue(body.contains("{\"id\":1,\"customerName\":\"Ada\",\"amount\":99.5}"));
            assertTrue(body.contains("{\"index\":{\"_index\":\"orders-us\",\"_id\":\"2\"}}"));
            assertTrue(body.endsWith("\n"));
        }
    }

    private void execute(PipelineDefinition definition, String jobName) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        new PipelineRuntime(env, true).run(definition);
        env.execute(jobName);
    }

    private PipelineDefinition definitionWithElasticsearchSink(String host) {
        return new PipelineDefinition(
                "pipeline-elasticsearch-sink",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                sourceConfig()),
                        new PipelineNode(
                                "sink-1",
                                "Elasticsearch Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.ELASTICSEARCH_SINK,
                                jsonNode("""
                                        {
                                          "hosts": ["%s"],
                                          "index": "orders-${region}",
                                          "primaryKeys": ["id"],
                                          "fields": ["id", "customerName", "amount"],
                                          "maxBatchSize": 2,
                                          "maxRetryCount": 1
                                        }
                                        """.formatted(host)))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));
    }

    private JsonNode sourceConfig() {
        return jsonNode("""
                {
                  "bootstrapServers": "127.0.0.1:9092",
                  "topics": ["input-topic"],
                  "groupId": "group-1",
                  "consumeMode": "earliest",
                  "authType": "NONE",
                  "format": "JSON",
                  "sampleData": [
                    "{\\"id\\":1,\\"region\\":\\"cn\\",\\"customerName\\":\\"Ada\\",\\"amount\\":99.5}",
                    "{\\"id\\":2,\\"region\\":\\"us\\",\\"customerName\\":\\"Grace\\",\\"amount\\":42.0}"
                  ]
                }
                """);
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static final class FakeElasticsearchServer implements AutoCloseable {

        private final HttpServer server;
        private final List<String> requestPaths = new CopyOnWriteArrayList<>();
        private final List<String> requestBodies = new CopyOnWriteArrayList<>();

        private FakeElasticsearchServer() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        private void handle(HttpExchange exchange) throws IOException {
            requestPaths.add(exchange.getRequestURI().getPath());
            requestBodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] bytes = "{\"errors\":false,\"items\":[]}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }

        private String baseUrl() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private List<String> requestPaths() {
            return requestPaths;
        }

        private List<String> requestBodies() {
            return requestBodies;
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
