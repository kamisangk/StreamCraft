package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import com.streamcraft.core.runtime.transform.TransformOperatorFactory;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.junit.jupiter.api.Test;

class PipelineRuntimeElasticsearchSourceExecutionTest {

    private static final Map<String, List<Map<String, Object>>> CAPTURED_RECORDS = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void elasticsearchSourceReadsFullScrollResult() throws Exception {
        try (FakeElasticsearchServer server = new FakeElasticsearchServer(List.of(
                """
                        {
                          "_scroll_id": "scroll-1",
                          "hits": {
                            "hits": [
                              {"_id": "1", "_source": {"id": 1, "customerName": "Ada", "amount": 99.5}},
                              {"_id": "2", "_source": {"id": 2, "customerName": "Grace", "amount": 42.0}}
                            ]
                          }
                        }
                        """,
                """
                        {
                          "_scroll_id": "scroll-1",
                          "hits": {"hits": []}
                        }
                        """))) {
            List<Map<String, Object>> records = execute(
                    "sink-es-full",
                    definitionWithElasticsearchSource(
                            """
                                    {
                                      "hosts": ["%s"],
                                      "index": "orders",
                                      "source": ["id", "customerName", "amount"],
                                      "readMode": "FULL",
                                      "scrollSize": 2,
                                      "scrollTime": "1m",
                                      "idField": "id"
                                    }
                                    """.formatted(server.baseUrl()),
                            "sink-es-full"),
                    "elasticsearch-source-full-test");

            assertEquals(2, records.size());
            assertEquals(1, records.get(0).get("id"));
            assertEquals("Ada", records.get(0).get("customerName"));
            assertEquals(42.0, records.get(1).get("amount"));
            assertTrue(server.requestBodies().get(0).contains("\"_source\":[\"id\",\"customerName\",\"amount\"]"));
        }
    }

    @Test
    void elasticsearchSourceReadsIncrementalRowsAfterInitialCursor() throws Exception {
        try (FakeElasticsearchServer server = new FakeElasticsearchServer(List.of(
                """
                        {
                          "_scroll_id": "scroll-2",
                          "hits": {
                            "hits": [
                              {"_id": "2", "_source": {"id": 2, "customerName": "Grace", "updated_at": 200}}
                            ]
                          }
                        }
                        """,
                """
                        {
                          "_scroll_id": "scroll-2",
                          "hits": {"hits": []}
                        }
                        """))) {
            List<Map<String, Object>> records = execute(
                    "sink-es-incremental",
                    definitionWithElasticsearchSource(
                            """
                                    {
                                      "hosts": ["%s"],
                                      "index": "orders",
                                      "readMode": "INCREMENTAL",
                                      "cursorField": "updated_at",
                                      "cursorType": "LONG",
                                      "initialCursorValue": "100",
                                      "pollIntervalMillis": 1,
                                      "maxPolls": 1,
                                      "idField": "id"
                                    }
                                    """.formatted(server.baseUrl()),
                            "sink-es-incremental"),
                    "elasticsearch-source-incremental-test");

            assertEquals(1, records.size());
            assertEquals(2, records.get(0).get("id"));
            assertEquals("Grace", records.get(0).get("customerName"));
            assertTrue(server.requestBodies().get(0).contains("\"range\":{\"updated_at\":{\"gt\":100}}"));
            assertTrue(server.requestBodies().get(0).contains("\"sort\":[{\"updated_at\":\"asc\"}]"));
        }
    }

    private List<Map<String, Object>> execute(String sinkId, PipelineDefinition definition, String jobName) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CAPTURED_RECORDS.clear();

        PipelineRuntime runtime = new PipelineRuntime(
                env,
                new KafkaSourceFactory(),
                new MockSourceFactory(),
                new ElasticsearchSourceFactory(),
                new JdbcSourceFactory(),
                new CapturingKafkaSinkFactory(),
                new JdbcSinkFactory(),
                new TransformOperatorFactory(),
                false,
                ExecutionMode.RUN);

        runtime.run(definition);
        env.execute(jobName);
        return CAPTURED_RECORDS.getOrDefault(sinkId, List.of());
    }

    private PipelineDefinition definitionWithElasticsearchSource(String sourceConfigJson, String sinkId) {
        return new PipelineDefinition(
                "pipeline-elasticsearch-source",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Elasticsearch Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.ELASTICSEARCH_SOURCE,
                                jsonNode(sourceConfigJson)),
                        new PipelineNode(
                                sinkId,
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("{}"))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", sinkId, "input-0")));
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static final class CapturingKafkaSinkFactory extends KafkaSinkFactory {

        @Override
        public void attach(DataStream<DataEntity> input, PipelineNode sinkNode) {
            String sinkId = sinkNode.id();
            CAPTURED_RECORDS.computeIfAbsent(sinkId, ignored -> Collections.synchronizedList(new ArrayList<>()));
            input.addSink(new RichSinkFunction<>() {
                @Override
                public void invoke(DataEntity value, Context context) {
                    CAPTURED_RECORDS.get(sinkId).add(Map.copyOf(value.fields()));
                }
            }).name("capture-" + sinkId);
        }
    }

    private static final class FakeElasticsearchServer implements AutoCloseable {

        private final HttpServer server;
        private final List<String> responses;
        private final List<String> requestBodies = new CopyOnWriteArrayList<>();
        private int responseIndex;

        private FakeElasticsearchServer(List<String> responses) throws IOException {
            this.responses = responses;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        private void handle(HttpExchange exchange) throws IOException {
            requestBodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            String response = responses.get(Math.min(responseIndex, responses.size() - 1));
            responseIndex++;
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }

        private String baseUrl() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
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
