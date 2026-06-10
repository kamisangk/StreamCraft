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
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class PipelineRuntimeInfluxDbSinkExecutionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void influxDbSinkWritesLineProtocolBatches() throws Exception {
        try (FakeInfluxDbServer server = new FakeInfluxDbServer()) {
            execute(definitionWithInfluxDbSink(server.baseUrl()), "influxdb-sink-write-test");

            assertEquals(List.of("/write"), server.requestPaths());
            assertTrue(server.decodedQueries().get(0).contains("db=metrics"));
            assertTrue(server.decodedQueries().get(0).contains("precision=ms"));
            String body = server.requestBodies().get(0);
            assertTrue(body.contains("cpu_cn,host=server\\ a,region=cn usage=0.64 1000"));
            assertTrue(body.contains("cpu_us,host=server\\ b,region=us usage=0.8 2000"));
            assertTrue(body.endsWith("\n"));
        }
    }

    private void execute(PipelineDefinition definition, String jobName) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        new PipelineRuntime(env, true).run(definition);
        env.execute(jobName);
    }

    private PipelineDefinition definitionWithInfluxDbSink(String url) {
        return new PipelineDefinition(
                "pipeline-influxdb-sink",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                sourceConfig()),
                        new PipelineNode(
                                "sink-1",
                                "InfluxDB Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.INFLUXDB_SINK,
                                jsonNode("""
                                        {
                                          "url": "%s",
                                          "database": "metrics",
                                          "measurement": "cpu_${region}",
                                          "keyTime": "event_time",
                                          "keyTags": ["host", "region"],
                                          "fields": ["usage"],
                                          "batchSize": 2,
                                          "maxRetries": 1
                                        }
                                        """.formatted(url)))),
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
                    "{\\"event_time\\":1000,\\"host\\":\\"server a\\",\\"region\\":\\"cn\\",\\"usage\\":0.64}",
                    "{\\"event_time\\":2000,\\"host\\":\\"server b\\",\\"region\\":\\"us\\",\\"usage\\":0.8}"
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

    private static final class FakeInfluxDbServer implements AutoCloseable {

        private final HttpServer server;
        private final List<String> requestPaths = new CopyOnWriteArrayList<>();
        private final List<String> decodedQueries = new CopyOnWriteArrayList<>();
        private final List<String> requestBodies = new CopyOnWriteArrayList<>();

        private FakeInfluxDbServer() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        private void handle(HttpExchange exchange) throws IOException {
            requestPaths.add(exchange.getRequestURI().getPath());
            decodedQueries.add(URLDecoder.decode(exchange.getRequestURI().getRawQuery(), StandardCharsets.UTF_8));
            requestBodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        }

        private String baseUrl() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private List<String> requestPaths() {
            return requestPaths;
        }

        private List<String> decodedQueries() {
            return decodedQueries;
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
