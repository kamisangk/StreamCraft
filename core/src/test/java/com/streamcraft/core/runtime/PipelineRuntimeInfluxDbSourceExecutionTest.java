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
import java.net.URLDecoder;
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

class PipelineRuntimeInfluxDbSourceExecutionTest {

    private static final Map<String, List<Map<String, Object>>> CAPTURED_RECORDS = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void influxDbSourceReadsQuerySeriesRows() throws Exception {
        try (FakeInfluxDbServer server = new FakeInfluxDbServer("""
                {
                  "results": [
                    {
                      "series": [
                        {
                          "name": "cpu",
                          "tags": {"host": "server-a"},
                          "columns": ["time", "usage", "region"],
                          "values": [
                            [1000, 0.64, "cn"],
                            [2000, 0.80, "us"]
                          ]
                        }
                      ]
                    }
                  ]
                }
                """)) {
            List<Map<String, Object>> records = execute(
                    "sink-influx-source",
                    definitionWithInfluxDbSource(server.baseUrl(), "sink-influx-source"),
                    "influxdb-source-full-test");

            assertEquals(2, records.size());
            assertEquals("cpu", records.get(0).get("_measurement"));
            assertEquals("server-a", records.get(0).get("host"));
            assertEquals(0.64, records.get(0).get("usage"));
            assertEquals("us", records.get(1).get("region"));
            assertEquals(List.of("/query"), server.requestPaths());
            assertTrue(server.decodedQueries().get(0).contains("db=metrics"));
            assertTrue(server.decodedQueries().get(0).contains("q=select * from cpu"));
            assertTrue(server.decodedQueries().get(0).contains("epoch=ms"));
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
                new InfluxDbSourceFactory(),
                new JdbcSourceFactory(),
                new CapturingKafkaSinkFactory(),
                new JdbcSinkFactory(),
                new ElasticsearchSinkFactory(),
                new InfluxDbSinkFactory(),
                new TransformOperatorFactory(),
                false,
                ExecutionMode.RUN);

        runtime.run(definition);
        env.execute(jobName);
        return CAPTURED_RECORDS.getOrDefault(sinkId, List.of());
    }

    private PipelineDefinition definitionWithInfluxDbSource(String url, String sinkId) {
        return new PipelineDefinition(
                "pipeline-influxdb-source",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "InfluxDB Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.INFLUXDB_SOURCE,
                                jsonNode("""
                                        {
                                          "url": "%s",
                                          "database": "metrics",
                                          "sql": "select * from cpu",
                                          "epoch": "ms",
                                          "readMode": "FULL",
                                          "idField": "region",
                                          "timestampField": "time"
                                        }
                                        """.formatted(url))),
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
            input.addSink(new RichSinkFunction<>() {
                @Override
                public void invoke(DataEntity value, Context context) {
                    CAPTURED_RECORDS
                            .computeIfAbsent(sinkId, ignored -> Collections.synchronizedList(new ArrayList<>()))
                            .add(value.fields());
                }
            }).name("capture-" + sinkId);
        }
    }

    private static final class FakeInfluxDbServer implements AutoCloseable {

        private final HttpServer server;
        private final String response;
        private final List<String> requestPaths = new CopyOnWriteArrayList<>();
        private final List<String> decodedQueries = new CopyOnWriteArrayList<>();

        private FakeInfluxDbServer(String response) throws IOException {
            this.response = response;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        private void handle(HttpExchange exchange) throws IOException {
            requestPaths.add(exchange.getRequestURI().getPath());
            decodedQueries.add(URLDecoder.decode(exchange.getRequestURI().getRawQuery(), StandardCharsets.UTF_8));
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
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

        private List<String> decodedQueries() {
            return decodedQueries;
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
