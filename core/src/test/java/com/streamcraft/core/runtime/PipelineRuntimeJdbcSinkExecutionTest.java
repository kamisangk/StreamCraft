package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class PipelineRuntimeJdbcSinkExecutionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void jdbcSinkInsertsConfiguredFieldsIntoTable() throws Exception {
        String url = createSinkDatabase();

        execute(
                definitionWithJdbcSink(url, "INSERT", List.of("id", "customer_name", "amount"), List.of(),
                        """
                                {"id":1,"customer_name":"Ada","amount":99.5}
                                """,
                        """
                                {"id":2,"customer_name":"Grace","amount":42.0}
                                """),
                "jdbc-sink-insert-test");

        List<Map<String, Object>> rows = readRows(url);
        assertEquals(2, rows.size());
        assertEquals(Map.of("id", 1L, "customer_name", "Ada", "amount", 99.5), rows.get(0));
        assertEquals(Map.of("id", 2L, "customer_name", "Grace", "amount", 42.0), rows.get(1));
    }

    @Test
    void jdbcSinkUpsertsByConfiguredKeyFields() throws Exception {
        String url = createSinkDatabase();
        insertExistingRow(url);

        execute(
                definitionWithJdbcSink(url, "UPSERT", List.of("id", "customer_name", "amount"), List.of("id"),
                        """
                                {"id":1,"customer_name":"Ada","amount":99.5}
                                """,
                        """
                                {"id":2,"customer_name":"Grace","amount":42.0}
                                """),
                "jdbc-sink-upsert-test");

        List<Map<String, Object>> rows = readRows(url);
        assertEquals(2, rows.size());
        assertEquals(Map.of("id", 1L, "customer_name", "Ada", "amount", 99.5), rows.get(0));
        assertEquals(Map.of("id", 2L, "customer_name", "Grace", "amount", 42.0), rows.get(1));
    }

    private void execute(PipelineDefinition definition, String jobName) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        new PipelineRuntime(env, true).run(definition);
        env.execute(jobName);
    }

    private String createSinkDatabase() throws Exception {
        String url = "jdbc:h2:mem:jdbc_sink_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table orders_out (
                        id bigint primary key,
                        customer_name varchar(64),
                        amount double precision
                    )
                    """);
        }
        return url;
    }

    private void insertExistingRow(String url) throws Exception {
        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {
            statement.execute("insert into orders_out (id, customer_name, amount) values (1, 'Old', 1.0)");
        }
    }

    private List<Map<String, Object>> readRows(String url) throws Exception {
        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select id, customer_name, amount from orders_out order by id")) {
            List<Map<String, Object>> rows = new ArrayList<>();
            while (resultSet.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id", resultSet.getLong("id"));
                row.put("customer_name", resultSet.getString("customer_name"));
                row.put("amount", resultSet.getDouble("amount"));
                rows.add(row);
            }
            return rows;
        }
    }

    private PipelineDefinition definitionWithJdbcSink(
            String url,
            String writeMode,
            List<String> fields,
            List<String> keyFields,
            String... samples) {
        return new PipelineDefinition(
                "pipeline-jdbc-sink",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                sourceConfig(samples)),
                        new PipelineNode(
                                "sink-1",
                                "Jdbc Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.JDBC_SINK,
                                jsonNode("""
                                        {
                                          "url": "%s",
                                          "driver": "org.h2.Driver",
                                          "tablePath": "orders_out",
                                          "writeMode": "%s",
                                          "fields": %s,
                                          "keyFields": %s,
                                          "batchSize": 2,
                                          "flushIntervalMillis": 1000
                                        }
                                        """.formatted(url, writeMode, jsonArray(fields), jsonArray(keyFields))))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));
    }

    private JsonNode sourceConfig(String... samples) {
        try {
            String sampleJson = "[" + String.join(",", List.of(samples).stream()
                    .map(sample -> {
                        try {
                            return objectMapper.writeValueAsString(sample.strip());
                        } catch (Exception exception) {
                            throw new IllegalStateException(exception);
                        }
                    })
                    .toList()) + "]";
            return objectMapper.readTree("""
                    {
                      "bootstrapServers": "127.0.0.1:9092",
                      "topics": ["input-topic"],
                      "groupId": "group-1",
                      "consumeMode": "earliest",
                      "authType": "NONE",
                      "format": "JSON",
                      "sampleData": %s
                    }
                    """.formatted(sampleJson));
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private String jsonArray(List<String> values) {
        try {
            return objectMapper.writeValueAsString(values);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
