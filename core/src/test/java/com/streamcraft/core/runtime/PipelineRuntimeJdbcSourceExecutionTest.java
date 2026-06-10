package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import com.streamcraft.core.runtime.transform.TransformOperatorFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.junit.jupiter.api.Test;

class PipelineRuntimeJdbcSourceExecutionTest {

    private static final Map<String, List<Map<String, Object>>> CAPTURED_RECORDS = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void jdbcSourceReadsFullQueryResult() throws Exception {
        String url = createOrdersDatabase();

        List<Map<String, Object>> records = execute(
                "sink-jdbc-full",
                definitionWithJdbcSource(
                        "source-jdbc-full",
                        """
                                {
                                  "url": "%s",
                                  "driver": "org.h2.Driver",
                                  "query": "select id as \\"id\\", customer_name as \\"customerName\\", amount as \\"amount\\" from orders order by id",
                                  "readMode": "FULL",
                                  "idField": "id"
                                }
                                """.formatted(url),
                        "sink-jdbc-full"),
                "jdbc-source-full-test");

        assertEquals(3, records.size());
        assertEquals(1L, records.get(0).get("id"));
        assertEquals("Ada", records.get(0).get("customerName"));
        assertEquals(99.50, records.get(0).get("amount"));
        assertEquals(3L, records.get(2).get("id"));
    }

    @Test
    void jdbcSourceReadsIncrementalRowsAfterInitialCursor() throws Exception {
        String url = createOrdersDatabase();

        List<Map<String, Object>> records = execute(
                "sink-jdbc-incremental",
                definitionWithJdbcSource(
                        "source-jdbc-incremental",
                        """
                                {
                                  "url": "%s",
                                  "driver": "org.h2.Driver",
                                  "query": "select id, customer_name as \\"customerName\\" from orders",
                                  "readMode": "INCREMENTAL",
                                  "cursorField": "ID",
                                  "cursorType": "LONG",
                                  "initialCursorValue": "1",
                                  "pollIntervalMillis": 1,
                                  "maxPolls": 1,
                                  "idField": "ID"
                                }
                                """.formatted(url),
                        "sink-jdbc-incremental"),
                "jdbc-source-incremental-test");

        assertEquals(2, records.size());
        assertEquals(2L, records.get(0).get("ID"));
        assertEquals("Grace", records.get(0).get("customerName"));
        assertEquals(3L, records.get(1).get("ID"));
    }

    private String createOrdersDatabase() throws Exception {
        String url = "jdbc:h2:mem:orders_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table orders (
                        id bigint primary key,
                        customer_name varchar(64),
                        amount double precision
                    )
                    """);
            statement.execute("insert into orders (id, customer_name, amount) values (1, 'Ada', 99.5)");
            statement.execute("insert into orders (id, customer_name, amount) values (2, 'Grace', 42.0)");
            statement.execute("insert into orders (id, customer_name, amount) values (3, 'Linus', 7.25)");
        }
        return url;
    }

    private List<Map<String, Object>> execute(String sinkId, PipelineDefinition definition, String jobName) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CAPTURED_RECORDS.clear();

        PipelineRuntime runtime = new PipelineRuntime(
                env,
                new KafkaSourceFactory(),
                new MockSourceFactory(),
                new JdbcSourceFactory(),
                new CapturingKafkaSinkFactory(),
                new TransformOperatorFactory(),
                false);

        runtime.run(definition);
        env.execute(jobName);
        return CAPTURED_RECORDS.getOrDefault(sinkId, List.of());
    }

    private PipelineDefinition definitionWithJdbcSource(String sourceId, String sourceConfigJson, String sinkId) {
        return new PipelineDefinition(
                "pipeline-jdbc-source",
                List.of(
                        new PipelineNode(
                                sourceId,
                                "Jdbc Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.JDBC_SOURCE,
                                jsonNode(sourceConfigJson)),
                        new PipelineNode(
                                sinkId,
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("{}"))),
                List.of(new PipelineEdge("edge-1", sourceId, "output-0", sinkId, "input-0")));
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
        public void attach(org.apache.flink.streaming.api.datastream.DataStream<com.streamcraft.core.model.DataEntity> input,
                           PipelineNode sinkNode) {
            String sinkId = sinkNode.id();
            input.addSink(new RichSinkFunction<>() {
                @Override
                public void invoke(com.streamcraft.core.model.DataEntity value, Context context) {
                    CAPTURED_RECORDS
                            .computeIfAbsent(sinkId, ignored -> java.util.Collections.synchronizedList(new java.util.ArrayList<>()))
                            .add(value.fields());
                }
            }).name("capture-" + sinkId);
        }
    }
}
