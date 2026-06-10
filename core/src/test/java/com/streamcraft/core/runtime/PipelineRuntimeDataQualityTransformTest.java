package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import com.streamcraft.core.runtime.transform.TransformOperatorFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.junit.jupiter.api.Test;

class PipelineRuntimeDataQualityTransformTest {

    private static final Map<String, List<Map<String, Object>>> CAPTURED_RECORDS = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void dirtyPortReceivesInvalidRecordsAndCleanPortKeepsValidRecords() throws Exception {
        PipelineDefinition definition = new PipelineDefinition(
                "pipeline-data-quality-dirty",
                List.of(
                        sourceNode(
                                "{\"amount\":20,\"status\":\"ok\"}",
                                "{\"status\":\"missing\"}"),
                        dataQualityNode("""
                                {
                                  "mode": "DIRTY_PORT",
                                  "errorField": "_streamcraft_quality_errors",
                                  "rules": [
                                    {
                                      "field": "amount",
                                      "ruleType": "NOT_NULL"
                                    },
                                    {
                                      "field": "amount",
                                      "ruleType": "TYPE",
                                      "valueType": "INTEGER"
                                    },
                                    {
                                      "field": "amount",
                                      "ruleType": "RANGE",
                                      "min": 10
                                    }
                                  ]
                                }
                                """),
                        sinkNode("sink-clean"),
                        sinkNode("sink-dirty")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "quality-1", "input-0"),
                        new PipelineEdge("edge-2", "quality-1", "output-0", "sink-clean", "input-0"),
                        new PipelineEdge("edge-3", "quality-1", "dirty", "sink-dirty", "input-0")));

        execute(definition, "data-quality-dirty-port-test");

        List<Map<String, Object>> cleanRecords = CAPTURED_RECORDS.getOrDefault("sink-clean", List.of());
        List<Map<String, Object>> dirtyRecords = CAPTURED_RECORDS.getOrDefault("sink-dirty", List.of());

        assertEquals(1, cleanRecords.size());
        assertEquals(20, cleanRecords.get(0).get("amount"));
        assertEquals("ok", cleanRecords.get(0).get("status"));

        assertEquals(1, dirtyRecords.size());
        assertNotNull(dirtyRecords.get(0).get("_streamcraft_quality_errors"));
    }

    @Test
    void markErrorKeepsInvalidRecordsOnMainOutput() throws Exception {
        PipelineDefinition definition = new PipelineDefinition(
                "pipeline-data-quality-mark-error",
                List.of(
                        sourceNode(
                                "{\"amount\":20,\"status\":\"ok\"}",
                                "{\"status\":\"missing\"}"),
                        dataQualityNode("""
                                {
                                  "mode": "MARK_ERROR",
                                  "errorField": "quality.errors",
                                  "rules": [
                                    {
                                      "field": "amount",
                                      "ruleType": "NOT_NULL"
                                    },
                                    {
                                      "field": "amount",
                                      "ruleType": "TYPE",
                                      "valueType": "INTEGER"
                                    },
                                    {
                                      "field": "amount",
                                      "ruleType": "RANGE",
                                      "min": 10
                                    },
                                    {
                                      "field": "status",
                                      "ruleType": "LENGTH",
                                      "minLength": 2,
                                      "maxLength": 4,
                                      "customMessage": "status length is invalid"
                                    }
                                  ]
                                }
                                """),
                        sinkNode("sink-mark-error")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "quality-1", "input-0"),
                        new PipelineEdge("edge-2", "quality-1", "output-0", "sink-mark-error", "input-0")));

        execute(definition, "data-quality-mark-error-test");

        List<Map<String, Object>> records = CAPTURED_RECORDS.getOrDefault("sink-mark-error", List.of());

        assertEquals(2, records.size());
        assertEquals(20, records.get(0).get("amount"));
        assertEquals("ok", records.get(0).get("status"));
        assertEquals(List.of(), records.get(0).getOrDefault("quality", List.of()));
        assertNotNull(records.get(1).get("quality"));
        assertTrue(qualityErrors(records.get(1)).contains("status length is invalid"));
    }

    @Test
    void discardDropsInvalidRecords() throws Exception {
        PipelineDefinition definition = new PipelineDefinition(
                "pipeline-data-quality-discard",
                List.of(
                        sourceNode(
                                "{\"amount\":20,\"status\":\"ok\"}",
                                "{\"status\":\"missing\"}"),
                        dataQualityNode("""
                                {
                                  "mode": "DISCARD",
                                  "rules": [
                                    {
                                      "field": "amount",
                                      "ruleType": "NOT_NULL"
                                    },
                                    {
                                      "field": "amount",
                                      "ruleType": "TYPE",
                                      "valueType": "INTEGER"
                                    },
                                    {
                                      "field": "amount",
                                      "ruleType": "RANGE",
                                      "min": 10
                                    }
                                  ]
                                }
                                """),
                        sinkNode("sink-discard")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "quality-1", "input-0"),
                        new PipelineEdge("edge-2", "quality-1", "output-0", "sink-discard", "input-0")));

        execute(definition, "data-quality-discard-test");

        List<Map<String, Object>> records = CAPTURED_RECORDS.getOrDefault("sink-discard", List.of());
        assertEquals(1, records.size());
        assertEquals(20, records.get(0).get("amount"));
    }

    @Test
    void failModeThrowsWhenRecordIsInvalid() {
        PipelineDefinition definition = new PipelineDefinition(
                "pipeline-data-quality-fail",
                List.of(
                        sourceNode(
                                "{\"amount\":20,\"status\":\"ok\"}",
                                "{\"amount\":2,\"status\":\"bad\"}"),
                        dataQualityNode("""
                                {
                                  "mode": "FAIL",
                                  "rules": [
                                    {
                                      "field": "amount",
                                      "ruleType": "RANGE",
                                      "min": 10,
                                      "customMessage": "amount is below warehouse minimum"
                                    }
                                  ]
                                }
                                """),
                        sinkNode("sink-fail")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "quality-1", "input-0"),
                        new PipelineEdge("edge-2", "quality-1", "output-0", "sink-fail", "input-0")));

        Exception exception = assertThrows(
                Exception.class,
                () -> execute(definition, "data-quality-fail-test"));

        assertTrue(hasMessage(exception, "amount is below warehouse minimum"));
    }

    private void execute(PipelineDefinition definition, String jobName) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CAPTURED_RECORDS.clear();

        PipelineRuntime runtime = new PipelineRuntime(
                env,
                new KafkaSourceFactory(),
                new MockSourceFactory(),
                new CapturingKafkaSinkFactory(),
                new TransformOperatorFactory(),
                true);

        runtime.run(definition);
        env.execute(jobName);
    }

    private PipelineNode sourceNode(String... sampleData) {
        ObjectNode config = objectMapper.createObjectNode()
                .put("bootstrapServers", "127.0.0.1:9092")
                .put("groupId", "group-1")
                .put("consumeMode", "earliest")
                .put("authType", "NONE")
                .put("format", "JSON");
        config.putArray("topics").add("input-topic");
        ArrayNode samples = config.putArray("sampleData");
        for (String sample : sampleData) {
            samples.add(sample);
        }
        return new PipelineNode(
                "source-1",
                "Source",
                PipelineNodeType.SOURCE,
                PipelineOperator.KAFKA_SOURCE,
                config);
    }

    private PipelineNode dataQualityNode(String configJson) {
        return new PipelineNode(
                "quality-1",
                "Data quality",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.valueOf("DATA_QUALITY"),
                jsonNode(configJson));
    }

    private PipelineNode sinkNode(String id) {
        return new PipelineNode(
                id,
                "Sink " + id,
                PipelineNodeType.SINK,
                PipelineOperator.KAFKA_SINK,
                jsonNode("""
                        {
                          "bootstrapServers": "127.0.0.1:9092",
                          "topic": "output-topic",
                          "deliveryGuarantee": "AT_LEAST_ONCE",
                          "authType": "NONE",
                          "format": "JSON"
                        }
                        """));
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> qualityErrors(Map<String, Object> record) {
        Map<String, Object> quality = (Map<String, Object>) record.get("quality");
        return (List<String>) quality.get("errors");
    }

    private boolean hasMessage(Throwable throwable, String expected) {
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(expected)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static final class CapturingKafkaSinkFactory extends KafkaSinkFactory {

        @Override
        public void attach(DataStream<DataEntity> stream, PipelineNode sinkNode) {
            String sinkId = sinkNode.id();
            CAPTURED_RECORDS.computeIfAbsent(sinkId, ignored -> Collections.synchronizedList(new ArrayList<>()));
            stream.addSink(new CapturingSinkFunction(sinkId)).name("capture-" + sinkId);
        }
    }

    private static final class CapturingSinkFunction extends RichSinkFunction<DataEntity> {

        private static final long serialVersionUID = 1L;
        private final String sinkId;

        private CapturingSinkFunction(String sinkId) {
            this.sinkId = sinkId;
        }

        @Override
        public void invoke(DataEntity value, Context context) {
            CAPTURED_RECORDS.get(sinkId).add(Map.copyOf(value.fields()));
        }
    }
}
