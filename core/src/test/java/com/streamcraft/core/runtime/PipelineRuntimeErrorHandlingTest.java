package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

class PipelineRuntimeErrorHandlingTest {

    private static final Map<String, List<Map<String, Object>>> CAPTURED_RECORDS = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void mockSourceSkipsInvalidJsonSamplesAndKeepsValidRecords() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                new PipelineDefinition(
                        "pipeline-source-error",
                        List.of(
                                sourceNode(
                                        "JSON",
                                        "{bad-json}",
                                        "{\"status\":\"active\"}"),
                                sinkNode("sink-source-error")),
                        List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-source-error", "input-0"))),
                "sink-source-error",
                "mock-source-skip-invalid-json-test");

        assertEquals(1, sinkRecords.size());
        assertEquals("active", sinkRecords.get(0).get("status"));
    }

    @Test
    void castTransformKeepsOriginalRecordWhenRecordConversionFails() throws Exception {
        List<Map<String, Object>> sinkRecords = execute(
                new PipelineDefinition(
                        "pipeline-cast-error",
                        List.of(
                                sourceNode("JSON", "{\"parsed\":{\"bytes\":\"abc\"}}"),
                                castNode("parsed.bytes", "test", "LONG"),
                                sinkNode("sink-cast-error")),
                        List.of(
                                new PipelineEdge("edge-1", "source-1", "output-0", "cast-1", "input-0"),
                                new PipelineEdge("edge-2", "cast-1", "output-0", "sink-cast-error", "input-0"))),
                "sink-cast-error",
                "cast-transform-keep-original-on-error-test");

        assertEquals(1, sinkRecords.size());
        assertEquals(Map.of("bytes", "abc"), sinkRecords.get(0).get("parsed"));
        assertEquals(null, sinkRecords.get(0).get("test"));
    }

    @Test
    void filterTransformRoutesEvaluationFailuresToFalseBranch() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CAPTURED_RECORDS.clear();

        CapturingKafkaSinkFactory sinkFactory = new CapturingKafkaSinkFactory();
        PipelineRuntime runtime = new PipelineRuntime(
                env,
                new KafkaSourceFactory(),
                new MockSourceFactory(),
                sinkFactory,
                new TransformOperatorFactory(),
                true);

        runtime.run(new PipelineDefinition(
                "pipeline-filter-error",
                List.of(
                        sourceNode("JSON", "{\"value\":10}", "{\"value\":0}"),
                        filterNode("10 / value >= 1"),
                        sinkNode("sink-filter-true"),
                        sinkNode("sink-filter-false")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "filter-1", "input-0"),
                        new PipelineEdge("edge-2", "filter-1", "true", "sink-filter-true", "input-0"),
                        new PipelineEdge("edge-3", "filter-1", "false", "sink-filter-false", "input-0"))));
        env.execute("filter-transform-route-evaluation-failures-test");

        List<Map<String, Object>> trueSinkRecords = sinkFactory.recordsFor("sink-filter-true");
        List<Map<String, Object>> falseSinkRecords = sinkFactory.recordsFor("sink-filter-false");
        assertEquals(1, trueSinkRecords.size());
        assertEquals(10, trueSinkRecords.get(0).get("value"));
        assertEquals(1, falseSinkRecords.size());
        assertEquals(0, falseSinkRecords.get(0).get("value"));
    }

    private List<Map<String, Object>> execute(PipelineDefinition definition, String sinkId, String jobName) throws Exception {
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
        return CAPTURED_RECORDS.getOrDefault(sinkId, List.of());
    }

    private PipelineNode sourceNode(String format, String... sampleData) {
        String samplesJson = toStringArrayJson(sampleData);
        return new PipelineNode(
                "source-1",
                "Source",
                PipelineNodeType.SOURCE,
                PipelineOperator.KAFKA_SOURCE,
                jsonNode("""
                        {
                          "bootstrapServers": "127.0.0.1:9092",
                          "topics": ["input-topic"],
                          "groupId": "group-1",
                          "consumeMode": "earliest",
                          "authType": "NONE",
                          "format": "%s",
                          "sampleData": %s
                        }
                        """.formatted(format, samplesJson)));
    }

    private PipelineNode castNode(String inputField, String outputField, String targetType) {
        return new PipelineNode(
                "cast-1",
                "Cast",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.CAST,
                jsonNode("""
                        {
                          "inputField": "%s",
                          "outputField": "%s",
                          "targetType": "%s"
                        }
                        """.formatted(inputField, outputField, targetType)));
    }

    private PipelineNode filterNode(String condition) {
        return new PipelineNode(
                "filter-1",
                "Filter",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.FILTER,
                jsonNode("""
                        {
                          "condition": "%s"
                        }
                        """.formatted(condition)));
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

    private String toStringArrayJson(String... values) {
        try {
            return objectMapper.writeValueAsString(values);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static final class CapturingKafkaSinkFactory extends KafkaSinkFactory {

        @Override
        public void attach(DataStream<DataEntity> stream, PipelineNode sinkNode) {
            String sinkId = sinkNode.id();
            CAPTURED_RECORDS.computeIfAbsent(sinkId, ignored -> Collections.synchronizedList(new ArrayList<>()));
            stream.addSink(new CapturingSinkFunction(sinkId)).name("capture-" + sinkId);
        }

        private List<Map<String, Object>> recordsFor(String sinkId) {
            return CAPTURED_RECORDS.getOrDefault(sinkId, List.of());
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
