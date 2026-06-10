package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

class PipelineRuntimeFilterBranchingTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Map<String, List<Map<String, Object>>> CAPTURED_RECORDS = new ConcurrentHashMap<>();

    @Test
    void routesFilterTrueAndFalseOutputsToMatchingSinkBranches() throws Exception {
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

        runtime.run(definitionWithTrueAndFalseSinks());
        env.execute("filter-branching-test");

        List<Map<String, Object>> trueSinkRecords = sinkFactory.recordsFor("sink-true");
        List<Map<String, Object>> falseSinkRecords = sinkFactory.recordsFor("sink-false");

        assertEquals(1, trueSinkRecords.size());
        assertEquals(1, falseSinkRecords.size());
        assertEquals("active", trueSinkRecords.get(0).get("status"));
        assertEquals("inactive", falseSinkRecords.get(0).get("status"));
    }

    @Test
    void allowsUnconnectedFilterFalseBranchAndDropsUnmatchedRecords() throws Exception {
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

        runtime.run(definitionWithOnlyTrueSink());
        env.execute("filter-true-only-branch-test");

        List<Map<String, Object>> trueSinkRecords = sinkFactory.recordsFor("sink-true-only");
        assertEquals(1, trueSinkRecords.size());
        assertEquals("active", trueSinkRecords.get(0).get("status"));
    }

    @Test
    void preservesOutput0BehaviorForNonFilterSingleOutputTransforms() throws Exception {
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

        runtime.run(definitionWithPutTransformSink());
        env.execute("put-single-output-smoke-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-put");
        assertEquals(2, sinkRecords.size());
        assertEquals("kept", sinkRecords.get(0).get("branch"));
        assertEquals("kept", sinkRecords.get(1).get("branch"));
    }

    @Test
    void putTransformCopiesReferencedFieldWhenValueUsesReferenceSyntax() throws Exception {
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

        runtime.run(definitionWithReferencedPutTransformSink());
        env.execute("put-reference-value-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-put-reference");
        assertEquals(2, sinkRecords.size());
        assertEquals("active", sinkRecords.get(0).get("branch"));
        assertEquals("inactive", sinkRecords.get(1).get("branch"));
    }

    @Test
    void putTransformKeepsLiteralStringWhenValueDoesNotUseReferenceSyntax() throws Exception {
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

        runtime.run(definitionWithLiteralPutTransformSink());
        env.execute("put-literal-value-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-put-literal");
        assertEquals(2, sinkRecords.size());
        assertEquals("status", sinkRecords.get(0).get("branch"));
        assertEquals("status", sinkRecords.get(1).get("branch"));
    }

    @Test
    void putTransformBuildsNestedObjectWhenFieldUsesDotPath() throws Exception {
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

        runtime.run(definitionWithNestedPutTransformSink());
        env.execute("put-nested-field-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-put-nested");
        assertEquals(1, sinkRecords.size());
        assertEquals("1", sinkRecords.get(0).get("test1"));
        assertEquals(Map.of("test", "1"), sinkRecords.get(0).get("test2"));
    }

    @Test
    void putTransformInterpolatesReferencedFieldsInsideTemplateText() throws Exception {
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

        runtime.run(definitionWithTemplatePutTransformSink());
        env.execute("put-template-value-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-put-template");
        assertEquals(1, sinkRecords.size());
        assertEquals("23333", sinkRecords.get(0).get("test"));
        assertEquals("23333拼接测试23333", sinkRecords.get(0).get("test1"));
    }

    @Test
    void pruneTransformRemovesNestedFieldWhenFieldUsesDotPath() throws Exception {
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

        runtime.run(definitionWithNestedPruneTransformSink());
        env.execute("prune-nested-field-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-prune-nested");
        assertEquals(1, sinkRecords.size());
        assertEquals(null, sinkRecords.get(0).get("test1"));
    }

    @Test
    void renameTransformRenamesNestedFieldWhenMappingUsesDotPath() throws Exception {
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

        runtime.run(definitionWithNestedRenameTransformSink());
        env.execute("rename-nested-field-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-rename-nested");
        assertEquals(1, sinkRecords.size());
        assertEquals(Map.of("test3", "value"), sinkRecords.get(0).get("test1"));
    }

    @Test
    void deserializeTransformWritesParsedObjectToTargetField() throws Exception {
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

        runtime.run(definitionWithDeserializeTransformSink("test"));
        env.execute("deserialize-target-field-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-deserialize");
        assertEquals(1, sinkRecords.size());
        assertEquals("{\"name\":\"alice\",\"age\":18,\"city\":\"shanghai\"}", sinkRecords.get(0).get("payload"));
        assertEquals(Map.of("name", "alice", "age", 18, "city", "shanghai"), sinkRecords.get(0).get("test"));
        assertEquals(null, sinkRecords.get(0).get("name"));
    }

    @Test
    void deserializeTransformWritesParsedObjectToNestedTargetField() throws Exception {
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

        runtime.run(definitionWithDeserializeTransformSink("test.info"));
        env.execute("deserialize-nested-target-field-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-deserialize");
        assertEquals(1, sinkRecords.size());
        assertEquals(
                Map.of("info", Map.of("name", "alice", "age", 18, "city", "shanghai")),
                sinkRecords.get(0).get("test"));
    }

    @Test
    void deserializeTransformKeepsOriginalRecordWhenJsonIsInvalid() throws Exception {
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

        runtime.run(definitionWithInvalidDeserializeTransformSink("test"));
        env.execute("deserialize-invalid-json-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-deserialize-invalid");
        assertEquals(1, sinkRecords.size());
        assertEquals("{bad-json}", sinkRecords.get(0).get("payload"));
        assertEquals(null, sinkRecords.get(0).get("test"));
    }

    @Test
    void deserializeTransformParsesKvIntoTargetField() throws Exception {
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

        runtime.run(definitionWithDeserializeTransformSink(
                sourceNodeWithPayload("name=alice,age=18"),
                "parsed",
                "KV",
                List.of()));
        env.execute("deserialize-kv-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-deserialize");
        assertEquals(1, sinkRecords.size());
        assertEquals(Map.of("name", "alice", "age", "18"), sinkRecords.get(0).get("parsed"));
    }

    @Test
    void deserializeTransformParsesCsvIntoTargetField() throws Exception {
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

        runtime.run(definitionWithDeserializeTransformSink(
                sourceNodeWithPayload("alice,18,shanghai"),
                "parsed",
                "CSV",
                List.of("name", "age", "city")));
        env.execute("deserialize-csv-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-deserialize");
        assertEquals(1, sinkRecords.size());
        assertEquals(
                Map.of("name", "alice", "age", "18", "city", "shanghai"),
                sinkRecords.get(0).get("parsed"));
    }

    @Test
    void deserializeTransformParsesCsvIntoTargetFieldWithCustomDelimiter() throws Exception {
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

        runtime.run(definitionWithDeserializeTransformSink(
                sourceNodeWithPayload("alice|18|shanghai"),
                "parsed",
                "CSV",
                List.of("name", "age", "city"),
                "|"));
        env.execute("deserialize-csv-custom-delimiter-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-deserialize");
        assertEquals(1, sinkRecords.size());
        assertEquals(
                Map.of("name", "alice", "age", "18", "city", "shanghai"),
                sinkRecords.get(0).get("parsed"));
    }

    @Test
    void deserializeTransformParsesXmlIntoTargetField() throws Exception {
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

        runtime.run(definitionWithDeserializeTransformSink(
                sourceNodeWithPayload("<root><name>alice</name><age>18</age></root>"),
                "parsed",
                "XML",
                List.of()));
        env.execute("deserialize-xml-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-deserialize");
        assertEquals(1, sinkRecords.size());
        assertEquals(Map.of("name", "alice", "age", "18"), sinkRecords.get(0).get("parsed"));
    }

    @Test
    void serializeTransformFormatsKvIntoTargetField() throws Exception {
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

        runtime.run(definitionWithSerializeTransformSink(
                sourceNode(),
                List.of("status", "value"),
                "output",
                "KV"));
        env.execute("serialize-kv-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-serialize");
        assertEquals(2, sinkRecords.size());
        assertEquals("status=active&value=10", sinkRecords.get(0).get("output"));
    }

    @Test
    void serializeTransformFormatsKvFromObjectFieldIntoTargetField() throws Exception {
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

        runtime.run(definitionWithSerializeTransformSink(
                sourceNodeWithObjectField(),
                List.of("test"),
                "output",
                "KV"));
        env.execute("serialize-kv-object-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-serialize");
        assertEquals(1, sinkRecords.size());
        assertEquals("name=alice&age=18&city=shanghai", sinkRecords.get(0).get("output"));
    }

    @Test
    void serializeTransformFormatsCsvIntoTargetField() throws Exception {
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

        runtime.run(definitionWithSerializeTransformSink(
                sourceNode(),
                List.of("status", "value"),
                "output",
                "CSV"));
        env.execute("serialize-csv-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-serialize");
        assertEquals(2, sinkRecords.size());
        assertEquals("active,10", sinkRecords.get(0).get("output"));
    }

    @Test
    void serializeTransformFormatsCsvFromObjectFieldIntoTargetField() throws Exception {
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

        runtime.run(definitionWithSerializeTransformSink(
                sourceNodeWithObjectField(),
                List.of("test"),
                "output",
                "CSV"));
        env.execute("serialize-csv-object-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-serialize");
        assertEquals(1, sinkRecords.size());
        assertEquals("alice,18,shanghai", sinkRecords.get(0).get("output"));
    }

    @Test
    void serializeTransformFormatsCsvWithCustomDelimiterIntoTargetField() throws Exception {
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

        runtime.run(definitionWithSerializeTransformSink(
                sourceNodeWithObjectField(),
                List.of("test"),
                "output",
                "CSV",
                "|"));
        env.execute("serialize-csv-custom-delimiter-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-serialize");
        assertEquals(1, sinkRecords.size());
        assertEquals("alice|18|shanghai", sinkRecords.get(0).get("output"));
    }

    @Test
    void serializeTransformFormatsXmlIntoTargetField() throws Exception {
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

        runtime.run(definitionWithSerializeTransformSink(
                sourceNode(),
                List.of("status", "value"),
                "output",
                "XML"));
        env.execute("serialize-xml-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-serialize");
        assertEquals(2, sinkRecords.size());
        assertEquals("<root><status>active</status><value>10</value></root>", sinkRecords.get(0).get("output"));
    }

    @Test
    void serializeTransformFormatsXmlFromObjectFieldIntoTargetField() throws Exception {
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

        runtime.run(definitionWithSerializeTransformSink(
                sourceNodeWithObjectField(),
                List.of("test"),
                "output",
                "XML"));
        env.execute("serialize-xml-object-test");

        List<Map<String, Object>> sinkRecords = sinkFactory.recordsFor("sink-serialize");
        assertEquals(1, sinkRecords.size());
        assertEquals("<test><name>alice</name><age>18</age><city>shanghai</city></test>", sinkRecords.get(0).get("output"));
    }

    @Test
    private PipelineDefinition definitionWithTrueAndFalseSinks() {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNode(), filterNode(), sinkNode("sink-true"), sinkNode("sink-false")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "filter-1", "input-0"),
                        new PipelineEdge("edge-2", "filter-1", "true", "sink-true", "input-0"),
                        new PipelineEdge("edge-3", "filter-1", "false", "sink-false", "input-0")));
    }

    private PipelineDefinition definitionWithOnlyTrueSink() {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNode(), filterNode(), sinkNode("sink-true-only")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "filter-1", "input-0"),
                        new PipelineEdge("edge-2", "filter-1", "true", "sink-true-only", "input-0")));
    }

    private PipelineDefinition definitionWithPutTransformSink() {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNode(), putNode(), sinkNode("sink-put")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "put-1", "input-0"),
                        new PipelineEdge("edge-2", "put-1", "output-0", "sink-put", "input-0")));
    }

    private PipelineDefinition definitionWithReferencedPutTransformSink() {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNode(), putNodeWithValue("${status}"), sinkNode("sink-put-reference")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "put-1", "input-0"),
                        new PipelineEdge("edge-2", "put-1", "output-0", "sink-put-reference", "input-0")));
    }

    private PipelineDefinition definitionWithLiteralPutTransformSink() {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNode(), putNodeWithValue("status"), sinkNode("sink-put-literal")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "put-1", "input-0"),
                        new PipelineEdge("edge-2", "put-1", "output-0", "sink-put-literal", "input-0")));
    }

    private PipelineDefinition definitionWithNestedPutTransformSink() {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNodeWithTestField(), putNodeWithFieldAndValue("test2.test", "${test1}"), sinkNode("sink-put-nested")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "put-1", "input-0"),
                        new PipelineEdge("edge-2", "put-1", "output-0", "sink-put-nested", "input-0")));
    }

    private PipelineDefinition definitionWithTemplatePutTransformSink() {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNodeWithTemplateField(), putNodeWithFieldAndValue("test1", "${test}拼接测试${test}"), sinkNode("sink-put-template")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "put-1", "input-0"),
                        new PipelineEdge("edge-2", "put-1", "output-0", "sink-put-template", "input-0")));
    }

    private PipelineDefinition definitionWithNestedPruneTransformSink() {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNodeWithNestedField(), pruneNodeWithField("test1.test2"), sinkNode("sink-prune-nested")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "prune-1", "input-0"),
                        new PipelineEdge("edge-2", "prune-1", "output-0", "sink-prune-nested", "input-0")));
    }

    private PipelineDefinition definitionWithNestedRenameTransformSink() {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNodeWithNestedField(), renameNodeWithMapping("test1.test2", "test1.test3"), sinkNode("sink-rename-nested")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "rename-1", "input-0"),
                        new PipelineEdge("edge-2", "rename-1", "output-0", "sink-rename-nested", "input-0")));
    }

    private PipelineDefinition definitionWithDeserializeTransformSink(String targetField) {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(
                        sourceNodeWithDeserializePayload(),
                        deserializeNode("payload", targetField, "JSON", List.of()),
                        sinkNode("sink-deserialize")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "deserialize-1", "input-0"),
                        new PipelineEdge("edge-2", "deserialize-1", "output-0", "sink-deserialize", "input-0")));
    }

    private PipelineDefinition definitionWithInvalidDeserializeTransformSink(String targetField) {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(
                        sourceNodeWithInvalidDeserializePayload(),
                        deserializeNode("payload", targetField, "JSON", List.of()),
                        sinkNode("sink-deserialize-invalid")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "deserialize-1", "input-0"),
                        new PipelineEdge("edge-2", "deserialize-1", "output-0", "sink-deserialize-invalid", "input-0")));
    }

    private PipelineDefinition definitionWithDeserializeTransformSink(
            PipelineNode sourceNode,
            String targetField,
            String format,
            List<String> fieldNames) {
        return definitionWithDeserializeTransformSink(sourceNode, targetField, format, fieldNames, null);
    }

    private PipelineDefinition definitionWithDeserializeTransformSink(
            PipelineNode sourceNode,
            String targetField,
            String format,
            List<String> fieldNames,
            String delimiter) {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(
                        sourceNode,
                        deserializeNode("payload", targetField, format, fieldNames, delimiter),
                        sinkNode("sink-deserialize")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "deserialize-1", "input-0"),
                        new PipelineEdge("edge-2", "deserialize-1", "output-0", "sink-deserialize", "input-0")));
    }

    private PipelineDefinition definitionWithSerializeTransformSink(
            PipelineNode sourceNode,
            List<String> sourceFields,
            String targetField,
            String format) {
        return definitionWithSerializeTransformSink(sourceNode, sourceFields, targetField, format, null);
    }

    private PipelineDefinition definitionWithSerializeTransformSink(
            PipelineNode sourceNode,
            List<String> sourceFields,
            String targetField,
            String format,
            String delimiter) {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(
                        sourceNode,
                        serializeNode(sourceFields, targetField, format, delimiter),
                        sinkNode("sink-serialize")),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "serialize-1", "input-0"),
                        new PipelineEdge("edge-2", "serialize-1", "output-0", "sink-serialize", "input-0")));
    }

    private PipelineNode sourceNode() {
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
                          "format": "JSON",
                          "sampleData": [
                            "{\\\"status\\\":\\\"active\\\",\\\"value\\\":10}",
                            "{\\\"status\\\":\\\"inactive\\\",\\\"value\\\":1}"
                          ]
                        }
                        """));
    }

    private PipelineNode sourceNodeWithTestField() {
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
                          "format": "JSON",
                          "sampleData": [
                            "{\\\"test1\\\":\\\"1\\\"}"
                          ]
                        }
                        """));
    }

    private PipelineNode sourceNodeWithTemplateField() {
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
                          "format": "JSON",
                          "sampleData": [
                            "{\\\"test\\\":\\\"23333\\\"}"
                          ]
                        }
                        """));
    }

    private PipelineNode sourceNodeWithNestedField() {
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
                          "format": "JSON",
                          "sampleData": [
                            "{\\\"test1\\\":{\\\"test2\\\":\\\"value\\\"}}"
                          ]
                        }
                        """));
    }

    private PipelineNode sourceNodeWithObjectField() {
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
                          "format": "JSON",
                          "sampleData": [
                            "{\\\"test\\\":{\\\"name\\\":\\\"alice\\\",\\\"age\\\":18,\\\"city\\\":\\\"shanghai\\\"}}"
                          ]
                        }
                        """));
    }

    private PipelineNode sourceNodeWithDeserializePayload() {
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
                          "format": "JSON",
                          "sampleData": [
                            "{\\\"payload\\\":\\\"{\\\\\\\"name\\\\\\\":\\\\\\\"alice\\\\\\\",\\\\\\\"age\\\\\\\":18,\\\\\\\"city\\\\\\\":\\\\\\\"shanghai\\\\\\\"}\\\"}"
                          ]
                        }
                        """));
    }

    private PipelineNode sourceNodeWithInvalidDeserializePayload() {
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
                          "format": "JSON",
                          "sampleData": [
                            "{\\\"payload\\\":\\\"{bad-json}\\\"}"
                          ]
                        }
                        """));
    }

    private PipelineNode sourceNodeWithPayload(String payload) {
        ObjectNode config = objectMapper.createObjectNode();
        config.put("bootstrapServers", "127.0.0.1:9092");
        ArrayNode topics = config.putArray("topics");
        topics.add("input-topic");
        config.put("groupId", "group-1");
        config.put("consumeMode", "earliest");
        config.put("authType", "NONE");
        config.put("format", "JSON");
        ArrayNode sampleData = config.putArray("sampleData");
        ObjectNode sampleRecord = objectMapper.createObjectNode();
        sampleRecord.put("payload", payload);
        sampleData.add(sampleRecord.toString());
        return new PipelineNode(
                "source-1",
                "Source",
                PipelineNodeType.SOURCE,
                PipelineOperator.KAFKA_SOURCE,
                config);
    }

    private PipelineNode filterNode() {
        return new PipelineNode(
                "filter-1",
                "Filter",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.FILTER,
                jsonNode("""
                        {
                          "condition": "['status'] == 'active'"
                        }
                        """));
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

    private PipelineNode putNode() {
        return putNodeWithValue("kept");
    }

    private PipelineNode putNodeWithValue(String value) {
        return putNodeWithFieldAndValue("branch", value);
    }

    private PipelineNode putNodeWithFieldAndValue(String field, String value) {
        return new PipelineNode(
                "put-1",
                "Put",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.PUT,
                jsonNode("""
                        {
                          "field": "%s",
                          "value": "%s"
                        }
                        """.formatted(field, value)));
    }

    private PipelineNode pruneNodeWithField(String field) {
        return new PipelineNode(
                "prune-1",
                "Prune",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.PRUNE,
                jsonNode("""
                        {
                          "fields": ["%s"]
                        }
                        """.formatted(field)));
    }

    private PipelineNode renameNodeWithMapping(String sourceField, String targetField) {
        return new PipelineNode(
                "rename-1",
                "Rename",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.RENAME,
                jsonNode("""
                        {
                          "mapping": {
                            "%s": "%s"
                          }
                        }
                        """.formatted(sourceField, targetField)));
    }

    private PipelineNode deserializeNode(
            String field,
            String targetField,
            String format,
            List<String> fieldNames,
            String delimiter) {
        return new PipelineNode(
                "deserialize-1",
                "Deserialize",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.DESERIALIZE,
                jsonNode("""
                        {
                          "field": "%s",
                          "targetField": "%s",
                          "format": "%s",
                          "fieldNames": %s,
                          "delimiter": %s
                        }
                        """.formatted(field, targetField, format, stringArrayJson(fieldNames), jsonStringOrNull(delimiter))));
    }

    private PipelineNode deserializeNode(String field, String targetField, String format, List<String> fieldNames) {
        return deserializeNode(field, targetField, format, fieldNames, null);
    }

    private PipelineNode serializeNode(List<String> sourceFields, String targetField, String format, String delimiter) {
        return new PipelineNode(
                "serialize-1",
                "Serialize",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.SERIALIZE,
                jsonNode("""
                        {
                          "sourceFields": %s,
                          "targetField": "%s",
                          "format": "%s",
                          "delimiter": %s
                        }
                        """.formatted(stringArrayJson(sourceFields), targetField, format, jsonStringOrNull(delimiter))));
    }

    private PipelineNode serializeNode(List<String> sourceFields, String targetField, String format) {
        return serializeNode(sourceFields, targetField, format, null);
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private String stringArrayJson(List<String> values) {
        try {
            return objectMapper.writeValueAsString(values);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private String jsonStringOrNull(String value) {
        if (value == null) {
            return "null";
        }
        try {
            return objectMapper.writeValueAsString(value);
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
