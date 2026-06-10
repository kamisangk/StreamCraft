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
import com.streamcraft.core.runtime.transform.TransformOperatorFactory;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class PipelineRuntimePreviewSinkCollectionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void previewCollectingSinkFactoryCapturesOutputsBySinkNodeId() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        PreviewCollectingSinkFactory previewSinkFactory = new PreviewCollectingSinkFactory();

        PipelineRuntime runtime = new PipelineRuntime(
                env,
                new KafkaSourceFactory(),
                new MockSourceFactory(),
                previewSinkFactory,
                new TransformOperatorFactory(),
                false,
                ExecutionMode.PREVIEW);

        runtime.run(definitionWithTwoPreviewSinks());
        env.execute("preview-sink-collection-test");

        Map<String, PreviewRunResult.PreviewOutput> outputByNodeId = previewSinkFactory.snapshot().outputs().stream()
                .collect(Collectors.toMap(PreviewRunResult.PreviewOutput::nodeId, Function.identity()));

        assertEquals(2, outputByNodeId.size());
        assertEquals(2, outputByNodeId.get("sink-left").records().size());
        assertEquals(2, outputByNodeId.get("sink-right").records().size());
        assertTrue(outputByNodeId.get("sink-left").records().contains("{\"status\":\"active\"}"));
        assertTrue(outputByNodeId.get("sink-left").records().contains("{\"status\":\"inactive\"}"));
        assertTrue(outputByNodeId.get("sink-right").records().contains("{\"status\":\"active\"}"));
        assertTrue(outputByNodeId.get("sink-right").records().contains("{\"status\":\"inactive\"}"));
    }

    @Test
    void previewCollectingSinkFactoryKeepsTextSamplesUnderStreamcraftMessageField() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        PreviewCollectingSinkFactory previewSinkFactory = new PreviewCollectingSinkFactory();

        PipelineRuntime runtime = new PipelineRuntime(
                env,
                new KafkaSourceFactory(),
                new MockSourceFactory(),
                previewSinkFactory,
                new TransformOperatorFactory(),
                false,
                ExecutionMode.PREVIEW);

        runtime.run(definitionWithTextPreviewSink());
        env.execute("preview-text-sink-collection-test");

        Map<String, PreviewRunResult.PreviewOutput> outputByNodeId = previewSinkFactory.snapshot().outputs().stream()
                .collect(Collectors.toMap(PreviewRunResult.PreviewOutput::nodeId, Function.identity()));

        assertEquals("hello world", outputByNodeId.get("sink-text").records().get(0));
    }

    @Test
    void previewCollectingSinkFactoryUsesConfiguredTextMessageFieldPayload() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        PreviewCollectingSinkFactory previewSinkFactory = new PreviewCollectingSinkFactory();

        PipelineRuntime runtime = new PipelineRuntime(
                env,
                new KafkaSourceFactory(),
                new MockSourceFactory(),
                previewSinkFactory,
                new TransformOperatorFactory(),
                false,
                ExecutionMode.PREVIEW);

        runtime.run(definitionWithJsonSourceAndTextSinkField());
        env.execute("preview-text-message-field-payload-test");

        Map<String, PreviewRunResult.PreviewOutput> outputByNodeId = previewSinkFactory.snapshot().outputs().stream()
                .collect(Collectors.toMap(PreviewRunResult.PreviewOutput::nodeId, Function.identity()));

        assertEquals("23aa333", outputByNodeId.get("sink-text-field").records().get(0));
    }

    private PipelineDefinition definitionWithTwoPreviewSinks() {
        return new PipelineDefinition(
                "preview-sink-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "useMockSource": false,
                                          "format": "JSON",
                                          "sampleData": [
                                            "{\\\"status\\\":\\\"active\\\"}",
                                            "{\\\"status\\\":\\\"inactive\\\"}"
                                          ]
                                        }
                                        """)),
                        new PipelineNode(
                                "sink-left",
                                "Left Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("{}")),
                        new PipelineNode(
                                "sink-right",
                                "Right Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("{}"))),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "sink-left", "input-0"),
                        new PipelineEdge("edge-2", "source-1", "output-0", "sink-right", "input-0")));
    }

    private PipelineDefinition definitionWithTextPreviewSink() {
        return new PipelineDefinition(
                "preview-text-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "format": "TEXT",
                                          "sampleData": [
                                            "hello world"
                                          ]
                                        }
                                        """)),
                        new PipelineNode(
                                "sink-text",
                                "Text Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("""
                                        {
                                          "format": "TEXT",
                                          "messageField": "_streamcraft_message"
                                        }
                                        """))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-text", "input-0")));
    }

    private PipelineDefinition definitionWithJsonSourceAndTextSinkField() {
        return new PipelineDefinition(
                "preview-json-to-text-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "format": "JSON",
                                          "sampleData": [
                                            "{\\\"test\\\":\\\"23aa333\\\"}"
                                          ]
                                        }
                                        """)),
                        new PipelineNode(
                                "sink-text-field",
                                "Text Sink Field",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("""
                                        {
                                          "format": "TEXT",
                                          "messageField": "test"
                                        }
                                        """))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-text-field", "input-0")));
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
