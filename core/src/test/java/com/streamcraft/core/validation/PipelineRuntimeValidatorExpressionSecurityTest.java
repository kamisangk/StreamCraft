package com.streamcraft.core.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import java.util.List;
import org.junit.jupiter.api.Test;

class PipelineRuntimeValidatorExpressionSecurityTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PipelineRuntimeValidator validator = new PipelineRuntimeValidator();

    @Test
    void rejectsDangerousFilterExpressions() {
        PipelineDefinition definition = pipelineWithFilter("T(java.lang.Runtime).getRuntime().exec('calc') == null");

        assertThrows(IllegalArgumentException.class, () -> validator.validate(definition));
    }

    @Test
    void rejectsDangerousEvalExpressions() {
        PipelineDefinition definition = pipelineWithEval("result", "T(java.lang.System).getenv('PATH')");

        assertThrows(IllegalArgumentException.class, () -> validator.validate(definition));
    }

    @Test
    void acceptsSafeFilterAndEvalExpressions() {
        PipelineDefinition definition = new PipelineDefinition(
                "pipeline-1",
                List.of(
                        sourceNode("earliest"),
                        filterNode("age > 18 && status == 'active'"),
                        evalNode("score", "price * quantity"),
                        sinkNode()),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "filter-1", "input-0"),
                        new PipelineEdge("edge-2", "filter-1", "true", "eval-1", "input-0"),
                        new PipelineEdge("edge-3", "eval-1", "output-0", "sink-1", "input-0")));

        assertDoesNotThrow(() -> validator.validate(definition));
    }

    @Test
    void rejectsUnsupportedConsumeMode() {
        PipelineDefinition definition = new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNode("from-nowhere"), sinkNode()),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> validator.validate(definition));

        assertTrue(exception.getMessage().contains("consumeMode"));
    }

    private PipelineDefinition pipelineWithFilter(String condition) {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNode("earliest"), filterNode(condition), sinkNode()),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "filter-1", "input-0"),
                        new PipelineEdge("edge-2", "filter-1", "true", "sink-1", "input-0")));
    }

    private PipelineDefinition pipelineWithEval(String targetField, String expression) {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNode("earliest"), evalNode(targetField, expression), sinkNode()),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "eval-1", "input-0"),
                        new PipelineEdge("edge-2", "eval-1", "output-0", "sink-1", "input-0")));
    }

    private PipelineNode sourceNode(String consumeMode) {
        return new PipelineNode(
                "source-1",
                "Source",
                PipelineNodeType.SOURCE,
                PipelineOperator.KAFKA_SOURCE,
                objectNode("""
                        {
                          "bootstrapServers": "127.0.0.1:9092",
                          "topics": ["input-topic"],
                          "groupId": "group-1",
                          "consumeMode": "%s",
                          "authType": "NONE",
                          "format": "JSON"
                        }
                        """.formatted(consumeMode.replace("\\", "\\\\").replace("\"", "\\\""))));
    }

    private PipelineNode filterNode(String condition) {
        return new PipelineNode(
                "filter-1",
                "Filter",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.FILTER,
                objectNode("""
                        {
                          "condition": "%s"
                        }
                        """.formatted(condition.replace("\\", "\\\\").replace("\"", "\\\""))));
    }

    private PipelineNode evalNode(String targetField, String expression) {
        return new PipelineNode(
                "eval-1",
                "Eval",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.EVAL,
                objectNode("""
                        {
                          "targetField": "%s",
                          "expression": "%s"
                        }
                        """.formatted(
                                targetField.replace("\\", "\\\\").replace("\"", "\\\""),
                                expression.replace("\\", "\\\\").replace("\"", "\\\""))));
    }

    private PipelineNode sinkNode() {
        return new PipelineNode(
                "sink-1",
                "Sink",
                PipelineNodeType.SINK,
                PipelineOperator.KAFKA_SINK,
                objectNode("""
                        {
                          "bootstrapServers": "127.0.0.1:9092",
                          "topic": "output-topic",
                          "deliveryGuarantee": "AT_LEAST_ONCE",
                          "authType": "NONE",
                          "format": "JSON"
                        }
                        """));
    }

    private com.fasterxml.jackson.databind.JsonNode objectNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
