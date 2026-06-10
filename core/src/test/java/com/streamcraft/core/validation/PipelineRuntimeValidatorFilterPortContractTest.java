package com.streamcraft.core.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import java.util.List;
import org.junit.jupiter.api.Test;

class PipelineRuntimeValidatorFilterPortContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PipelineRuntimeValidator validator = new PipelineRuntimeValidator();

    @Test
    void runtimeValidationAcceptsFilterTruePortToDefaultInputPort() {
        assertDoesNotThrow(() -> validator.validate(definitionWithFilterSourcePort("true")));
    }

    @Test
    void runtimeValidationAcceptsFilterFalsePortToDefaultInputPort() {
        assertDoesNotThrow(() -> validator.validate(definitionWithFilterSourcePort("false")));
    }

    @Test
    void runtimeValidationRejectsLegacyFilterOutputPortToDefaultInputPort() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> validator.validate(definitionWithFilterSourcePort("output-0")));

        assertTrue(exception.getMessage().contains("FILTER"));
        assertTrue(exception.getMessage().contains("true"));
        assertTrue(exception.getMessage().contains("false"));
    }

    @Test
    void runtimeValidationRejectsNonFilterTrueSourcePort() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> validator.validate(definitionWithSourceNodePort("source-1", "true", "input-0")));

        assertTrue(exception.getMessage().contains("KAFKA_SOURCE"));
        assertTrue(exception.getMessage().contains("output-0"));
    }

    @Test
    void runtimeValidationRejectsNonFilterFalseSourcePort() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> validator.validate(definitionWithSourceNodePort("source-1", "false", "input-0")));

        assertTrue(exception.getMessage().contains("KAFKA_SOURCE"));
        assertTrue(exception.getMessage().contains("output-0"));
    }

    @Test
    void runtimeValidationRejectsNonDefaultTargetPort() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> validator.validate(definitionWithSourceNodePort("filter-1", "true", "input-1")));

        assertTrue(exception.getMessage().contains("unsupported target port"));
        assertTrue(exception.getMessage().contains("input-0"));
    }

    @Test
    void runtimeValidationRejectsMissingSourcePort() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> validator.validate(definitionWithMissingSourcePort()));

        assertTrue(exception.getMessage().contains("sourcePortId"));
    }

    @Test
    void runtimeValidationRejectsMissingTargetPort() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> validator.validate(definitionWithMissingTargetPort()));

        assertTrue(exception.getMessage().contains("targetPortId"));
    }

    private PipelineDefinition definitionWithFilterSourcePort(String sourcePortId) {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNode(), filterNode(), sinkNode()),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "filter-1", "input-0"),
                        new PipelineEdge("edge-2", "filter-1", sourcePortId, "sink-1", "input-0")));
    }

    private PipelineDefinition definitionWithSourceNodePort(String sourceNodeId, String sourcePortId, String targetPortId) {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNode(), filterNode(), sinkNode()),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "filter-1", "input-0"),
                        new PipelineEdge("edge-2", sourceNodeId, sourcePortId, "sink-1", targetPortId)));
    }

    private PipelineDefinition definitionWithMissingSourcePort() {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNode(), sinkNode()),
                List.of(new PipelineEdge("edge-1", "source-1", null, "sink-1", "input-0")));
    }

    private PipelineDefinition definitionWithMissingTargetPort() {
        return new PipelineDefinition(
                "pipeline-1",
                List.of(sourceNode(), sinkNode()),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", null)));
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
                          "format": "JSON"
                        }
                        """));
    }

    private PipelineNode filterNode() {
        return new PipelineNode(
                "filter-1",
                "Filter",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.FILTER,
                jsonNode("""
                        {
                          "condition": "value > 0"
                        }
                        """));
    }

    private PipelineNode sinkNode() {
        return new PipelineNode(
                "sink-1",
                "Sink",
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
}
