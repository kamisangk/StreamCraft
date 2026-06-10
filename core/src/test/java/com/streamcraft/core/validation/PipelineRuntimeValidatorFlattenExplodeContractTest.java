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

class PipelineRuntimeValidatorFlattenExplodeContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PipelineRuntimeValidator validator = new PipelineRuntimeValidator();

    @Test
    void acceptsFlattenConfigWithOptionalFieldsOmitted() {
        assertDoesNotThrow(() -> validator.validate(definitionWithTransform("""
                {
                  "sourceField": "customer"
                }
                """, PipelineOperator.FLATTEN)));
    }

    @Test
    void rejectsFlattenConfigWithoutSourceField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithTransform("""
                        {
                          "targetPrefix": "customer_flat"
                        }
                        """, PipelineOperator.FLATTEN)));

        assertTrue(exception.getMessage().contains("sourceField"));
    }

    @Test
    void rejectsFlattenConfigWithBlankDelimiter() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithTransform("""
                        {
                          "sourceField": "customer",
                          "delimiter": " "
                        }
                        """, PipelineOperator.FLATTEN)));

        assertTrue(exception.getMessage().contains("delimiter"));
    }

    @Test
    void acceptsExplodeConfigWithRequiredFields() {
        assertDoesNotThrow(() -> validator.validate(definitionWithTransform("""
                {
                  "sourceField": "items",
                  "targetField": "item"
                }
                """, PipelineOperator.EXPLODE)));
    }

    @Test
    void rejectsExplodeConfigWithoutTargetField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithTransform("""
                        {
                          "sourceField": "items"
                        }
                        """, PipelineOperator.EXPLODE)));

        assertTrue(exception.getMessage().contains("targetField"));
    }

    private PipelineDefinition definitionWithTransform(String transformConfig, PipelineOperator operator) {
        return new PipelineDefinition(
                "pipeline-transform",
                List.of(sourceNode(), transformNode(transformConfig, operator), sinkNode()),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "transform-1", "input-0"),
                        new PipelineEdge("edge-2", "transform-1", "output-0", "sink-1", "input-0")));
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

    private PipelineNode transformNode(String transformConfig, PipelineOperator operator) {
        return new PipelineNode(
                "transform-1",
                "Transform",
                PipelineNodeType.TRANSFORM,
                operator,
                jsonNode(transformConfig));
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
