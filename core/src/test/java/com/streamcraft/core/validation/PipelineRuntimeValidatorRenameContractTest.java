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

class PipelineRuntimeValidatorRenameContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PipelineRuntimeValidator validator = new PipelineRuntimeValidator();

    @Test
    void acceptsRenameConfigWithAtLeastOneMapping() {
        assertDoesNotThrow(() -> validator.validate(definitionWithRename("""
                {
                  "mapping": {
                    "source_field": "target_field"
                  }
                }
                """)));
    }

    @Test
    void rejectsRenameConfigWithoutMappings() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithRename("""
                        {
                          "mapping": {}
                        }
                        """)));

        assertTrue(exception.getMessage().contains("mapping"));
    }

    private PipelineDefinition definitionWithRename(String renameConfig) {
        return new PipelineDefinition(
                "pipeline-rename",
                List.of(sourceNode(), renameNode(renameConfig), sinkNode()),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "rename-1", "input-0"),
                        new PipelineEdge("edge-2", "rename-1", "output-0", "sink-1", "input-0")));
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

    private PipelineNode renameNode(String renameConfig) {
        return new PipelineNode(
                "rename-1",
                "Rename",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.RENAME,
                jsonNode(renameConfig));
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
