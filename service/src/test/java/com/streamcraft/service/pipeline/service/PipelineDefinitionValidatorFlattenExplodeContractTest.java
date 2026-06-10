package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorFlattenExplodeContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void saveAllowsValidFlattenTransformConfig() {
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithTransform("FLATTEN", """
                {
                  "sourceField": "customer",
                  "targetPrefix": "customer",
                  "delimiter": "_",
                  "removeSourceField": true
                }
                """)));
    }

    @Test
    void saveRejectsFlattenWithoutSourceField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithTransform("FLATTEN", """
                        {
                          "targetPrefix": "customer",
                          "delimiter": "_"
                        }
                        """)));

        assertTrue(exception.getMessage().contains("sourceField"));
    }

    @Test
    void saveRejectsFlattenWithBlankDelimiter() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithTransform("FLATTEN", """
                        {
                          "sourceField": "customer",
                          "delimiter": " "
                        }
                        """)));

        assertTrue(exception.getMessage().contains("delimiter"));
    }

    @Test
    void runAllowsValidExplodeTransformConfig() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithTransform("EXPLODE", """
                {
                  "sourceField": "items",
                  "targetField": "item",
                  "keepEmpty": false
                }
                """)));
    }

    @Test
    void previewRejectsExplodeWithoutTargetField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForPreview(previewDefinitionWithTransform("EXPLODE", """
                        {
                          "sourceField": "items"
                        }
                        """)));

        assertTrue(exception.getMessage().contains("targetField"));
    }

    @Test
    void saveRejectsExplodeWithoutSourceField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithTransform("EXPLODE", """
                        {
                          "targetField": "item"
                        }
                        """)));

        assertTrue(exception.getMessage().contains("sourceField"));
    }

    private String saveDefinitionWithTransform(String operator, String config) {
        return """
                {
                  "nodes": [
                    {"id": "source-1", "type": "SOURCE", "operator": "KAFKA_SOURCE"},
                    {
                      "id": "transform-1",
                      "type": "TRANSFORM",
                      "operator": "%s",
                      "config": %s
                    },
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "transform-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "transform-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(operator, config);
    }

    private String runtimeDefinitionWithTransform(String operator, String config) {
        return """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["orders"],
                        "groupId": "group-a",
                        "consumeMode": "earliest",
                        "authType": "NONE",
                        "format": "JSON"
                      }
                    },
                    {
                      "id": "transform-1",
                      "name": "%s",
                      "type": "TRANSFORM",
                      "operator": "%s",
                      "config": %s
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topic": "orders-out",
                        "deliveryGuarantee": "AT_LEAST_ONCE",
                        "authType": "NONE",
                        "format": "JSON"
                      }
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "transform-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "transform-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(operator, operator, config);
    }

    private String previewDefinitionWithTransform(String operator, String config) {
        return """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "format": "JSON",
                        "sampleData": ["{\\\"items\\\":[1,2]}"]
                      }
                    },
                    {
                      "id": "transform-1",
                      "name": "%s",
                      "type": "TRANSFORM",
                      "operator": "%s",
                      "config": %s
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {}
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "transform-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "transform-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(operator, operator, config);
    }
}
