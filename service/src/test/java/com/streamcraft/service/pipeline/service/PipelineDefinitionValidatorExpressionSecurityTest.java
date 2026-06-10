package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorExpressionSecurityTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void rejectsDangerousFilterExpressionsDuringRunValidation() {
        String definition = """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["input-topic"],
                        "groupId": "group-1",
                        "consumeMode": "earliest",
                        "authType": "NONE",
                        "format": "JSON"
                      }
                    },
                    {
                      "id": "filter-1",
                      "type": "TRANSFORM",
                      "operator": "FILTER",
                      "config": {
                        "condition": "T(java.lang.Runtime).getRuntime().exec('calc') == null"
                      }
                    },
                    {
                      "id": "sink-1",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topic": "output-topic",
                        "deliveryGuarantee": "AT_LEAST_ONCE",
                        "authType": "NONE",
                        "format": "JSON"
                      }
                    }
                  ],
                  "edges": [
                    {"id": "edge-1", "sourceNodeId": "source-1", "sourcePortId": "output-0", "targetNodeId": "filter-1", "targetPortId": "input-0"},
                    {"id": "edge-2", "sourceNodeId": "filter-1", "sourcePortId": "true", "targetNodeId": "sink-1", "targetPortId": "input-0"}
                  ]
                }
                """;

        assertThrows(IllegalArgumentException.class, () -> validator.validateForRun(definition));
    }

    @Test
    void acceptsSafeExpressionsDuringRunValidation() {
        String definition = """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["input-topic"],
                        "groupId": "group-1",
                        "consumeMode": "earliest",
                        "authType": "NONE",
                        "format": "JSON"
                      }
                    },
                    {
                      "id": "filter-1",
                      "type": "TRANSFORM",
                      "operator": "FILTER",
                      "config": {
                        "condition": "age > 18 && status == 'active'"
                      }
                    },
                    {
                      "id": "eval-1",
                      "type": "TRANSFORM",
                      "operator": "EVAL",
                      "config": {
                        "targetField": "score",
                        "expression": "price * quantity"
                      }
                    },
                    {
                      "id": "sink-1",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topic": "output-topic",
                        "deliveryGuarantee": "AT_LEAST_ONCE",
                        "authType": "NONE",
                        "format": "JSON"
                      }
                    }
                  ],
                  "edges": [
                    {"id": "edge-1", "sourceNodeId": "source-1", "sourcePortId": "output-0", "targetNodeId": "filter-1", "targetPortId": "input-0"},
                    {"id": "edge-2", "sourceNodeId": "filter-1", "sourcePortId": "true", "targetNodeId": "eval-1", "targetPortId": "input-0"},
                    {"id": "edge-3", "sourceNodeId": "eval-1", "sourcePortId": "output-0", "targetNodeId": "sink-1", "targetPortId": "input-0"}
                  ]
                }
                """;

        assertDoesNotThrow(() -> validator.validateForRun(definition));
    }

    @Test
    void rejectsUnsupportedConsumeModeDuringRunValidation() {
        String definition = """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["input-topic"],
                        "groupId": "group-1",
                        "consumeMode": "from-nowhere",
                        "authType": "NONE",
                        "format": "JSON"
                      }
                    },
                    {
                      "id": "sink-1",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topic": "output-topic",
                        "deliveryGuarantee": "AT_LEAST_ONCE",
                        "authType": "NONE",
                        "format": "JSON"
                      }
                    }
                  ],
                  "edges": [
                    {"id": "edge-1", "sourceNodeId": "source-1", "sourcePortId": "output-0", "targetNodeId": "sink-1", "targetPortId": "input-0"}
                  ]
                }
                """;

        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> validator.validateForRun(definition));

        assertTrue(exception.getMessage().contains("consumeMode"));
    }
}
