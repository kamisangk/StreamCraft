package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorStreamJoinContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void runAllowsValidStreamJoinWithTwoSources() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithStreamJoin("""
                {
                  "leftKeyField": "orderId",
                  "rightKeyField": "orderId",
                  "targetField": "detail",
                  "joinType": "LEFT",
                  "missingStrategy": "KEEP_ORIGINAL",
                  "overwriteTargetField": false,
                  "timeMode": "PROCESSING_TIME",
                  "timeUnit": "SECONDS",
                  "windowBefore": 60,
                  "windowAfter": 60,
                  "watermarkDelay": 30
                }
                """)));
    }

    @Test
    void saveRejectsStreamJoinWithoutBothInputPorts() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithStreamJoin("""
                        {
                          "leftKeyField": "orderId",
                          "rightKeyField": "orderId",
                          "targetField": "detail",
                          "windowBefore": 60,
                          "windowAfter": 60
                        }
                        """, false)));

        assertTrue(exception.getMessage().contains("left"));
        assertTrue(exception.getMessage().contains("right"));
    }

    @Test
    void runRejectsStreamJoinWithoutRightKeyField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForRun(runtimeDefinitionWithStreamJoin("""
                        {
                          "leftKeyField": "orderId",
                          "targetField": "detail",
                          "windowBefore": 60,
                          "windowAfter": 60
                        }
                        """)));

        assertTrue(exception.getMessage().contains("rightKeyField"));
    }

    @Test
    void runRejectsStreamJoinWithInvalidNumericWindowField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForRun(runtimeDefinitionWithStreamJoin("""
                        {
                          "leftKeyField": "orderId",
                          "rightKeyField": "orderId",
                          "targetField": "detail",
                          "windowBefore": "bad",
                          "windowAfter": 60
                        }
                        """)));

        assertTrue(exception.getMessage().contains("windowBefore"));
    }

    private String saveDefinitionWithStreamJoin(String config, boolean includeRightPort) {
        String rightEdge = includeRightPort
                ? """
                    ,{"id": "edge-2", "sourceNodeId": "source-2", "sourcePortId": "output-0", "targetNodeId": "stream-join-1", "targetPortId": "right"}
                    """
                : "";
        return """
                {
                  "nodes": [
                    {"id": "source-1", "type": "SOURCE", "operator": "KAFKA_SOURCE"},
                    {"id": "source-2", "type": "SOURCE", "operator": "KAFKA_SOURCE"},
                    {"id": "stream-join-1", "type": "TRANSFORM", "operator": "STREAM_JOIN", "config": %s},
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {"id": "edge-1", "sourceNodeId": "source-1", "sourcePortId": "output-0", "targetNodeId": "stream-join-1", "targetPortId": "left"}%s,
                    {"id": "edge-3", "sourceNodeId": "stream-join-1", "sourcePortId": "output-0", "targetNodeId": "sink-1", "targetPortId": "input-0"}
                  ]
                }
                """.formatted(config, rightEdge);
    }

    private String runtimeDefinitionWithStreamJoin(String config) {
        return """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["orders-left"],
                        "groupId": "group-left",
                        "consumeMode": "earliest",
                        "authType": "NONE",
                        "format": "JSON"
                      }
                    },
                    {
                      "id": "source-2",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["orders-right"],
                        "groupId": "group-right",
                        "consumeMode": "earliest",
                        "authType": "NONE",
                        "format": "JSON"
                      }
                    },
                    {"id": "stream-join-1", "type": "TRANSFORM", "operator": "STREAM_JOIN", "config": %s},
                    {
                      "id": "sink-1",
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
                    {"id": "edge-1", "sourceNodeId": "source-1", "sourcePortId": "output-0", "targetNodeId": "stream-join-1", "targetPortId": "left"},
                    {"id": "edge-2", "sourceNodeId": "source-2", "sourcePortId": "output-0", "targetNodeId": "stream-join-1", "targetPortId": "right"},
                    {"id": "edge-3", "sourceNodeId": "stream-join-1", "sourcePortId": "output-0", "targetNodeId": "sink-1", "targetPortId": "input-0"}
                  ]
                }
                """.formatted(config);
    }
}
