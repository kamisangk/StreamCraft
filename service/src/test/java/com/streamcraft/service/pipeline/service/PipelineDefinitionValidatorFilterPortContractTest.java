package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorFilterPortContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void runValidationAcceptsFilterTruePortToDefaultInputPort() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithFilterSourcePort("true")));
    }

    @Test
    void runValidationAcceptsFilterFalsePortToDefaultInputPort() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithFilterSourcePort("false")));
    }

    @Test
    void saveValidationRejectsLegacyFilterOutputPortToDefaultInputPort() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> validator.validateForSave(saveDefinitionWithFilterSourcePort("output-0")));

        assertTrue(exception.getMessage().contains("FILTER"));
        assertTrue(exception.getMessage().contains("true"));
        assertTrue(exception.getMessage().contains("false"));
    }

    @Test
    void saveValidationAcceptsFilterTruePortToDefaultInputPort() {
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithFilterSourcePort("true")));
    }

    @Test
    void saveValidationAcceptsFilterFalsePortToDefaultInputPort() {
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithFilterSourcePort("false")));
    }

    @Test
    void runValidationRejectsNonFilterTrueSourcePort() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForRun(runtimeDefinitionWithSourceNodePort("source-1", "true", "input-0")));

        assertTrue(exception.getMessage().contains("KAFKA_SOURCE"));
        assertTrue(exception.getMessage().contains("output-0"));
    }

    @Test
    void runValidationRejectsNonFilterFalseSourcePort() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForRun(runtimeDefinitionWithSourceNodePort("source-1", "false", "input-0")));

        assertTrue(exception.getMessage().contains("KAFKA_SOURCE"));
        assertTrue(exception.getMessage().contains("output-0"));
    }

    @Test
    void runValidationRejectsNonDefaultTargetPort() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForRun(runtimeDefinitionWithSourceNodePort("filter-1", "true", "input-1")));

        assertTrue(exception.getMessage().contains("unsupported target port"));
        assertTrue(exception.getMessage().contains("input-0"));
    }

    @Test
    void saveValidationRejectsNonDefaultTargetPort() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithTargetPort("input-1")));

        assertTrue(exception.getMessage().contains("unsupported target port"));
        assertTrue(exception.getMessage().contains("input-0"));
    }

    @Test
    void saveValidationRejectsMissingSourcePort() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionMissingEdgeField("sourcePortId")));

        assertTrue(exception.getMessage().contains("sourcePortId"));
    }

    @Test
    void saveValidationRejectsMissingTargetPort() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionMissingEdgeField("targetPortId")));

        assertTrue(exception.getMessage().contains("targetPortId"));
    }

    private String runtimeDefinitionWithFilterSourcePort(String sourcePortId) {
        return """
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
                        "condition": "value > 0"
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
                    {"id": "edge-2", "sourceNodeId": "filter-1", "sourcePortId": "%s", "targetNodeId": "sink-1", "targetPortId": "input-0"}
                  ]
                }
                """.formatted(sourcePortId);
    }

    private String saveDefinitionWithFilterSourcePort(String sourcePortId) {
        return """
                {
                  "nodes": [
                    {"id": "source-1", "type": "SOURCE", "operator": "KAFKA_SOURCE"},
                    {"id": "filter-1", "type": "TRANSFORM", "operator": "FILTER"},
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {"id": "edge-1", "sourceNodeId": "source-1", "sourcePortId": "output-0", "targetNodeId": "filter-1", "targetPortId": "input-0"},
                    {"id": "edge-2", "sourceNodeId": "filter-1", "sourcePortId": "%s", "targetNodeId": "sink-1", "targetPortId": "input-0"}
                  ]
                }
                """.formatted(sourcePortId);
    }

    private String runtimeDefinitionWithSourceNodePort(String sourceNodeId, String sourcePortId, String targetPortId) {
        return """
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
                        "condition": "value > 0"
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
                    {"id": "edge-2", "sourceNodeId": "%s", "sourcePortId": "%s", "targetNodeId": "sink-1", "targetPortId": "%s"}
                  ]
                }
                """.formatted(sourceNodeId, sourcePortId, targetPortId);
    }

    private String saveDefinitionWithTargetPort(String targetPortId) {
        return """
                {
                  "nodes": [
                    {"id": "source-1", "type": "SOURCE", "operator": "KAFKA_SOURCE"},
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {"id": "edge-1", "sourceNodeId": "source-1", "sourcePortId": "output-0", "targetNodeId": "sink-1", "targetPortId": "%s"}
                  ]
                }
                """.formatted(targetPortId);
    }

    private String saveDefinitionMissingEdgeField(String missingField) {
        String sourcePort = "sourcePortId".equals(missingField) ? "" : ", \"sourcePortId\": \"output-0\"";
        String targetPort = "targetPortId".equals(missingField) ? "" : ", \"targetPortId\": \"input-0\"";
        return """
                {
                  "nodes": [
                    {"id": "source-1", "type": "SOURCE", "operator": "KAFKA_SOURCE"},
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {"id": "edge-1", "sourceNodeId": "source-1"%s, "targetNodeId": "sink-1"%s}
                  ]
                }
                """.formatted(sourcePort, targetPort);
    }
}
