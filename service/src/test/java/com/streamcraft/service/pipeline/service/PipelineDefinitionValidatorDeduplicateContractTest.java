package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorDeduplicateContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void saveAllowsValidDeduplicateTransformConfig() {
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithDeduplicate("""
                {
                  "keyFields": ["order.id", "region"],
                  "timeMode": "PROCESSING_TIME",
                  "ttlSeconds": 3600,
                  "keepStrategy": "FIRST",
                  "duplicateStrategy": "DISCARD"
                }
                """)));
    }

    @Test
    void saveAllowsProcessingTimeLastDeduplicateTransformConfig() {
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithDeduplicate("""
                {
                  "keyFields": ["order.id", "region"],
                  "timeMode": "PROCESSING_TIME",
                  "ttlSeconds": 3600,
                  "keepStrategy": "LAST",
                  "duplicateStrategy": "DISCARD"
                }
                """)));
    }

    @Test
    void saveAllowsEventTimeLatestDeduplicateTransformConfig() {
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithDeduplicate("""
                {
                  "keyFields": ["order.id"],
                  "timeMode": "EVENT_TIME",
                  "eventTimeField": "eventTime",
                  "windowSeconds": 300,
                  "watermarkDelaySeconds": 30,
                  "keepStrategy": "EVENT_TIME_LATEST",
                  "lateDataStrategy": "DISCARD",
                  "duplicateStrategy": "DISCARD"
                }
                """)));
    }

    @Test
    void saveRejectsEventTimeDeduplicateWithoutEventTimeField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithDeduplicate("""
                        {
                          "keyFields": ["order.id"],
                          "timeMode": "EVENT_TIME",
                          "windowSeconds": 300,
                          "watermarkDelaySeconds": 30,
                          "keepStrategy": "EVENT_TIME_LATEST"
                        }
                        """)));

        assertTrue(exception.getMessage().contains("eventTimeField"));
    }

    @Test
    void saveRejectsDeduplicateWithoutKeyFields() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithDeduplicate("""
                        {
                          "keyFields": [],
                          "ttlSeconds": 3600,
                          "keepStrategy": "FIRST"
                        }
                        """)));

        assertTrue(exception.getMessage().contains("keyFields"));
    }

    @Test
    void saveRejectsDeduplicateWithInvalidTtl() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithDeduplicate("""
                        {
                          "keyFields": ["order.id"],
                          "ttlSeconds": 0,
                          "keepStrategy": "FIRST"
                        }
                        """)));

        assertTrue(exception.getMessage().contains("ttlSeconds"));
    }

    @Test
    void saveRejectsDeduplicateWithUnsupportedKeepStrategy() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithDeduplicate("""
                        {
                          "keyFields": ["order.id"],
                          "timeMode": "PROCESSING_TIME",
                          "ttlSeconds": 3600,
                          "keepStrategy": "LATEST"
                        }
                        """)));

        assertTrue(exception.getMessage().contains("keepStrategy"));
    }

    private String saveDefinitionWithDeduplicate(String deduplicateConfig) {
        return """
                {
                  "nodes": [
                    {"id": "source-1", "type": "SOURCE", "operator": "KAFKA_SOURCE"},
                    {
                      "id": "deduplicate-1",
                      "type": "TRANSFORM",
                      "operator": "DEDUPLICATE",
                      "config": %s
                    },
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "deduplicate-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "deduplicate-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(deduplicateConfig);
    }
}
