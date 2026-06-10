package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorLookupJoinContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void saveAllowsValidLookupJoinTransformConfig() {
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithLookupJoin("""
                {
                  "sourceField": "countryCode",
                  "targetField": "country",
                  "joinType": "LEFT",
                  "missingStrategy": "KEEP_ORIGINAL",
                  "entries": [
                    {"key": "CN", "fields": {"name": "China", "region": "APAC"}},
                    {"key": "US", "fields": {"name": "United States", "region": "NA"}}
                  ],
                  "overwriteTargetField": false
                }
                """)));
    }

    @Test
    void runRejectsLookupJoinWithoutTargetField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForRun(runtimeDefinitionWithLookupJoin("""
                        {
                          "sourceField": "countryCode",
                          "entries": [
                            {"key": "CN", "fields": {"name": "China"}}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("targetField"));
    }

    @Test
    void saveRejectsLookupJoinWithDuplicateKeys() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithLookupJoin("""
                        {
                          "sourceField": "countryCode",
                          "targetField": "country",
                          "entries": [
                            {"key": "CN", "fields": {"name": "China"}},
                            {"key": "CN", "fields": {"name": "Duplicate"}}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("unique"));
    }

    private String saveDefinitionWithLookupJoin(String config) {
        return """
                {
                  "nodes": [
                    {"id": "source-1", "type": "SOURCE", "operator": "KAFKA_SOURCE"},
                    {"id": "lookup-join-1", "type": "TRANSFORM", "operator": "LOOKUP_JOIN", "config": %s},
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {"id": "edge-1", "sourceNodeId": "source-1", "sourcePortId": "output-0", "targetNodeId": "lookup-join-1", "targetPortId": "input-0"},
                    {"id": "edge-2", "sourceNodeId": "lookup-join-1", "sourcePortId": "output-0", "targetNodeId": "sink-1", "targetPortId": "input-0"}
                  ]
                }
                """.formatted(config);
    }

    private String runtimeDefinitionWithLookupJoin(String config) {
        return """
                {
                  "nodes": [
                    {
                      "id": "source-1",
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
                    {"id": "lookup-join-1", "type": "TRANSFORM", "operator": "LOOKUP_JOIN", "config": %s},
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
                    {"id": "edge-1", "sourceNodeId": "source-1", "sourcePortId": "output-0", "targetNodeId": "lookup-join-1", "targetPortId": "input-0"},
                    {"id": "edge-2", "sourceNodeId": "lookup-join-1", "sourcePortId": "output-0", "targetNodeId": "sink-1", "targetPortId": "input-0"}
                  ]
                }
                """.formatted(config);
    }
}
