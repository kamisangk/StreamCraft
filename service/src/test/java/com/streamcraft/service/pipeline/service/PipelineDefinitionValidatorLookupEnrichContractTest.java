package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorLookupEnrichContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void saveAllowsValidLookupEnrichTransformConfig() {
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithLookupEnrich("""
                {
                  "sourceField": "countryCode",
                  "targetField": "countryScore",
                  "entries": [
                    {"key": "CN", "value": "86", "valueType": "NUMBER"},
                    {"key": "US", "value": "1", "valueType": "NUMBER"}
                  ],
                  "missingStrategy": "DISCARD",
                  "overwriteTargetField": false
                }
                """)));
    }

    @Test
    void saveAllowsLookupEnrichFailMissingStrategyAndJsonValue() {
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithLookupEnrich("""
                {
                  "sourceField": "countryCode",
                  "targetField": "country",
                  "entries": [
                    {"key": "CN", "value": "{\\"name\\":\\"China\\",\\"region\\":\\"APAC\\"}", "valueType": "JSON"}
                  ],
                  "missingStrategy": "FAIL",
                  "overwriteTargetField": false
                }
                """)));
    }

    @Test
    void runRejectsLookupEnrichWithoutTargetField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForRun(runtimeDefinitionWithLookupEnrich("""
                        {
                          "sourceField": "countryCode",
                          "entries": [
                            {"key": "CN", "value": "China"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("targetField"));
    }

    @Test
    void saveRejectsLookupEnrichWithDuplicateKeys() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithLookupEnrich("""
                        {
                          "sourceField": "countryCode",
                          "targetField": "countryName",
                          "entries": [
                            {"key": "CN", "value": "China"},
                            {"key": "CN", "value": "Duplicate"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("unique"));
    }

    private String saveDefinitionWithLookupEnrich(String lookupConfig) {
        return """
                {
                  "nodes": [
                    {"id": "source-1", "type": "SOURCE", "operator": "KAFKA_SOURCE"},
                    {
                      "id": "lookup-enrich-1",
                      "type": "TRANSFORM",
                      "operator": "LOOKUP_ENRICH",
                      "config": %s
                    },
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "lookup-enrich-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "lookup-enrich-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(lookupConfig);
    }

    private String runtimeDefinitionWithLookupEnrich(String lookupConfig) {
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
                      "id": "lookup-enrich-1",
                      "name": "Lookup enrich",
                      "type": "TRANSFORM",
                      "operator": "LOOKUP_ENRICH",
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
                      "targetNodeId": "lookup-enrich-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "lookup-enrich-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(lookupConfig);
    }
}
