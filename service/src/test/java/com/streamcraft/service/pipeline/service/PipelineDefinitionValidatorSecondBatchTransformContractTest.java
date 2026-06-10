package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorSecondBatchTransformContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void saveAllowsTimeDeriveMaskHashCaseWhenAndRouteConfigs() {
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithTransform("TIME_DERIVE", """
                {
                  "sourceField": "eventTime",
                  "sourceFormat": "PATTERN",
                  "sourcePattern": "yyyy-MM-dd HH:mm:ss",
                  "sourceTimeZone": "Asia/Shanghai",
                  "outputTimeZone": "UTC",
                  "parseErrorStrategy": "KEEP_ORIGINAL",
                  "derivations": [{"outputField": "dt", "type": "DATE"}]
                }
                """)));
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithTransform("MASK_HASH", """
                {
                  "rules": [{"sourceField": "phone", "targetField": "phoneMasked", "action": "MASK"}]
                }
                """)));
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithTransform("CASE_WHEN", """
                {
                  "targetField": "bucket",
                  "cases": [{"condition": "amount >= 100", "value": "large"}],
                  "defaultValue": "other"
                }
                """)));
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithTransform("CASE_WHEN", """
                {
                  "targetField": "bucket",
                  "cases": [{"condition": "amount >= 100", "value": "large"}],
                  "defaultMode": "NONE",
                  "defaultValue": "",
                  "defaultExpression": ""
                }
                """)));
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithTransform("CASE_WHEN", """
                {
                  "targetField": "bucket",
                  "cases": [{"condition": "amount >= 100", "value": "large"}],
                  "defaultMode": "EXPRESSION",
                  "defaultExpression": "amount >= 50 ? 'medium' : 'small'"
                }
                """)));
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithTransform("ROUTE", """
                {
                  "routes": [{"portId": "large", "condition": "amount >= 100"}],
                  "includeUnmatched": true,
                  "unmatchedPort": "unmatched"
                }
                """, "large")));
    }

    @Test
    void saveRejectsInvalidSecondBatchConfigs() {
        assertMessageContains("TIME_DERIVE", "{\"derivations\":[]}", "sourceField");
        assertMessageContains("MASK_HASH", "{\"rules\":[{\"targetField\":\"x\"}]}", "sourceField");
        assertMessageContains("MASK_HASH", "{\"rules\":[{\"sourceField\":\"phone\"}]}", "targetField");
        assertMessageContains("CASE_WHEN", "{\"cases\":[{\"condition\":\"true\",\"value\":\"x\"}]}", "targetField");
        assertMessageContains("CASE_WHEN", """
                {
                  "targetField": "bucket",
                  "cases": [{"condition": "true", "value": "x"}],
                  "defaultMode": "VALUE"
                }
                """, "value");
        assertMessageContains("CASE_WHEN", """
                {
                  "targetField": "bucket",
                  "cases": [{"condition": "true", "value": "x"}],
                  "defaultMode": "BAD"
                }
                """, "defaultMode");
        assertMessageContains("ROUTE", "{\"routes\":[{\"portId\":\"bad port\",\"condition\":\"true\"}]}", "portId");
    }

    private void assertMessageContains(String operator, String config, String expected) {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithTransform(operator, config)));
        assertTrue(exception.getMessage().contains(expected));
    }

    private String saveDefinitionWithTransform(String operator, String config) {
        return saveDefinitionWithTransform(operator, config, "output-0");
    }

    private String saveDefinitionWithTransform(String operator, String config, String sourcePortId) {
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
                    {"id": "edge-1", "sourceNodeId": "source-1", "sourcePortId": "output-0", "targetNodeId": "transform-1", "targetPortId": "input-0"},
                    {"id": "edge-2", "sourceNodeId": "transform-1", "sourcePortId": "%s", "targetNodeId": "sink-1", "targetPortId": "input-0"}
                  ]
                }
                """.formatted(operator, config, sourcePortId);
    }
}
