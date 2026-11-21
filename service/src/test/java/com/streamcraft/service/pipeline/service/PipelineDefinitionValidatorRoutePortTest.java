package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorRoutePortTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void rejectsUnknownPortOnSaveRunAndPreview() {
        String definition = definition("typo", true);

        assertPortError(assertThrows(IllegalArgumentException.class, () -> validator.validateForSave(definition)));
        assertPortError(assertThrows(IllegalArgumentException.class, () -> validator.validateForRun(definition)));
        assertPortError(assertThrows(IllegalArgumentException.class, () -> validator.validateForPreview(definition)));
    }

    @Test
    void checksConfiguredAndFallbackPorts() {
        assertDoesNotThrow(() -> validator.validateForSave(definition("output-0", false)));
        assertDoesNotThrow(() -> validator.validateForRun(definition("fallback", true)));
        assertThrows(IllegalArgumentException.class, () -> validator.validateForPreview(definition("fallback", false)));
    }

    private static void assertPortError(IllegalArgumentException error) {
        assertTrue(error.getMessage().contains("route-edge uses unsupported source port typo for source operator ROUTE"));
        assertTrue(error.getMessage().contains("output-0"));
        assertTrue(error.getMessage().contains("fallback"));
    }

    private static String definition(String port, boolean includeUnmatched) {
        return """
                {
                  "pipelineId": "pipeline-1",
                  "nodes": [
                    {"id":"source", "type":"SOURCE", "operator":"KAFKA_SOURCE", "config":{
                      "bootstrapServers":"127.0.0.1:9092", "topics":["input"], "groupId":"group",
                      "consumeMode":"earliest", "authType":"NONE", "format":"JSON", "sampleData":["{}"]}},
                    {"id":"route", "type":"TRANSFORM", "operator":"ROUTE", "config":{
                      "routes":[{"portId":"output-0", "condition":"value > 0"}],
                      "includeUnmatched":%s, "unmatchedPort":"fallback"}},
                    {"id":"sink", "type":"SINK", "operator":"KAFKA_SINK", "config":{
                      "bootstrapServers":"127.0.0.1:9092", "topic":"output", "authType":"NONE",
                      "deliveryGuarantee":"AT_LEAST_ONCE", "format":"JSON"}}
                  ],
                  "edges": [
                    {"id":"input-edge", "sourceNodeId":"source", "sourcePortId":"records",
                     "targetNodeId":"route", "targetPortId":"records"},
                    {"id":"route-edge", "sourceNodeId":"route", "sourcePortId":"%s",
                     "targetNodeId":"sink", "targetPortId":"records"}
                  ]
                }
                """.formatted(includeUnmatched, port);
    }
}
