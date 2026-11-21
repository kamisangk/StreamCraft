package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class MysqlSourceValidationTest {
    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void acceptsMysqlRunAndUsesSampleDataForPreview() {
        assertDoesNotThrow(() -> validator.validateForRun(definition("jdbc:mysql://localhost:3306/shop")));
        assertDoesNotThrow(() -> validator.validateForPreview(definition("unconfigured")));
        var error = assertThrows(IllegalArgumentException.class,
                () -> validator.validateForRun(definition("jdbc:h2:mem:test")));
        assertTrue(error.getMessage().contains("url"));
    }

    private static String definition(String url) {
        return """
                {"nodes":[
                  {"id":"in","type":"SOURCE","operator":"MYSQL_SOURCE","config":{
                    "url":"%s","tablePath":"shop.orders","sampleData":["{}"]}},
                  {"id":"out","type":"SINK","operator":"KAFKA_SINK","config":{
                    "bootstrapServers":"localhost:9092","topic":"out","format":"JSON","authType":"NONE",
                    "deliveryGuarantee":"AT_LEAST_ONCE"}}
                ],"edges":[{"id":"edge","sourceNodeId":"in","sourcePortId":"records",
                    "targetNodeId":"out","targetPortId":"records"}]}
                """.formatted(url);
    }
}
