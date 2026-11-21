package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class MysqlSinkValidationTest {
    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void validatesMysqlSinkOnRunAndAllowsPreviewWithoutConnectionDetails() {
        String valid = definition("""
                {"url":"jdbc:mysql://localhost:3306/shop","database":"shop","table":"orders",
                 "fields":["id","name"],"primaryKeys":["id"],"enableUpsert":true}
                """);
        assertDoesNotThrow(() -> validator.validateForSave(valid));
        assertDoesNotThrow(() -> validator.validateForRun(valid));
        assertDoesNotThrow(() -> validator.validateForPreview(definition("{}")));
        var error = assertThrows(IllegalArgumentException.class, () -> validator.validateForRun(definition("""
                {"url":"jdbc:mysql://localhost:3306/shop","fields":["id"]}
                """)));
        assertTrue(error.getMessage().contains("table"));
    }

    private static String definition(String sinkConfig) {
        return """
                {"nodes":[
                  {"id":"in","type":"SOURCE","operator":"MYSQL_SOURCE","config":{
                    "url":"jdbc:mysql://localhost:3306/shop","tablePath":"shop.input","sampleData":["{}"]}},
                  {"id":"out","type":"SINK","operator":"MYSQL_SINK","config":%s}
                ],"edges":[{"id":"edge","sourceNodeId":"in","sourcePortId":"records",
                    "targetNodeId":"out","targetPortId":"records"}]}
                """.formatted(sinkConfig);
    }
}
