package com.streamcraft.shared.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineNodeConfigValidationSupportTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void rejectsDeserializeCsvWithoutFieldNames() throws Exception {
        JsonNode config = objectMapper.readTree("""
                {
                  "field": "payload",
                  "targetField": "parsed",
                  "format": "CSV"
                }
                """);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> PipelineNodeConfigValidationSupport.validateTransformConfig(
                        "DESERIALIZE", config, issue -> new IllegalArgumentException(issue.defaultMessage())));

        assertTrue(exception.getMessage().contains("fieldNames"));
    }

    @Test
    void acceptsAggregateConfigThroughSharedValidation() throws Exception {
        JsonNode config = objectMapper.readTree("""
                {
                  "windowType": "COUNT",
                  "countWindowSize": 10,
                  "aggregations": [
                    {"function": "COUNT", "outputField": "record_count"}
                  ]
                }
                """);

        assertDoesNotThrow(() -> PipelineNodeConfigValidationSupport.validateTransformConfig(
                "AGGREGATE", config, issue -> new IllegalArgumentException(issue.defaultMessage())));
    }
}
