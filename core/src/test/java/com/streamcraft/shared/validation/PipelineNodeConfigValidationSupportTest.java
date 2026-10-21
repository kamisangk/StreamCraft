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
    void acceptsKafkaTransactionTimeoutWhenOmittedOrNull() throws Exception {
        var config = objectMapper.readTree("""
                {"bootstrapServers":"broker:9092","topic":"out","deliveryGuarantee":"AT_LEAST_ONCE",
                 "format":"JSON","authType":"NONE"}
                """);
        assertDoesNotThrow(() -> PipelineNodeConfigValidationSupport.validateSinkConfig("KAFKA_SINK", config,
                issue -> new IllegalArgumentException(issue.defaultMessage())));
        ((com.fasterxml.jackson.databind.node.ObjectNode) config).putNull("transactionTimeoutMs");
        assertDoesNotThrow(() -> PipelineNodeConfigValidationSupport.validateSinkConfig("KAFKA_SINK", config,
                issue -> new IllegalArgumentException(issue.defaultMessage())));
    }

    @Test
    void rejectsInvalidKafkaTransactionTimeoutBeforeSubmission() throws Exception {
        JsonNode config = objectMapper.readTree("""
                {"bootstrapServers":"broker:9092","topic":"out","deliveryGuarantee":"EXACTLY_ONCE",
                 "format":"JSON","authType":"NONE","transactionTimeoutMs":0}
                """);
        var exception = assertThrows(IllegalArgumentException.class,
                () -> PipelineNodeConfigValidationSupport.validateSinkConfig("KAFKA_SINK", config,
                        issue -> new IllegalArgumentException(issue.defaultMessage())));
        assertTrue(exception.getMessage().contains("transactionTimeoutMs"));
    }

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

    @Test
    void rejectsKafkaSourceWithoutTopics() throws Exception {
        JsonNode config = objectMapper.readTree("""
                {
                  "bootstrapServers": "127.0.0.1:9092",
                  "topics": [],
                  "groupId": "group-a",
                  "consumeMode": "earliest",
                  "authType": "NONE",
                  "format": "JSON"
                }
                """);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> PipelineNodeConfigValidationSupport.validateSourceConfig(
                        "KAFKA_SOURCE", config, issue -> new IllegalArgumentException(issue.defaultMessage())));

        assertTrue(exception.getMessage().contains("topic"));
    }

    @Test
    void previewRejectsTextKafkaSinkWithoutMessageField() throws Exception {
        JsonNode config = objectMapper.readTree("""
                {
                  "format": "TEXT"
                }
                """);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> PipelineNodeConfigValidationSupport.validatePreviewSink(
                        "KAFKA_SINK",
                        config,
                        issue -> new IllegalArgumentException(issue.defaultMessage())));

        assertTrue(exception.getMessage().contains("messageField"));
    }
}
