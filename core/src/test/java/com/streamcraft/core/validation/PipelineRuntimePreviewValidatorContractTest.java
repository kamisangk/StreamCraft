package com.streamcraft.core.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import com.streamcraft.core.runtime.ExecutionMode;
import java.util.List;
import org.junit.jupiter.api.Test;

class PipelineRuntimePreviewValidatorContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PipelineRuntimeValidator validator = new PipelineRuntimeValidator();

    @Test
    void previewValidationAllowsMockKafkaSourceWithoutRuntimeKafkaFields() {
        assertDoesNotThrow(() -> validator.validate(previewDefinition(), ExecutionMode.PREVIEW));
    }

    @Test
    void previewValidationAllowsKafkaSourceWithoutSampleDataModeFlag() {
        assertDoesNotThrow(() -> validator.validate(previewDefinitionWithoutMockFlag(), ExecutionMode.PREVIEW));
    }

    @Test
    void runValidationStillRequiresRuntimeKafkaFields() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> validator.validate(previewDefinition(), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("bootstrapServers"));
    }

    @Test
    void runValidationRequiresScramMechanismWhenKafkaAuthTypeIsSaslScram() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithScramAuthMissingMechanism(), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("scramMechanism"));
    }

    @Test
    void previewValidationRejectsObjectArraySampleData() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(previewDefinitionWithSamples("""
                        [
                          {"status": "ok"}
                        ]
                        """, "JSON"), ExecutionMode.PREVIEW));

        assertTrue(exception.getMessage().contains("string array"));
    }

    @Test
    void previewValidationRejectsInvalidJsonStringsWhenFormatIsJson() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(previewDefinitionWithSamples("""
                        ["not-json"]
                        """, "JSON"), ExecutionMode.PREVIEW));

        assertTrue(exception.getMessage().contains("JSON object"));
    }

    @Test
    void runValidationRequiresMessageFieldWhenKafkaSinkUsesTextFormat() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTextSink(), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("messageField"));
    }

    @Test
    void runValidationAllowsLookupEnrichWithStaticEntries() {
        assertDoesNotThrow(() -> validator.validate(runDefinitionWithTransform("LOOKUP_ENRICH", """
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
                """), ExecutionMode.RUN));
    }

    @Test
    void runValidationAllowsLookupEnrichFailMissingStrategyAndJsonValue() {
        assertDoesNotThrow(() -> validator.validate(runDefinitionWithTransform("LOOKUP_ENRICH", """
                {
                  "sourceField": "countryCode",
                  "targetField": "country",
                  "entries": [
                    {"key": "CN", "value": "{\\"name\\":\\"China\\",\\"region\\":\\"APAC\\"}", "valueType": "JSON"}
                  ],
                  "missingStrategy": "FAIL",
                  "overwriteTargetField": false
                }
                """), ExecutionMode.RUN));
    }

    @Test
    void runValidationRejectsLookupEnrichWithDuplicateKeys() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTransform("LOOKUP_ENRICH", """
                        {
                          "sourceField": "countryCode",
                          "targetField": "countryName",
                          "entries": [
                            {"key": "CN", "value": "China"},
                            {"key": "CN", "value": "Duplicate China"}
                          ]
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("unique"));
    }

    @Test
    void previewValidationRejectsUnsupportedKafkaSinkFormat() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(previewDefinitionWithSinkConfig("""
                        {
                          "format": "AVRO"
                        }
                        """), ExecutionMode.PREVIEW));

        assertTrue(exception.getMessage().contains("JSON, TEXT"));
    }

    @Test
    void previewValidationRequiresMessageFieldWhenKafkaSinkUsesTextFormat() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(previewDefinitionWithSinkConfig("""
                        {
                          "format": "TEXT"
                        }
                        """), ExecutionMode.PREVIEW));

        assertTrue(exception.getMessage().contains("messageField"));
    }

    @Test
    void runValidationRejectsDeserializeCsvWithoutFieldNames() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithDeserializeConfig("""
                        {
                          "field": "payload",
                          "targetField": "parsed",
                          "format": "CSV"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("fieldNames"));
    }

    @Test
    void runValidationAllowsDeserializeCsvWithCustomDelimiter() {
        assertDoesNotThrow(() -> validator.validate(runDefinitionWithDeserializeConfig("""
                        {
                          "field": "payload",
                          "targetField": "parsed",
                          "format": "CSV",
                          "fieldNames": ["name", "age"],
                          "delimiter": "|"
                        }
                        """), ExecutionMode.RUN));
    }

    @Test
    void runValidationRejectsRemovedTextPassthroughFormat() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithSerializeConfig("""
                        {
                          "sourceFields": ["payload"],
                          "targetField": "output",
                          "format": "TEXT_PASSTHROUGH"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(!exception.getMessage().contains("TEXT_PASSTHROUGH"));
        assertTrue(exception.getMessage().contains("JSON"));
    }

    @Test
    void runValidationAllowsSerializeCsvWithCustomDelimiter() {
        assertDoesNotThrow(() -> validator.validate(runDefinitionWithSerializeConfig("""
                        {
                          "sourceFields": ["payload"],
                          "targetField": "output",
                          "format": "CSV",
                          "delimiter": "|"
                        }
                        """), ExecutionMode.RUN));
    }

    @Test
    void runValidationRejectsGrokWithoutPattern() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTransform("GROK", """
                        {
                          "inputField": "_streamcraft_message",
                          "outputField": "parsed"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("Grok pattern"));
    }

    @Test
    void runValidationRejectsGrokWithoutInputField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTransform("GROK", """
                        {
                          "outputField": "parsed",
                          "pattern": "(?<level>\\\\w+)"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("inputField"));
    }

    @Test
    void runValidationRejectsGrokWithoutOutputField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTransform("GROK", """
                        {
                          "inputField": "_streamcraft_message",
                          "pattern": "(?<level>\\\\w+)"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("outputField"));
    }

    @Test
    void runValidationAllowsGrokWithNamedRegexPattern() {
        assertDoesNotThrow(() -> validator.validate(runDefinitionWithTransform("GROK", """
                        {
                          "inputField": "_streamcraft_message",
                          "outputField": "parsed",
                          "pattern": "(?<level>\\\\w+) (?<message>.*)"
                        }
                        """), ExecutionMode.RUN));
    }

    @Test
    void runValidationAllowsGrokWithStandardPatternLibraryAndNestedFieldPaths() {
        assertDoesNotThrow(() -> validator.validate(runDefinitionWithTransform("GROK", """
                        {
                          "inputField": "_streamcraft_message",
                          "outputField": "parsed.result",
                          "pattern": "%{LOGLEVEL:test.level} %{GREEDYDATA:test.message}"
                        }
                        """), ExecutionMode.RUN));
    }

    @Test
    void runValidationAllowsGrokWithTypeSuffix() {
        assertDoesNotThrow(() -> validator.validate(runDefinitionWithTransform("GROK", """
                        {
                          "inputField": "_streamcraft_message",
                          "outputField": "parsed",
                          "pattern": "%{NUMBER:metrics.age:int}"
                        }
                        """), ExecutionMode.RUN));
    }

    @Test
    void runValidationRejectsCastWithoutInputField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTransform("CAST", """
                        {
                          "outputField": "test.age",
                          "targetType": "INT"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("inputField"));
    }

    @Test
    void runValidationRejectsCastWithoutOutputField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTransform("CAST", """
                        {
                          "inputField": "test.age",
                          "targetType": "INT"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("outputField"));
    }

    @Test
    void runValidationRejectsCastWithUnsupportedTargetType() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTransform("CAST", """
                        {
                          "inputField": "test.age",
                          "outputField": "profile.age",
                          "targetType": "DECIMAL"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("targetType"));
    }

    @Test
    void runValidationAllowsEvalWithNestedTargetField() {
        assertDoesNotThrow(() -> validator.validate(runDefinitionWithTransform("EVAL", """
                        {
                          "targetField": "test.result",
                          "expression": "price * quantity"
                        }
                        """), ExecutionMode.RUN));
    }

    @Test
    void runValidationAllowsEvalWithOutputModeAndErrorStrategy() {
        assertDoesNotThrow(() -> validator.validate(runDefinitionWithTransform("EVAL", """
                        {
                          "targetField": "test.result",
                          "expression": "price * quantity",
                          "outputMode": "WRITE_IF_ABSENT",
                          "errorStrategy": "PUT_NULL"
                        }
                        """), ExecutionMode.RUN));
    }

    @Test
    void runValidationRejectsEvalWithUnsupportedErrorStrategy() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTransform("EVAL", """
                        {
                          "targetField": "test.result",
                          "expression": "price * quantity",
                          "errorStrategy": "IGNORE"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("errorStrategy"));
    }

    @Test
    void runValidationAllowsDeduplicateWithKeyFieldsAndTtl() {
        assertDoesNotThrow(() -> validator.validate(runDefinitionWithTransform("DEDUPLICATE", """
                        {
                          "keyFields": ["order.id", "region"],
                          "timeMode": "PROCESSING_TIME",
                          "ttlSeconds": 3600,
                          "keepStrategy": "FIRST",
                          "duplicateStrategy": "DISCARD"
                        }
                        """), ExecutionMode.RUN));
    }

    @Test
    void runValidationAllowsDeduplicateWithProcessingTimeLastStrategy() {
        assertDoesNotThrow(() -> validator.validate(runDefinitionWithTransform("DEDUPLICATE", """
                        {
                          "keyFields": ["order.id"],
                          "timeMode": "PROCESSING_TIME",
                          "ttlSeconds": 3600,
                          "keepStrategy": "LAST",
                          "duplicateStrategy": "DISCARD"
                        }
                        """), ExecutionMode.RUN));
    }

    @Test
    void runValidationAllowsDeduplicateWithEventTimeLatestStrategy() {
        assertDoesNotThrow(() -> validator.validate(runDefinitionWithTransform("DEDUPLICATE", """
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
                        """), ExecutionMode.RUN));
    }

    @Test
    void runValidationRejectsEventTimeDeduplicateWithoutEventTimeField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTransform("DEDUPLICATE", """
                        {
                          "keyFields": ["order.id"],
                          "timeMode": "EVENT_TIME",
                          "windowSeconds": 300,
                          "watermarkDelaySeconds": 30,
                          "keepStrategy": "EVENT_TIME_LATEST"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("eventTimeField"));
    }

    @Test
    void runValidationRejectsDeduplicateWithoutKeyFields() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTransform("DEDUPLICATE", """
                        {
                          "keyFields": [],
                          "ttlSeconds": 3600,
                          "keepStrategy": "FIRST"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("keyFields"));
    }

    @Test
    void runValidationRejectsDeduplicateWithInvalidTtl() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTransform("DEDUPLICATE", """
                        {
                          "keyFields": ["order.id"],
                          "ttlSeconds": 0,
                          "keepStrategy": "FIRST"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("ttlSeconds"));
    }

    @Test
    void runValidationRejectsDeduplicateWithUnsupportedKeepStrategy() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(runDefinitionWithTransform("DEDUPLICATE", """
                        {
                          "keyFields": ["order.id"],
                          "ttlSeconds": 3600,
                          "keepStrategy": "LATEST"
                        }
                        """), ExecutionMode.RUN));

        assertTrue(exception.getMessage().contains("keepStrategy"));
    }

    private PipelineDefinition previewDefinition() {
        return new PipelineDefinition(
                "preview-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "useMockSource": true,
                                          "format": "JSON",
                                          "sampleData": [
                                            "{\\\"status\\\":\\\"ok\\\"}"
                                          ]
                                        }
                                        """)),
                        new PipelineNode(
                                "sink-1",
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("{}"))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));
    }

    private PipelineDefinition previewDefinitionWithoutMockFlag() {
        return new PipelineDefinition(
                "preview-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "format": "JSON",
                                          "sampleData": [
                                            "{\\\"status\\\":\\\"ok\\\"}"
                                          ]
                                        }
                                        """)),
                        new PipelineNode(
                                "sink-1",
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("{}"))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));
    }

    private PipelineDefinition previewDefinitionWithSamples(String sampleDataJson, String format) {
        return new PipelineDefinition(
                "preview-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "format": "%s",
                                          "sampleData": %s
                                        }
                                        """.formatted(format, sampleDataJson))),
                        new PipelineNode(
                                "sink-1",
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("{}"))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));
    }

    private PipelineDefinition runDefinitionWithTextSink() {
        return new PipelineDefinition(
                "run-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topics": ["orders"],
                                          "groupId": "group-a",
                                          "consumeMode": "earliest",
                                          "authType": "NONE",
                                          "format": "JSON"
                                        }
                                        """)),
                        new PipelineNode(
                                "sink-1",
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topic": "orders-out",
                                          "deliveryGuarantee": "AT_LEAST_ONCE",
                                          "authType": "NONE",
                                          "format": "TEXT"
                                        }
                                        """))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));
    }

    private PipelineDefinition runDefinitionWithScramAuthMissingMechanism() {
        return new PipelineDefinition(
                "run-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topics": ["orders"],
                                          "groupId": "group-a",
                                          "consumeMode": "earliest",
                                          "authType": "SASL_SCRAM",
                                          "username": "alice",
                                          "password": "secret",
                                          "format": "JSON"
                                        }
                                        """)),
                        new PipelineNode(
                                "sink-1",
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topic": "orders-out",
                                          "deliveryGuarantee": "AT_LEAST_ONCE",
                                          "authType": "NONE",
                                          "format": "JSON"
                                        }
                                        """))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));
    }

    private PipelineDefinition previewDefinitionWithSinkConfig(String sinkConfigJson) {
        return new PipelineDefinition(
                "preview-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "format": "TEXT",
                                          "sampleData": ["hello world"]
                                        }
                                        """)),
                        new PipelineNode(
                                "sink-1",
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                        jsonNode(sinkConfigJson))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));
    }

    private PipelineDefinition runDefinitionWithDeserializeConfig(String deserializeConfigJson) {
        return new PipelineDefinition(
                "run-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topics": ["orders"],
                                          "groupId": "group-a",
                                          "consumeMode": "earliest",
                                          "authType": "NONE",
                                          "format": "JSON"
                                        }
                                        """)),
                        new PipelineNode(
                                "deserialize-1",
                                "Deserialize",
                                PipelineNodeType.TRANSFORM,
                                PipelineOperator.DESERIALIZE,
                                jsonNode(deserializeConfigJson)),
                        new PipelineNode(
                                "sink-1",
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topic": "orders-out",
                                          "deliveryGuarantee": "AT_LEAST_ONCE",
                                          "authType": "NONE",
                                          "format": "JSON"
                                        }
                                        """))),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "deserialize-1", "input-0"),
                        new PipelineEdge("edge-2", "deserialize-1", "output-0", "sink-1", "input-0")));
    }

    private PipelineDefinition runDefinitionWithSerializeConfig(String serializeConfigJson) {
        return new PipelineDefinition(
                "run-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topics": ["orders"],
                                          "groupId": "group-a",
                                          "consumeMode": "earliest",
                                          "authType": "NONE",
                                          "format": "JSON"
                                        }
                                        """)),
                        new PipelineNode(
                                "serialize-1",
                                "Serialize",
                                PipelineNodeType.TRANSFORM,
                                PipelineOperator.SERIALIZE,
                                jsonNode(serializeConfigJson)),
                        new PipelineNode(
                                "sink-1",
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topic": "orders-out",
                                          "deliveryGuarantee": "AT_LEAST_ONCE",
                                          "authType": "NONE",
                                          "format": "JSON"
                                        }
                                        """))),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "serialize-1", "input-0"),
                        new PipelineEdge("edge-2", "serialize-1", "output-0", "sink-1", "input-0")));
    }

    private PipelineDefinition runDefinitionWithTransform(String operator, String transformConfigJson) {
        return new PipelineDefinition(
                "run-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topics": ["orders"],
                                          "groupId": "group-a",
                                          "consumeMode": "earliest",
                                          "authType": "NONE",
                                          "format": "JSON"
                                        }
                                        """)),
                        new PipelineNode(
                                "transform-1",
                                operator,
                                PipelineNodeType.TRANSFORM,
                                PipelineOperator.valueOf(operator),
                                jsonNode(transformConfigJson)),
                        new PipelineNode(
                                "sink-1",
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topic": "orders-out",
                                          "deliveryGuarantee": "AT_LEAST_ONCE",
                                          "authType": "NONE",
                                          "format": "JSON"
                                        }
                                        """))),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "transform-1", "input-0"),
                        new PipelineEdge("edge-2", "transform-1", "output-0", "sink-1", "input-0")));
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
