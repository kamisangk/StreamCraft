package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.config.PipelineRuntimeProperties;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorPreviewContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void previewAllowsKafkaSourceWithoutSampleDataModeFlag() {
        assertDoesNotThrow(() -> validator.validateForPreview("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "sampleData": []
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {}
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """));
    }

    @Test
    void previewAllowsSampleDataSourceAndSinkWithoutKafkaFields() {
        assertDoesNotThrow(() -> validator.validateForPreview("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "format": "JSON",
                        "sampleData": [
                          "{\\\"id\\\":1}"
                        ]
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {}
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """));
    }

    @Test
    void previewAllowsKafkaSourceTextSamplesAsStringArray() {
        assertDoesNotThrow(() -> validator.validateForPreview("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "format": "TEXT",
                        "sampleData": ["hello world"]
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {}
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """));
    }

    @Test
    void previewAllowsMultipleKafkaSourcesWhenEachProvidesSampleData() {
        assertDoesNotThrow(() -> validator.validateForPreview("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source A",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "format": "TEXT",
                        "sampleData": ["left"]
                      }
                    },
                    {
                      "id": "source-2",
                      "name": "Kafka Source B",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "format": "TEXT",
                        "sampleData": ["right"]
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {}
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "source-2",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """));
    }

    @Test
    void previewRejectsObjectArraySampleData() {
        assertThatThrownBy(() -> validator.validateForPreview("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "format": "JSON",
                        "sampleData": [
                          {"status": "ok"}
                        ]
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {}
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("string array");
    }

    @Test
    void previewRejectsInvalidJsonSampleStringsWhenFormatIsJson() {
        assertThatThrownBy(() -> validator.validateForPreview("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "format": "JSON",
                        "sampleData": ["not-json"]
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {}
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON object");
    }

    @Test
    void runRequiresMessageFieldWhenKafkaSinkUsesTextFormat() {
        assertThatThrownBy(() -> validator.validateForRun("""
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
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topic": "orders-out",
                        "deliveryGuarantee": "AT_LEAST_ONCE",
                        "authType": "NONE",
                        "format": "TEXT"
                      }
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("messageField");
    }

    @Test
    void previewRejectsUnsupportedKafkaSinkFormat() {
        assertThatThrownBy(() -> validator.validateForPreview("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "format": "TEXT",
                        "sampleData": ["hello world"]
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "format": "AVRO"
                      }
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON, TEXT");
    }

    @Test
    void previewRequiresMessageFieldWhenKafkaSinkUsesTextFormat() {
        assertThatThrownBy(() -> validator.validateForPreview("""
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "format": "TEXT",
                        "sampleData": ["hello world"]
                      }
                    },
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "format": "TEXT"
                      }
                    }
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("messageField");
    }

    @Test
    void runRejectsDeserializeNodeWithoutField() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithDeserializeConfig("""
                {
                  "targetField": "parsed"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field");
    }

    @Test
    void runRejectsDeserializeNodeWithoutTargetField() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithDeserializeConfig("""
                {
                  "field": "payload"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetField");
    }

    @Test
    void runAllowsDeserializeNodeWithNestedTargetField() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithDeserializeConfig("""
                {
                  "field": "payload",
                  "targetField": "test.info",
                  "format": "JSON"
                }
                """)));
    }

    @Test
    void runRejectsDeserializeNodeWithUnsupportedFormat() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithDeserializeConfig("""
                {
                  "field": "payload",
                  "targetField": "parsed",
                  "format": "AVRO"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Pipeline config format must be one of:")
                .hasMessageContaining("JSON")
                .hasMessageContaining("KV")
                .hasMessageContaining("CSV")
                .hasMessageContaining("XML")
                .hasMessageNotContaining("TEXT_PASSTHROUGH");
    }

    @Test
    void runRejectsDeserializeCsvWithoutFieldNames() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithDeserializeConfig("""
                {
                  "field": "payload",
                  "targetField": "parsed",
                  "format": "CSV"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fieldNames");
    }

    @Test
    void runAllowsDeserializeCsvWithFieldNames() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithDeserializeConfig("""
                {
                  "field": "payload",
                  "targetField": "parsed",
                  "format": "CSV",
                  "fieldNames": ["name", "age"]
                }
                """)));
    }

    @Test
    void runAllowsDeserializeCsvWithCustomDelimiter() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithDeserializeConfig("""
                {
                  "field": "payload",
                  "targetField": "parsed",
                  "format": "CSV",
                  "fieldNames": ["name", "age"],
                  "delimiter": "|"
                }
                """)));
    }

    @Test
    void runRejectsSerializeWithoutSourceFields() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithSerializeConfig("""
                {
                  "targetField": "output",
                  "format": "JSON"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sourceFields");
    }

    @Test
    void runRejectsSerializeWithRemovedTextPassthroughFormat() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithSerializeConfig("""
                {
                  "sourceFields": ["payload"],
                  "targetField": "output",
                  "format": "TEXT_PASSTHROUGH"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("TEXT_PASSTHROUGH")
                .hasMessageContaining("JSON");
    }

    @Test
    void runAllowsSerializeCsvWithCustomDelimiter() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithSerializeConfig("""
                {
                  "sourceFields": ["payload"],
                  "targetField": "output",
                  "format": "CSV",
                  "delimiter": "|"
                }
                """)));
    }

    @Test
    void runRejectsGrokWithoutPattern() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("GROK", """
                {
                  "inputField": "_streamcraft_message",
                  "outputField": "parsed"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Grok pattern");
    }

    @Test
    void runRejectsGrokWithoutInputField() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("GROK", """
                {
                  "outputField": "parsed",
                  "pattern": "(?<level>\\\\w+)"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputField");
    }

    @Test
    void runRejectsGrokWithoutOutputField() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("GROK", """
                {
                  "inputField": "_streamcraft_message",
                  "pattern": "(?<level>\\\\w+)"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outputField");
    }

    @Test
    void runAllowsGrokWithNamedRegexPattern() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithTransform("GROK", """
                {
                  "inputField": "_streamcraft_message",
                  "outputField": "parsed",
                  "pattern": "(?<level>\\\\w+) (?<message>.*)"
                }
                """)));
    }

    @Test
    void runAllowsGrokWithStandardPatternLibraryAndNestedFieldPaths() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithTransform("GROK", """
                {
                  "inputField": "_streamcraft_message",
                  "outputField": "parsed.result",
                  "pattern": "%{LOGLEVEL:test.level} %{GREEDYDATA:test.message}"
                }
                """)));
    }

    @Test
    void runAllowsGrokWithTypeSuffix() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithTransform("GROK", """
                {
                  "inputField": "_streamcraft_message",
                  "outputField": "parsed",
                  "pattern": "%{NUMBER:metrics.age:int}"
                }
                """)));
    }

    @Test
    void runRejectsCastWithoutInputField() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("CAST", """
                {
                  "outputField": "test.age",
                  "targetType": "INT"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputField");
    }

    @Test
    void runRejectsCastWithoutOutputField() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("CAST", """
                {
                  "inputField": "test.age",
                  "targetType": "INT"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outputField");
    }

    @Test
    void runRejectsCastWithUnsupportedTargetType() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("CAST", """
                {
                  "inputField": "test.age",
                  "outputField": "profile.age",
                  "targetType": "DECIMAL"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetType");
    }

    @Test
    void runAllowsEvalWithNestedTargetField() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithTransform("EVAL", """
                {
                  "targetField": "test.result",
                  "expression": "price * quantity"
                }
                """)));
    }

    @Test
    void runAllowsEvalWithOutputModeAndErrorStrategy() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithTransform("EVAL", """
                {
                  "targetField": "test.result",
                  "expression": "price * quantity",
                  "outputMode": "WRITE_IF_ABSENT",
                  "errorStrategy": "PUT_NULL"
                }
                """)));
    }

    @Test
    void runRejectsEvalWithUnsupportedErrorStrategy() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("EVAL", """
                {
                  "targetField": "test.result",
                  "expression": "price * quantity",
                  "errorStrategy": "IGNORE"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("errorStrategy");
    }

    @Test
    void runAllowsDeduplicateWithKeyFieldsAndTtl() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithTransform("DEDUPLICATE", """
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
    void runAllowsDeduplicateWithProcessingTimeLastStrategy() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithTransform("DEDUPLICATE", """
                {
                  "keyFields": ["order.id"],
                  "timeMode": "PROCESSING_TIME",
                  "ttlSeconds": 3600,
                  "keepStrategy": "LAST",
                  "duplicateStrategy": "DISCARD"
                }
                """)));
    }

    @Test
    void runAllowsDeduplicateWithEventTimeLatestStrategy() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithTransform("DEDUPLICATE", """
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
    void runRejectsEventTimeDeduplicateWithoutEventTimeField() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("DEDUPLICATE", """
                {
                  "keyFields": ["order.id"],
                  "timeMode": "EVENT_TIME",
                  "windowSeconds": 300,
                  "watermarkDelaySeconds": 30,
                  "keepStrategy": "EVENT_TIME_LATEST"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("eventTimeField");
    }

    @Test
    void runRejectsDeduplicateWithoutKeyFields() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("DEDUPLICATE", """
                {
                  "keyFields": [],
                  "ttlSeconds": 3600,
                  "keepStrategy": "FIRST"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keyFields");
    }

    @Test
    void runRejectsDeduplicateWithInvalidTtl() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("DEDUPLICATE", """
                {
                  "keyFields": ["order.id"],
                  "ttlSeconds": 0,
                  "keepStrategy": "FIRST"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ttlSeconds");
    }

    @Test
    void runRejectsDeduplicateWithUnsupportedKeepStrategy() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("DEDUPLICATE", """
                {
                  "keyFields": ["order.id"],
                  "ttlSeconds": 3600,
                  "keepStrategy": "LATEST"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keepStrategy");
    }

    @Test
    void runAllowsLookupEnrichWithStaticEntries() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithTransform("LOOKUP_ENRICH", """
                {
                  "sourceField": "countryCode",
                  "targetField": "countryName",
                  "entries": [
                    {"key": "CN", "value": "China"},
                    {"key": "US", "value": "United States"}
                  ],
                  "missingStrategy": "KEEP_ORIGINAL",
                  "overwriteTargetField": false
                }
                """)));
    }

    @Test
    void runRejectsLookupEnrichWithDuplicateKeys() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("LOOKUP_ENRICH", """
                {
                  "sourceField": "countryCode",
                  "targetField": "countryName",
                  "entries": [
                    {"key": "CN", "value": "China"},
                    {"key": "CN", "value": "Duplicate China"}
                  ]
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unique");
    }

    @Test
    void runAllowsDataQualityWithDirtyPortModeAndRules() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithTransform("DATA_QUALITY", """
                {
                  "mode": "DIRTY_PORT",
                  "errorField": "_streamcraft_quality_errors",
                  "rules": [
                    {
                      "field": "amount",
                      "ruleType": "NOT_NULL"
                    },
                    {
                      "field": "amount",
                      "ruleType": "TYPE",
                      "valueType": "NUMBER"
                    },
                    {
                      "field": "amount",
                      "ruleType": "RANGE",
                      "min": 0,
                      "max": 1000
                    },
                    {
                      "field": "status",
                      "ruleType": "ENUM",
                      "enumValues": ["PAID", "CANCELLED"]
                    },
                    {
                      "field": "status",
                      "ruleType": "REGEX",
                      "pattern": "^[A-Z_]+$"
                    },
                    {
                      "field": "comment",
                      "ruleType": "LENGTH",
                      "minLength": 2,
                      "maxLength": 120
                    }
                  ]
                }
                """)));
    }

    @Test
    void runRejectsDataQualityWithoutRules() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("DATA_QUALITY", """
                {
                  "mode": "DIRTY_PORT",
                  "rules": []
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rules");
    }

    @Test
    void runRejectsDataQualityWithInvalidPattern() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("DATA_QUALITY", """
                {
                  "mode": "DIRTY_PORT",
                  "rules": [
                    {
                      "field": "status",
                      "ruleType": "REGEX",
                      "pattern": "["
                    }
                  ]
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rule pattern");
    }

    @Test
    void runAllowsCustomCodeWithJavaSourceCode() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithTransform("CUSTOM_CODE", """
                {
                  "language": "JAVA",
                  "compilePattern": "SOURCE_CODE",
                  "className": "MyTransform",
                  "sourceCode": "public class MyTransform implements com.streamcraft.core.runtime.transform.custom.CustomTransform { public com.streamcraft.core.model.DataEntity process(com.streamcraft.core.model.DataEntity input, com.streamcraft.core.runtime.transform.custom.CustomTransformContext context) { return input; } }",
                  "errorStrategy": "KEEP_ORIGINAL"
                }
                """)));
    }

    @Test
    void runRejectsCustomCodeWithoutSourceCode() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("CUSTOM_CODE", """
                {
                  "language": "JAVA",
                  "compilePattern": "SOURCE_CODE",
                  "className": "MyTransform",
                  "errorStrategy": "KEEP_ORIGINAL"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sourceCode");
    }

    @Test
    void runRejectsCustomCodeWithUnsupportedErrorStrategy() {
        assertThatThrownBy(() -> validator.validateForRun(runtimeDefinitionWithTransform("CUSTOM_CODE", """
                {
                  "language": "JAVA",
                  "compilePattern": "SOURCE_CODE",
                  "className": "MyTransform",
                  "sourceCode": "public class MyTransform {}",
                  "errorStrategy": "IGNORE"
                }
                """)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("errorStrategy");
    }

    @Test
    void runtimePropertiesExposePreviewExecutionSettings() {
        assertThat(Arrays.stream(PipelineRuntimeProperties.class.getRecordComponents())
                .map(RecordComponent::getName))
                .contains("serviceBaseUrl", "testMode", "parallelism")
                .doesNotContain("coreJarPath", "previewTimeoutSeconds");
    }

    private String runtimeDefinitionWithDeserializeConfig(String deserializeConfig) {
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
                      "id": "deserialize-1",
                      "name": "Deserialize",
                      "type": "TRANSFORM",
                      "operator": "DESERIALIZE",
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
                      "targetNodeId": "deserialize-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "deserialize-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(deserializeConfig);
    }

    private String runtimeDefinitionWithSerializeConfig(String serializeConfig) {
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
                      "id": "serialize-1",
                      "name": "Serialize",
                      "type": "TRANSFORM",
                      "operator": "SERIALIZE",
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
                      "targetNodeId": "serialize-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "serialize-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(serializeConfig);
    }

    private String runtimeDefinitionWithTransform(String operator, String transformConfig) {
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
                      "id": "transform-1",
                      "name": "%s",
                      "type": "TRANSFORM",
                      "operator": "%s",
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
                      "targetNodeId": "transform-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "transform-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(operator, operator, transformConfig);
    }
}
