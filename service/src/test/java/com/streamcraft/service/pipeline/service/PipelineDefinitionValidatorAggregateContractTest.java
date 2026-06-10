package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.config.UiMessageService;
import java.util.Locale;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.context.support.ResourceBundleMessageSource;

class PipelineDefinitionValidatorAggregateContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @AfterEach
    void resetLocale() {
        LocaleContextHolder.resetLocaleContext();
    }

    @Test
    void runAllowsAggregateTransformWithGroupedTumblingProcessingTimeCountAndSum() {
        assertDoesNotThrow(() -> validator.validateForRun(runtimeDefinitionWithAggregate("""
                {
                  "mode": "GROUPED",
                  "groupBy": ["user.id", "region"],
                  "windowType": "TUMBLING_TIME",
                  "timeMode": "PROCESSING_TIME",
                  "timeUnit": "SECONDS",
                  "windowSize": 60,
                  "aggregations": [
                    {"function": "COUNT", "outputField": "count"},
                    {"function": "SUM", "field": "amount", "outputField": "amountSum"}
                  ]
                }
                """)));
    }

    @Test
    void previewAllowsAggregateTransformWithGlobalCountWindow() {
        assertDoesNotThrow(() -> validator.validateForPreview(previewDefinitionWithAggregate("""
                {
                  "mode": "GLOBAL",
                  "windowType": "COUNT",
                  "countWindowSize": 100,
                  "aggregations": [
                    {"function": "COUNT", "outputField": "count"}
                  ]
                }
                """)));
    }

    @Test
    void saveAllowsValidAggregateTransformConfig() {
        assertDoesNotThrow(() -> validator.validateForSave(saveDefinitionWithAggregate("""
                {
                  "mode": "GLOBAL",
                  "windowType": "COUNT",
                  "countWindowSize": 100,
                  "aggregations": [
                    {"function": "COUNT", "outputField": "count"}
                  ]
                }
                """)));
    }

    @Test
    void saveRejectsGroupedAggregateWithoutGroupBy() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForSave(saveDefinitionWithAggregate("""
                        {
                          "mode": "GROUPED",
                          "groupBy": [],
                          "windowType": "COUNT",
                          "countWindowSize": 100,
                          "aggregations": [
                            {"function": "COUNT", "outputField": "count"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("groupBy"));
    }

    @Test
    void saveLocalizesAggregateValidationErrorsForCurrentLocale() {
        PipelineDefinitionValidator localizedValidator =
                new PipelineDefinitionValidator(new ObjectMapper(), new UiMessageService(messageSource()));
        LocaleContextHolder.setLocale(Locale.SIMPLIFIED_CHINESE);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> localizedValidator.validateForSave(saveDefinitionWithAggregate("""
                        {
                          "mode": "GROUPED",
                          "groupBy": [],
                          "windowType": "COUNT",
                          "countWindowSize": 100,
                          "aggregations": [
                            {"function": "COUNT", "outputField": "count"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("\u805a\u5408\u914d\u7f6e"));
        assertTrue(exception.getMessage().contains("groupBy"));
        assertTrue(exception.getMessage().contains("\u5206\u7ec4"));
        assertTrue(!exception.getMessage().contains("Aggregate config"));
    }

    @Test
    void saveRejectsFractionalTimeWindowSizeWithLocalizedIntegerMessage() {
        PipelineDefinitionValidator localizedValidator =
                new PipelineDefinitionValidator(new ObjectMapper(), new UiMessageService(messageSource()));
        LocaleContextHolder.setLocale(Locale.US);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> localizedValidator.validateForSave(saveDefinitionWithAggregate("""
                        {
                          "mode": "GLOBAL",
                          "windowType": "TUMBLING_TIME",
                          "timeMode": "PROCESSING_TIME",
                          "timeUnit": "SECONDS",
                          "windowSize": 1.5,
                          "aggregations": [
                            {"function": "COUNT", "outputField": "count"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("windowSize"));
        assertTrue(exception.getMessage().contains("valid integer"));
    }

    @Test
    void previewRejectsFractionalCountWindowSize() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForPreview(previewDefinitionWithAggregate("""
                        {
                          "mode": "GLOBAL",
                          "windowType": "COUNT",
                          "countWindowSize": "2.9",
                          "aggregations": [
                            {"function": "COUNT", "outputField": "count"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("countWindowSize"));
        assertTrue(exception.getMessage().contains("valid integer"));
    }

    @Test
    void runRejectsDuplicateOutputFields() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateForRun(runtimeDefinitionWithAggregate("""
                        {
                          "mode": "GLOBAL",
                          "windowType": "COUNT",
                          "countWindowSize": 100,
                          "aggregations": [
                            {"function": "COUNT", "outputField": "value"},
                            {"function": "SUM", "field": "amount", "outputField": "value"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("outputField"));
    }

    private String runtimeDefinitionWithAggregate(String aggregateConfig) {
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
                      "id": "aggregate-1",
                      "name": "Aggregate",
                      "type": "TRANSFORM",
                      "operator": "AGGREGATE",
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
                      "targetNodeId": "aggregate-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "aggregate-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(aggregateConfig);
    }

    private String previewDefinitionWithAggregate(String aggregateConfig) {
        return """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "name": "Kafka Source",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "format": "JSON",
                        "sampleData": ["{\\\"id\\\":1,\\\"amount\\\":12}"]
                      }
                    },
                    {
                      "id": "aggregate-1",
                      "name": "Aggregate",
                      "type": "TRANSFORM",
                      "operator": "AGGREGATE",
                      "config": %s
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
                      "targetNodeId": "aggregate-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "aggregate-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(aggregateConfig);
    }

    private String saveDefinitionWithAggregate(String aggregateConfig) {
        return """
                {
                  "nodes": [
                    {"id": "source-1", "type": "SOURCE", "operator": "KAFKA_SOURCE"},
                    {
                      "id": "aggregate-1",
                      "type": "TRANSFORM",
                      "operator": "AGGREGATE",
                      "config": %s
                    },
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "aggregate-1",
                      "targetPortId": "input-0"
                    },
                    {
                      "id": "edge-2",
                      "sourceNodeId": "aggregate-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(aggregateConfig);
    }

    private ResourceBundleMessageSource messageSource() {
        ResourceBundleMessageSource source = new ResourceBundleMessageSource();
        source.setBasename("messages");
        source.setDefaultEncoding("UTF-8");
        source.setFallbackToSystemLocale(false);
        return source;
    }
}
