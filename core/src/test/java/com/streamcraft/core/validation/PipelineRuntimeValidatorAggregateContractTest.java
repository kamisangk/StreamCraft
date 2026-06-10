package com.streamcraft.core.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import com.streamcraft.shared.aggregation.AggregateConfig;
import com.streamcraft.shared.aggregation.AggregateConfigParser;
import java.util.List;
import org.junit.jupiter.api.Test;

class PipelineRuntimeValidatorAggregateContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PipelineRuntimeValidator validator = new PipelineRuntimeValidator();

    @Test
    void acceptsGroupedTumblingAggregateConfig() {
        assertDoesNotThrow(() -> validator.validate(definitionWithAggregate("""
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
    void acceptsGlobalCountAggregateConfig() {
        assertDoesNotThrow(() -> validator.validate(definitionWithAggregate("""
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
    void parserDefaultsAggregateScopeAndWatermarkDelayForProductDefaults() throws Exception {
        JsonNode config = objectMapper.readTree("""
                {
                  "windowType": "TUMBLING_TIME",
                  "timeMode": "EVENT_TIME",
                  "timeUnit": "SECONDS",
                  "windowSize": 60,
                  "aggregations": [
                    {"function": "COUNT", "outputField": "count"}
                  ]
                }
                """);

        AggregateConfig parsed = AggregateConfigParser.parse(config, IllegalArgumentException::new);

        assertEquals(AggregateConfig.Mode.GLOBAL, parsed.mode());
        assertEquals("", parsed.eventTimeField());
        assertEquals(AggregateConfig.EventTimeUnit.MILLISECONDS, parsed.eventTimeUnit());
        assertEquals(30, parsed.watermarkDelay());
    }

    @Test
    void rejectsGroupedAggregateWithoutGroupBy() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithAggregate("""
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
    void rejectsSlidingWindowWhenSlideIsGreaterThanSize() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithAggregate("""
                        {
                          "mode": "GLOBAL",
                          "windowType": "SLIDING_TIME",
                          "timeMode": "PROCESSING_TIME",
                          "timeUnit": "SECONDS",
                          "windowSize": 10,
                          "windowSlide": 20,
                          "aggregations": [
                            {"function": "COUNT", "outputField": "count"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("windowSlide"));
    }

    @Test
    void rejectsEventTimeAggregateWithNegativeWatermarkDelay() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithAggregate("""
                        {
                          "mode": "GLOBAL",
                          "windowType": "TUMBLING_TIME",
                          "timeMode": "EVENT_TIME",
                          "timeUnit": "SECONDS",
                          "windowSize": 60,
                          "watermarkDelay": -1,
                          "aggregations": [
                            {"function": "COUNT", "outputField": "count"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("watermarkDelay"));
    }

    @Test
    void rejectsCountEventTimeAggregateWithNegativeWatermarkDelay() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithAggregate("""
                        {
                          "mode": "GLOBAL",
                          "windowType": "COUNT",
                          "timeMode": "EVENT_TIME",
                          "watermarkDelay": -1,
                          "countWindowSize": 100,
                          "aggregations": [
                            {"function": "COUNT", "outputField": "count"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("watermarkDelay"));
    }

    @Test
    void rejectsCountAggregateWithInvalidCountWindowSize() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithAggregate("""
                        {
                          "mode": "GLOBAL",
                          "windowType": "COUNT",
                          "countWindowSize": "abc",
                          "aggregations": [
                            {"function": "COUNT", "outputField": "count"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("countWindowSize"));
    }

    @Test
    void rejectsCountAggregateWithFractionalCountWindowSize() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithAggregate("""
                        {
                          "mode": "GLOBAL",
                          "windowType": "COUNT",
                          "countWindowSize": 2.9,
                          "aggregations": [
                            {"function": "COUNT", "outputField": "count"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("countWindowSize"));
        assertTrue(exception.getMessage().contains("valid integer"));
    }

    @Test
    void rejectsTumblingAggregateWithInvalidWindowSize() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithAggregate("""
                        {
                          "mode": "GLOBAL",
                          "windowType": "TUMBLING_TIME",
                          "timeMode": "PROCESSING_TIME",
                          "timeUnit": "SECONDS",
                          "windowSize": "abc",
                          "aggregations": [
                            {"function": "COUNT", "outputField": "count"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("windowSize"));
    }

    @Test
    void rejectsTumblingAggregateWithFractionalWindowSizeString() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithAggregate("""
                        {
                          "mode": "GLOBAL",
                          "windowType": "TUMBLING_TIME",
                          "timeMode": "PROCESSING_TIME",
                          "timeUnit": "SECONDS",
                          "windowSize": "1.5",
                          "aggregations": [
                            {"function": "COUNT", "outputField": "count"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("windowSize"));
        assertTrue(exception.getMessage().contains("valid integer"));
    }

    @Test
    void rejectsNumericAggregationWithoutField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithAggregate("""
                        {
                          "mode": "GLOBAL",
                          "windowType": "COUNT",
                          "countWindowSize": 100,
                          "aggregations": [
                            {"function": "SUM", "outputField": "amountSum"}
                          ]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("field"));
    }

    @Test
    void rejectsDuplicateAggregationOutputFields() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithAggregate("""
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

    private PipelineDefinition definitionWithAggregate(String aggregateConfig) {
        return new PipelineDefinition(
                "pipeline-aggregate",
                List.of(sourceNode(), aggregateNode(aggregateConfig), sinkNode()),
                List.of(
                        new PipelineEdge("edge-1", "source-1", "output-0", "aggregate-1", "input-0"),
                        new PipelineEdge("edge-2", "aggregate-1", "output-0", "sink-1", "input-0")));
    }

    private PipelineNode sourceNode() {
        return new PipelineNode(
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
                        """));
    }

    private PipelineNode aggregateNode(String aggregateConfig) {
        return new PipelineNode(
                "aggregate-1",
                "Aggregate",
                PipelineNodeType.TRANSFORM,
                PipelineOperator.AGGREGATE,
                jsonNode(aggregateConfig));
    }

    private PipelineNode sinkNode() {
        return new PipelineNode(
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
                        """));
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
