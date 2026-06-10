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
import java.util.List;
import org.junit.jupiter.api.Test;

class PipelineRuntimeValidatorInfluxDbSinkContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PipelineRuntimeValidator validator = new PipelineRuntimeValidator();

    @Test
    void runValidationAllowsInfluxDbSink() {
        assertDoesNotThrow(() -> validator.validate(definitionWithInfluxDbSink("""
                {
                  "url": "http://127.0.0.1:8086",
                  "database": "metrics",
                  "measurement": "cpu_${region}",
                  "keyTags": ["host"],
                  "fields": ["usage"]
                }
                """)));
    }

    @Test
    void runValidationRejectsInfluxDbSinkWithoutMeasurement() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithInfluxDbSink("""
                        {
                          "url": "http://127.0.0.1:8086",
                          "database": "metrics"
                        }
                        """)));

        assertTrue(exception.getMessage().contains("measurement"));
    }

    private PipelineDefinition definitionWithInfluxDbSink(String sinkConfigJson) {
        return new PipelineDefinition(
                "pipeline-influxdb-sink",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topics": ["metrics"],
                                          "groupId": "group-1",
                                          "consumeMode": "earliest",
                                          "authType": "NONE",
                                          "format": "JSON"
                                        }
                                        """)),
                        new PipelineNode(
                                "sink-1",
                                "InfluxDB Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.INFLUXDB_SINK,
                                jsonNode(sinkConfigJson))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
