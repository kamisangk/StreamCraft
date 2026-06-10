package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorInfluxDbSinkContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void runValidationAllowsInfluxDbSink() {
        assertThatNoException().isThrownBy(() -> validator.validateForRun(definitionWithInfluxDbSink("""
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
        assertThatThrownBy(() -> validator.validateForRun(definitionWithInfluxDbSink("""
                {
                  "url": "http://127.0.0.1:8086",
                  "database": "metrics"
                }
                """))).hasMessageContaining("measurement");
    }

    private String definitionWithInfluxDbSink(String sinkConfigJson) {
        return """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["metrics"],
                        "groupId": "group-1",
                        "consumeMode": "earliest",
                        "authType": "NONE",
                        "format": "JSON"
                      }
                    },
                    {
                      "id": "sink-1",
                      "type": "SINK",
                      "operator": "INFLUXDB_SINK",
                      "config": %s
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
                """.formatted(sinkConfigJson);
    }
}
