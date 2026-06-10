package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorInfluxDbSourceContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void runValidationAllowsInfluxDbSource() {
        assertThatNoException().isThrownBy(() -> validator.validateForRun(definitionWithInfluxDbSource("""
                {
                  "url": "http://127.0.0.1:8086",
                  "database": "metrics",
                  "sql": "select * from cpu",
                  "epoch": "ms"
                }
                """)));
    }

    @Test
    void runValidationRejectsInfluxDbSourceWithoutSql() {
        assertThatThrownBy(() -> validator.validateForRun(definitionWithInfluxDbSource("""
                {
                  "url": "http://127.0.0.1:8086",
                  "database": "metrics"
                }
                """))).hasMessageContaining("sql");
    }

    private String definitionWithInfluxDbSource(String sourceConfigJson) {
        return """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "type": "SOURCE",
                      "operator": "INFLUXDB_SOURCE",
                      "config": %s
                    },
                    {
                      "id": "sink-1",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topic": "metrics-out",
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
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(sourceConfigJson);
    }
}
