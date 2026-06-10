package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorJdbcSourceContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void runValidationAllowsJdbcSourceFullMode() {
        assertThatNoException().isThrownBy(() -> validator.validateForRun(definitionWithJdbcSource("""
                {
                  "url": "jdbc:postgresql://127.0.0.1:5432/dw",
                  "driver": "org.postgresql.Driver",
                  "query": "select id, name from dim_customer",
                  "readMode": "FULL"
                }
                """)));
    }

    @Test
    void runValidationRejectsJdbcIncrementalSourceWithoutCursorField() {
        assertThatThrownBy(() -> validator.validateForRun(definitionWithJdbcSource("""
                {
                  "url": "jdbc:postgresql://127.0.0.1:5432/dw",
                  "driver": "org.postgresql.Driver",
                  "query": "select id, updated_at from orders",
                  "readMode": "INCREMENTAL"
                }
                """))).hasMessageContaining("cursorField");
    }

    @Test
    void previewValidationAllowsJdbcSourceSampleData() {
        assertThatNoException().isThrownBy(() -> validator.validateForPreview(definitionWithJdbcSource("""
                {
                  "format": "JSON",
                  "sampleData": ["{\\"id\\":1,\\"status\\":\\"ok\\"}"]
                }
                """)));
    }

    private String definitionWithJdbcSource(String sourceConfigJson) {
        return """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "type": "SOURCE",
                      "operator": "JDBC_SOURCE",
                      "config": %s
                    },
                    {
                      "id": "sink-1",
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
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """.formatted(sourceConfigJson);
    }
}
