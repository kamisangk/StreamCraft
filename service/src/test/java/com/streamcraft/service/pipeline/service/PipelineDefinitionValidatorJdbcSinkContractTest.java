package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorJdbcSinkContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void runValidationAllowsJdbcSinkInsertMode() {
        assertThatNoException().isThrownBy(() -> validator.validateForRun(definitionWithJdbcSink("""
                {
                  "url": "jdbc:postgresql://127.0.0.1:5432/dw",
                  "driver": "org.postgresql.Driver",
                  "tablePath": "dwd_orders",
                  "writeMode": "INSERT",
                  "fields": ["id", "customer_name", "amount"],
                  "batchSize": 500
                }
                """)));
    }

    @Test
    void runValidationRejectsJdbcUpsertSinkWithoutKeyFields() {
        assertThatThrownBy(() -> validator.validateForRun(definitionWithJdbcSink("""
                {
                  "url": "jdbc:postgresql://127.0.0.1:5432/dw",
                  "driver": "org.postgresql.Driver",
                  "tablePath": "dwd_orders",
                  "writeMode": "UPSERT",
                  "fields": ["id", "customer_name", "amount"]
                }
                """))).hasMessageContaining("keyFields");
    }

    @Test
    void previewValidationAllowsJdbcSinkBecausePreviewInterceptsOutput() {
        assertThatNoException().isThrownBy(() -> validator.validateForPreview(definitionWithJdbcSink("""
                {
                  "writeMode": "INSERT"
                }
                """)));
    }

    private String definitionWithJdbcSink(String sinkConfigJson) {
        return """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "format": "JSON",
                        "sampleData": ["{\\"id\\":1,\\"customer_name\\":\\"Ada\\",\\"amount\\":99.5}"],
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["orders"],
                        "groupId": "group-1",
                        "consumeMode": "earliest",
                        "authType": "NONE"
                      }
                    },
                    {
                      "id": "sink-1",
                      "type": "SINK",
                      "operator": "JDBC_SINK",
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
