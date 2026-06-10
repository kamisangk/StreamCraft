package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorElasticsearchSourceContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void runValidationAllowsElasticsearchSourceFullMode() {
        assertThatNoException().isThrownBy(() -> validator.validateForRun(definitionWithElasticsearchSource("""
                {
                  "hosts": ["http://127.0.0.1:9200"],
                  "index": "orders-*",
                  "source": ["_id", "customerName", "amount"],
                  "query": {"match_all": {}},
                  "readMode": "FULL",
                  "scrollSize": 500
                }
                """)));
    }

    @Test
    void runValidationRejectsElasticsearchIncrementalSourceWithoutCursorField() {
        assertThatThrownBy(() -> validator.validateForRun(definitionWithElasticsearchSource("""
                {
                  "hosts": ["http://127.0.0.1:9200"],
                  "index": "orders",
                  "readMode": "INCREMENTAL"
                }
                """))).hasMessageContaining("cursorField");
    }

    @Test
    void previewValidationAllowsElasticsearchSourceSampleData() {
        assertThatNoException().isThrownBy(() -> validator.validateForPreview(definitionWithElasticsearchSource("""
                {
                  "format": "JSON",
                  "sampleData": ["{\\"id\\":1,\\"status\\":\\"ok\\"}"]
                }
                """)));
    }

    private String definitionWithElasticsearchSource(String sourceConfigJson) {
        return """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "type": "SOURCE",
                      "operator": "ELASTICSEARCH_SOURCE",
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
