package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorElasticsearchSinkContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void runValidationAllowsElasticsearchSink() {
        assertThatNoException().isThrownBy(() -> validator.validateForRun(definitionWithElasticsearchSink("""
                {
                  "hosts": ["http://127.0.0.1:9200"],
                  "index": "orders-${region}",
                  "primaryKeys": ["id"],
                  "maxBatchSize": 100
                }
                """)));
    }

    @Test
    void runValidationRejectsElasticsearchSinkWithoutHosts() {
        assertThatThrownBy(() -> validator.validateForRun(definitionWithElasticsearchSink("""
                {
                  "index": "orders"
                }
                """))).hasMessageContaining("hosts");
    }

    private String definitionWithElasticsearchSink(String sinkConfigJson) {
        return """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "type": "SOURCE",
                      "operator": "KAFKA_SOURCE",
                      "config": {
                        "bootstrapServers": "127.0.0.1:9092",
                        "topics": ["orders"],
                        "groupId": "group-1",
                        "consumeMode": "earliest",
                        "authType": "NONE",
                        "format": "JSON"
                      }
                    },
                    {
                      "id": "sink-1",
                      "type": "SINK",
                      "operator": "ELASTICSEARCH_SINK",
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
