package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorHdfsFileSourceContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void runValidationAllowsHdfsFileSource() {
        assertThatNoException().isThrownBy(() -> validator.validateForRun(definitionWithHdfsFileSource("""
                {
                  "fs.defaultFS": "hdfs://nameservice1",
                  "path": "/warehouse/ods/orders",
                  "file_format_type": "json"
                }
                """)));
    }

    @Test
    void runValidationRejectsHdfsFileSourceWithoutFormat() {
        assertThatThrownBy(() -> validator.validateForRun(definitionWithHdfsFileSource("""
                {
                  "fs.defaultFS": "hdfs://nameservice1",
                  "path": "/warehouse/ods/orders"
                }
                """))).hasMessageContaining("file_format_type");
    }

    private String definitionWithHdfsFileSource(String sourceConfigJson) {
        return """
                {
                  "nodes": [
                    {
                      "id": "source-1",
                      "type": "SOURCE",
                      "operator": "HDFS_FILE_SOURCE",
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
