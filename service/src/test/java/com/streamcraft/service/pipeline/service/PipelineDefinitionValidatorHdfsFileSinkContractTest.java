package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorHdfsFileSinkContractTest {

    private final PipelineDefinitionValidator validator = new PipelineDefinitionValidator(new ObjectMapper());

    @Test
    void runValidationAllowsHdfsFileSink() {
        assertThatNoException().isThrownBy(() -> validator.validateForRun(definitionWithHdfsFileSink("""
                {
                  "fs.defaultFS": "hdfs://nameservice1",
                  "path": "/warehouse/dwd/orders",
                  "file_format_type": "json"
                }
                """)));
    }

    @Test
    void runValidationRejectsHdfsFileSinkWithoutPath() {
        assertThatThrownBy(() -> validator.validateForRun(definitionWithHdfsFileSink("""
                {
                  "fs.defaultFS": "hdfs://nameservice1",
                  "file_format_type": "json"
                }
                """))).hasMessageContaining("path");
    }

    private String definitionWithHdfsFileSink(String sinkConfigJson) {
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
                      "operator": "HDFS_FILE_SINK",
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
