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

class PipelineRuntimeValidatorHdfsFileSinkContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PipelineRuntimeValidator validator = new PipelineRuntimeValidator();

    @Test
    void runValidationAllowsHdfsFileSink() {
        assertDoesNotThrow(() -> validator.validate(definitionWithHdfsFileSink("""
                {
                  "fs.defaultFS": "hdfs://nameservice1",
                  "path": "/warehouse/dwd/orders",
                  "file_format_type": "json"
                }
                """)));
    }

    @Test
    void runValidationRejectsHdfsFileSinkWithoutDefaultFs() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithHdfsFileSink("""
                        {
                          "path": "/warehouse/dwd/orders",
                          "file_format_type": "json"
                        }
                        """)));

        assertTrue(exception.getMessage().contains("fs.defaultFS"));
    }

    private PipelineDefinition definitionWithHdfsFileSink(String sinkConfigJson) {
        return new PipelineDefinition(
                "pipeline-hdfs-file-sink",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topics": ["orders"],
                                          "groupId": "group-1",
                                          "consumeMode": "earliest",
                                          "authType": "NONE",
                                          "format": "JSON"
                                        }
                                        """)),
                        new PipelineNode(
                                "sink-1",
                                "HDFS File Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.HDFS_FILE_SINK,
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
