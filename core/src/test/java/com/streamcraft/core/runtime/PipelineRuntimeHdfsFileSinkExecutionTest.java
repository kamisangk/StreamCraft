package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PipelineRuntimeHdfsFileSinkExecutionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    @Test
    void hdfsFileSinkWritesJsonLinesThroughFlinkFileSystem() throws Exception {
        Path outputDir = Files.createDirectories(tempDir.resolve("out"));

        execute(definitionWithHdfsFileSink(outputDir.toUri().toString()), "hdfs-file-sink-json-test");

        List<Path> outputFiles;
        try (var stream = Files.list(outputDir)) {
            outputFiles = stream.filter(Files::isRegularFile).toList();
        }
        assertEquals(1, outputFiles.size());
        String content = Files.readString(outputFiles.get(0));
        assertTrue(content.contains("{\"id\":\"o-1\",\"amount\":12.5}"));
        assertTrue(content.contains("{\"id\":\"o-2\",\"amount\":18.0}"));
        assertTrue(content.endsWith("\n"));
    }

    private void execute(PipelineDefinition definition, String jobName) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        new PipelineRuntime(env, true).run(definition);
        env.execute(jobName);
    }

    private PipelineDefinition definitionWithHdfsFileSink(String path) {
        return new PipelineDefinition(
                "pipeline-hdfs-file-sink",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                sourceConfig()),
                        new PipelineNode(
                                "sink-1",
                                "HDFS File Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.HDFS_FILE_SINK,
                                jsonNode("""
                                        {
                                          "fs.defaultFS": "file:///",
                                          "path": "%s",
                                          "tmp_path": "%s",
                                          "file_format_type": "json",
                                          "sink_columns": ["id", "amount"],
                                          "custom_filename": true,
                                          "file_name_expression": "orders-${now}",
                                          "filename_time_format": "yyyyMMddHHmmss",
                                          "batch_size": 2
                                        }
                                        """.formatted(path, tempDir.resolve("tmp").toUri())))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));
    }

    private JsonNode sourceConfig() {
        return jsonNode("""
                {
                  "bootstrapServers": "127.0.0.1:9092",
                  "topics": ["orders"],
                  "groupId": "group-1",
                  "consumeMode": "earliest",
                  "authType": "NONE",
                  "format": "JSON",
                  "sampleData": [
                    "{\\"id\\":\\"o-1\\",\\"amount\\":12.5,\\"ignored\\":true}",
                    "{\\"id\\":\\"o-2\\",\\"amount\\":18.0,\\"ignored\\":false}"
                  ]
                }
                """);
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
