package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import com.streamcraft.core.runtime.transform.TransformOperatorFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PipelineRuntimeHdfsFileSourceExecutionTest {

    private static final Map<String, List<Map<String, Object>>> CAPTURED_RECORDS = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    @Test
    void hdfsFileSourceReadsJsonLinesFromFlinkFileSystem() throws Exception {
        Path inputDir = Files.createDirectories(tempDir.resolve("dt=20260521"));
        Files.writeString(inputDir.resolve("orders.json"), """
                {"id":"o-1","ts":1000,"amount":12.5}
                {"id":"o-2","ts":2000,"amount":18.0}
                """);

        List<Map<String, Object>> records = execute(
                "sink-hdfs-source",
                definitionWithHdfsFileSource(tempDir.toUri().toString(), "sink-hdfs-source"),
                "hdfs-file-source-json-test");

        assertEquals(2, records.size());
        assertEquals("o-1", records.get(0).get("id"));
        assertEquals(12.5, records.get(0).get("amount"));
        assertEquals("20260521", records.get(0).get("dt"));
        assertEquals("o-2", records.get(1).get("id"));
    }

    private List<Map<String, Object>> execute(String sinkId, PipelineDefinition definition, String jobName)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CAPTURED_RECORDS.clear();

        PipelineRuntime runtime = new PipelineRuntime(
                env,
                new KafkaSourceFactory(),
                new MockSourceFactory(),
                new ElasticsearchSourceFactory(),
                new InfluxDbSourceFactory(),
                new HdfsFileSourceFactory(),
                new JdbcSourceFactory(),
                new CapturingKafkaSinkFactory(),
                new JdbcSinkFactory(),
                new ElasticsearchSinkFactory(),
                new InfluxDbSinkFactory(),
                new HdfsFileSinkFactory(),
                new TransformOperatorFactory(),
                false,
                ExecutionMode.RUN);

        runtime.run(definition);
        env.execute(jobName);
        return CAPTURED_RECORDS.getOrDefault(sinkId, List.of());
    }

    private PipelineDefinition definitionWithHdfsFileSource(String path, String sinkId) {
        return new PipelineDefinition(
                "pipeline-hdfs-file-source",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "HDFS File Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.HDFS_FILE_SOURCE,
                                jsonNode("""
                                        {
                                          "fs.defaultFS": "file:///",
                                          "path": "%s",
                                          "file_format_type": "json",
                                          "parse_partition_from_path": true,
                                          "file_filter_pattern": ".*orders\\\\.json",
                                          "idField": "id",
                                          "timestampField": "ts"
                                        }
                                        """.formatted(path))),
                        new PipelineNode(
                                sinkId,
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("{}"))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", sinkId, "input-0")));
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static final class CapturingKafkaSinkFactory extends KafkaSinkFactory {

        @Override
        public void attach(DataStream<DataEntity> input, PipelineNode sinkNode) {
            String sinkId = sinkNode.id();
            input.addSink(new RichSinkFunction<>() {
                @Override
                public void invoke(DataEntity value, Context context) {
                    CAPTURED_RECORDS
                            .computeIfAbsent(sinkId, ignored -> Collections.synchronizedList(new ArrayList<>()))
                            .add(value.fields());
                }
            }).name("capture-" + sinkId);
        }
    }
}
