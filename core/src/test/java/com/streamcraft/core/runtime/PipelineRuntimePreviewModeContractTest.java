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
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class PipelineRuntimePreviewModeContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void previewModeForcesKafkaSourcesToUseMockFactory() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TrackingKafkaSourceFactory kafkaSourceFactory = new TrackingKafkaSourceFactory();
        TrackingMockSourceFactory mockSourceFactory = new TrackingMockSourceFactory();

        PipelineRuntime runtime = new PipelineRuntime(
                env,
                kafkaSourceFactory,
                mockSourceFactory,
                new NoOpKafkaSinkFactory(),
                new TransformOperatorFactory(),
                false,
                ExecutionMode.PREVIEW);

        runtime.run(definitionWithKafkaSource(false));

        assertEquals(0, kafkaSourceFactory.createCount);
        assertEquals(1, mockSourceFactory.createCount);
    }

    @Test
    void runModeKeepsKafkaSourceWhenSampleModeIsDisabled() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TrackingKafkaSourceFactory kafkaSourceFactory = new TrackingKafkaSourceFactory();
        TrackingMockSourceFactory mockSourceFactory = new TrackingMockSourceFactory();

        PipelineRuntime runtime = new PipelineRuntime(
                env,
                kafkaSourceFactory,
                mockSourceFactory,
                new NoOpKafkaSinkFactory(),
                new TransformOperatorFactory(),
                false,
                ExecutionMode.RUN);

        runtime.run(definitionWithKafkaSource(false));

        assertEquals(1, kafkaSourceFactory.createCount);
        assertEquals(0, mockSourceFactory.createCount);
    }

    private PipelineDefinition definitionWithKafkaSource(boolean useMockSource) {
        return new PipelineDefinition(
                "preview-pipeline",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "useMockSource": %s,
                                          "format": "JSON",
                                          "sampleData": [
                                            "{\\\"status\\\":\\\"ok\\\"}"
                                          ]
                                        }
                                        """.formatted(useMockSource))),
                        new PipelineNode(
                                "sink-1",
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("{}"))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static final class TrackingKafkaSourceFactory extends KafkaSourceFactory {

        private int createCount;

        @Override
        public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
            createCount++;
            return env.fromCollection(List.of(new DataEntity("kafka", 1L, Map.of("source", "kafka"), Map.of())));
        }
    }

    private static final class TrackingMockSourceFactory extends MockSourceFactory {

        private int createCount;

        @Override
        public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
            createCount++;
            return env.fromCollection(List.of(new DataEntity("mock", 1L, Map.of("source", "mock"), Map.of())));
        }
    }

    private static final class NoOpKafkaSinkFactory extends KafkaSinkFactory {

        @Override
        public void attach(DataStream<DataEntity> stream, PipelineNode sinkNode) {
            // no-op for source selection contract coverage
        }
    }
}
