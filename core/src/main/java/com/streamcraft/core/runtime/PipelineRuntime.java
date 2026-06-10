package com.streamcraft.core.runtime;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.runtime.graph.NodeInputKey;
import com.streamcraft.core.runtime.graph.NodePortKey;
import com.streamcraft.core.runtime.graph.RuntimeGraphPlanner;
import com.streamcraft.core.runtime.metrics.InputMetricsCollector;
import com.streamcraft.core.runtime.metrics.OutputMetricsCollector;
import com.streamcraft.core.runtime.transform.TransformOperatorFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PipelineRuntime {

    private static final String DEFAULT_OUTPUT_PORT = "output-0";
    private static final String DEFAULT_INPUT_PORT = "input-0";

    private final StreamExecutionEnvironment env;
    private final KafkaSourceFactory kafkaSourceFactory;
    private final MockSourceFactory mockSourceFactory;
    private final ElasticsearchSourceFactory elasticsearchSourceFactory;
    private final InfluxDbSourceFactory influxDbSourceFactory;
    private final HdfsFileSourceFactory hdfsFileSourceFactory;
    private final JdbcSourceFactory jdbcSourceFactory;
    private final KafkaSinkFactory kafkaSinkFactory;
    private final JdbcSinkFactory jdbcSinkFactory;
    private final ElasticsearchSinkFactory elasticsearchSinkFactory;
    private final InfluxDbSinkFactory influxDbSinkFactory;
    private final HdfsFileSinkFactory hdfsFileSinkFactory;
    private final TransformOperatorFactory transformFactory;
    private final boolean testMode;
    private final ExecutionMode executionMode;

    public PipelineRuntime(StreamExecutionEnvironment env) {
        this(env, false, ExecutionMode.RUN);
    }

    public PipelineRuntime(StreamExecutionEnvironment env, boolean testMode) {
        this(env, testMode, ExecutionMode.RUN);
    }

    public PipelineRuntime(StreamExecutionEnvironment env, boolean testMode, ExecutionMode executionMode) {
        this(env, new KafkaSourceFactory(), new MockSourceFactory(), new ElasticsearchSourceFactory(),
                new InfluxDbSourceFactory(), new HdfsFileSourceFactory(), new JdbcSourceFactory(),
                new KafkaSinkFactory(), new JdbcSinkFactory(), new ElasticsearchSinkFactory(),
                new InfluxDbSinkFactory(), new HdfsFileSinkFactory(), new TransformOperatorFactory(),
                testMode, executionMode);
    }

    public PipelineRuntime(StreamExecutionEnvironment env,
                           KafkaSourceFactory kafkaSourceFactory,
                           MockSourceFactory mockSourceFactory,
                           KafkaSinkFactory kafkaSinkFactory,
                           TransformOperatorFactory transformFactory,
                           boolean testMode) {
        this(env, kafkaSourceFactory, mockSourceFactory, new JdbcSourceFactory(), kafkaSinkFactory,
                new JdbcSinkFactory(), transformFactory, testMode, ExecutionMode.RUN);
    }

    public PipelineRuntime(StreamExecutionEnvironment env,
                           KafkaSourceFactory kafkaSourceFactory,
                           MockSourceFactory mockSourceFactory,
                           JdbcSourceFactory jdbcSourceFactory,
                           KafkaSinkFactory kafkaSinkFactory,
                           TransformOperatorFactory transformFactory,
                           boolean testMode) {
        this(env, kafkaSourceFactory, mockSourceFactory, jdbcSourceFactory, kafkaSinkFactory,
                new JdbcSinkFactory(), transformFactory, testMode, ExecutionMode.RUN);
    }

    public PipelineRuntime(StreamExecutionEnvironment env,
                           KafkaSourceFactory kafkaSourceFactory,
                           MockSourceFactory mockSourceFactory,
                           KafkaSinkFactory kafkaSinkFactory,
                           TransformOperatorFactory transformFactory,
                           boolean testMode,
                           ExecutionMode executionMode) {
        this(env, kafkaSourceFactory, mockSourceFactory, new JdbcSourceFactory(), kafkaSinkFactory,
                new JdbcSinkFactory(), transformFactory, testMode, executionMode);
    }

    public PipelineRuntime(StreamExecutionEnvironment env,
                           KafkaSourceFactory kafkaSourceFactory,
                           MockSourceFactory mockSourceFactory,
                           JdbcSourceFactory jdbcSourceFactory,
                           KafkaSinkFactory kafkaSinkFactory,
                           TransformOperatorFactory transformFactory,
                           boolean testMode,
                           ExecutionMode executionMode) {
        this(env, kafkaSourceFactory, mockSourceFactory, jdbcSourceFactory, kafkaSinkFactory,
                new JdbcSinkFactory(), transformFactory, testMode, executionMode);
    }

    public PipelineRuntime(StreamExecutionEnvironment env,
                           KafkaSourceFactory kafkaSourceFactory,
                           MockSourceFactory mockSourceFactory,
                           ElasticsearchSourceFactory elasticsearchSourceFactory,
                           JdbcSourceFactory jdbcSourceFactory,
                           KafkaSinkFactory kafkaSinkFactory,
                           JdbcSinkFactory jdbcSinkFactory,
                           TransformOperatorFactory transformFactory,
                           boolean testMode,
                           ExecutionMode executionMode) {
        this(env, kafkaSourceFactory, mockSourceFactory, elasticsearchSourceFactory, new InfluxDbSourceFactory(),
                new HdfsFileSourceFactory(), jdbcSourceFactory, kafkaSinkFactory, jdbcSinkFactory,
                new ElasticsearchSinkFactory(), new InfluxDbSinkFactory(), new HdfsFileSinkFactory(),
                transformFactory, testMode, executionMode);
    }

    public PipelineRuntime(StreamExecutionEnvironment env,
                           KafkaSourceFactory kafkaSourceFactory,
                           MockSourceFactory mockSourceFactory,
                           ElasticsearchSourceFactory elasticsearchSourceFactory,
                           InfluxDbSourceFactory influxDbSourceFactory,
                           JdbcSourceFactory jdbcSourceFactory,
                           KafkaSinkFactory kafkaSinkFactory,
                           JdbcSinkFactory jdbcSinkFactory,
                           ElasticsearchSinkFactory elasticsearchSinkFactory,
                           InfluxDbSinkFactory influxDbSinkFactory,
                           TransformOperatorFactory transformFactory,
                           boolean testMode,
                           ExecutionMode executionMode) {
        this(env, kafkaSourceFactory, mockSourceFactory, elasticsearchSourceFactory, influxDbSourceFactory,
                new HdfsFileSourceFactory(), jdbcSourceFactory, kafkaSinkFactory, jdbcSinkFactory,
                elasticsearchSinkFactory, influxDbSinkFactory, new HdfsFileSinkFactory(), transformFactory,
                testMode, executionMode);
    }

    public PipelineRuntime(StreamExecutionEnvironment env,
                           KafkaSourceFactory kafkaSourceFactory,
                           MockSourceFactory mockSourceFactory,
                           ElasticsearchSourceFactory elasticsearchSourceFactory,
                           InfluxDbSourceFactory influxDbSourceFactory,
                           HdfsFileSourceFactory hdfsFileSourceFactory,
                           JdbcSourceFactory jdbcSourceFactory,
                           KafkaSinkFactory kafkaSinkFactory,
                           JdbcSinkFactory jdbcSinkFactory,
                           ElasticsearchSinkFactory elasticsearchSinkFactory,
                           InfluxDbSinkFactory influxDbSinkFactory,
                           HdfsFileSinkFactory hdfsFileSinkFactory,
                           TransformOperatorFactory transformFactory,
                           boolean testMode,
                           ExecutionMode executionMode) {
        this.env = env;
        this.kafkaSourceFactory = kafkaSourceFactory;
        this.mockSourceFactory = mockSourceFactory;
        this.elasticsearchSourceFactory = elasticsearchSourceFactory;
        this.influxDbSourceFactory = influxDbSourceFactory;
        this.hdfsFileSourceFactory = hdfsFileSourceFactory;
        this.jdbcSourceFactory = jdbcSourceFactory;
        this.kafkaSinkFactory = kafkaSinkFactory;
        this.jdbcSinkFactory = jdbcSinkFactory;
        this.elasticsearchSinkFactory = elasticsearchSinkFactory;
        this.influxDbSinkFactory = influxDbSinkFactory;
        this.hdfsFileSinkFactory = hdfsFileSinkFactory;
        this.transformFactory = transformFactory;
        this.testMode = testMode;
        this.executionMode = executionMode == null ? ExecutionMode.RUN : executionMode;
    }

    public PipelineRuntime(StreamExecutionEnvironment env,
                           KafkaSourceFactory kafkaSourceFactory,
                           MockSourceFactory mockSourceFactory,
                           JdbcSourceFactory jdbcSourceFactory,
                           KafkaSinkFactory kafkaSinkFactory,
                           JdbcSinkFactory jdbcSinkFactory,
                           TransformOperatorFactory transformFactory,
                           boolean testMode,
                           ExecutionMode executionMode) {
        this(env, kafkaSourceFactory, mockSourceFactory, new ElasticsearchSourceFactory(), jdbcSourceFactory,
                kafkaSinkFactory, jdbcSinkFactory, transformFactory, testMode, executionMode);
    }

    public void run(PipelineDefinition definition) {
        RuntimeGraphPlanner.Plan plan = new RuntimeGraphPlanner().plan(definition);
        Map<NodePortKey, DataStream<DataEntity>> streamsByOutputPort = new HashMap<>();

        for (String nodeId : plan.topologicalNodeIds()) {
            PipelineNode node = plan.nodeById().get(nodeId);
            if (node.type() == PipelineNodeType.SOURCE) {
                streamsByOutputPort.put(defaultOutput(node.id()), createSource(node));
                continue;
            }
            if (node.type() == PipelineNodeType.TRANSFORM) {
                TransformOutputs outputs = transformFactory.apply(mergeInputsByPort(node, plan, streamsByOutputPort), node);
                outputs.streamsByPort().forEach((portId, stream) -> {
                    DataStream<DataEntity> outputWithMetrics = stream.map(new OutputMetricsCollector(node.id()))
                            .name("output-metrics-" + node.id() + "-" + portId);
                    streamsByOutputPort.put(new NodePortKey(node.id(), portId), outputWithMetrics);
                });
                continue;
            }
            if (node.type() == PipelineNodeType.SINK) {
                attachSink(node, mergeDefaultInput(node, plan, streamsByOutputPort));
            }
        }
    }

    private void attachSink(PipelineNode sinkNode, DataStream<DataEntity> input) {
        if (executionMode.interceptSinks()) {
            kafkaSinkFactory.attach(input, sinkNode);
            return;
        }

        switch (sinkNode.operator()) {
            case KAFKA_SINK -> kafkaSinkFactory.attach(input, sinkNode);
            case JDBC_SINK -> jdbcSinkFactory.attach(input, sinkNode);
            case ELASTICSEARCH_SINK -> elasticsearchSinkFactory.attach(input, sinkNode);
            case INFLUXDB_SINK -> influxDbSinkFactory.attach(input, sinkNode);
            case HDFS_FILE_SINK -> hdfsFileSinkFactory.attach(input, sinkNode);
            default -> throw new IllegalArgumentException(
                    "Unsupported sink operator for runtime execution: " + sinkNode.operator());
        }
    }

    private DataStream<DataEntity> createSource(PipelineNode sourceNode) {
        boolean useMockSource = executionMode.forceMockSources()
                || testMode
                || sourceNode.config().path("useMockSource").asBoolean(false);
        DataStream<DataEntity> sourceStream;
        if (useMockSource) {
            sourceStream = mockSourceFactory.create(env, sourceNode);
        } else {
            sourceStream = switch (sourceNode.operator()) {
                case KAFKA_SOURCE -> kafkaSourceFactory.create(env, sourceNode);
                case JDBC_SOURCE -> jdbcSourceFactory.create(env, sourceNode);
                case ELASTICSEARCH_SOURCE -> elasticsearchSourceFactory.create(env, sourceNode);
                case INFLUXDB_SOURCE -> influxDbSourceFactory.create(env, sourceNode);
                case HDFS_FILE_SOURCE -> hdfsFileSourceFactory.create(env, sourceNode);
                default -> throw new IllegalArgumentException(
                        "Unsupported source operator for runtime execution: " + sourceNode.operator());
            };
        }
        return sourceStream.map(new OutputMetricsCollector(sourceNode.id()))
                .name("output-metrics-" + sourceNode.id() + "-" + DEFAULT_OUTPUT_PORT);
    }

    private DataStream<DataEntity> mergeDefaultInput(PipelineNode node,
                                                     RuntimeGraphPlanner.Plan plan,
                                                     Map<NodePortKey, DataStream<DataEntity>> streamsByOutputPort) {
        Map<String, DataStream<DataEntity>> inputsByPort = mergeInputsByPort(node, plan, streamsByOutputPort);
        DataStream<DataEntity> input = inputsByPort.get(DEFAULT_INPUT_PORT);
        if (input == null) {
            throw new IllegalArgumentException("Node " + node.id() + " does not have an executable input path on input-0.");
        }
        return input;
    }

    private Map<String, DataStream<DataEntity>> mergeInputsByPort(PipelineNode node,
                                                                  RuntimeGraphPlanner.Plan plan,
                                                                  Map<NodePortKey, DataStream<DataEntity>> streamsByOutputPort) {
        Map<String, DataStream<DataEntity>> mergedInputs = new LinkedHashMap<>();
        for (var entry : plan.incomingByPort().entrySet()) {
            if (!node.id().equals(entry.getKey().nodeId())) {
                continue;
            }
            DataStream<DataEntity> merged = mergePortInputs(node, entry.getKey(), entry.getValue(), streamsByOutputPort);
            mergedInputs.put(entry.getKey().inputPortId(), merged);
        }
        if (mergedInputs.isEmpty()) {
            throw new IllegalArgumentException("Node " + node.id() + " does not have an executable input path.");
        }
        return Map.copyOf(mergedInputs);
    }

    private DataStream<DataEntity> mergePortInputs(PipelineNode node,
                                                   NodeInputKey inputKey,
                                                   List<com.streamcraft.core.model.PipelineEdge> edges,
                                                   Map<NodePortKey, DataStream<DataEntity>> streamsByOutputPort) {
        List<DataStream<DataEntity>> inputStreams = new ArrayList<>();
        for (var edge : edges) {
            DataStream<DataEntity> upstreamStream = streamsByOutputPort.get(
                    new NodePortKey(edge.sourceNodeId(), edge.sourcePortId()));
            if (upstreamStream == null) {
                throw new IllegalArgumentException("Missing stream for edge source node: " + edge.sourceNodeId());
            }
            inputStreams.add(upstreamStream);
        }
        DataStream<DataEntity> mergedStream = inputStreams.get(0);
        if (inputStreams.size() > 1) {
            @SuppressWarnings("unchecked")
            DataStream<DataEntity>[] additionalStreams = inputStreams.subList(1, inputStreams.size())
                    .toArray(DataStream[]::new);
            mergedStream = mergedStream.union(additionalStreams);
        }
        return mergedStream.map(new InputMetricsCollector(node.id()))
                .name("input-metrics-" + node.id() + "-" + inputKey.inputPortId());
    }

    private NodePortKey defaultOutput(String nodeId) {
        return new NodePortKey(nodeId, DEFAULT_OUTPUT_PORT);
    }
}
