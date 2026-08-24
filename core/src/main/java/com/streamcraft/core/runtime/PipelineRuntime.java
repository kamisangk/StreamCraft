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

    public PipelineRuntime(StreamExecutionEnvironment env,
                           PipelineRuntimeDependencies dependencies,
                           boolean testMode,
                           ExecutionMode executionMode) {
        this.env = env;
        this.kafkaSourceFactory = dependencies.kafkaSourceFactory();
        this.mockSourceFactory = dependencies.mockSourceFactory();
        this.elasticsearchSourceFactory = dependencies.elasticsearchSourceFactory();
        this.influxDbSourceFactory = dependencies.influxDbSourceFactory();
        this.hdfsFileSourceFactory = dependencies.hdfsFileSourceFactory();
        this.jdbcSourceFactory = dependencies.jdbcSourceFactory();
        this.kafkaSinkFactory = dependencies.kafkaSinkFactory();
        this.jdbcSinkFactory = dependencies.jdbcSinkFactory();
        this.elasticsearchSinkFactory = dependencies.elasticsearchSinkFactory();
        this.influxDbSinkFactory = dependencies.influxDbSinkFactory();
        this.hdfsFileSinkFactory = dependencies.hdfsFileSinkFactory();
        this.transformFactory = dependencies.transformFactory();
        this.testMode = testMode;
        this.executionMode = executionMode == null ? ExecutionMode.RUN : executionMode;
    }

    public PipelineRuntime(StreamExecutionEnvironment env,
                           KafkaSourceFactory kafkaSourceFactory,
                           MockSourceFactory mockSourceFactory,
                           KafkaSinkFactory kafkaSinkFactory,
                           TransformOperatorFactory transformFactory,
                           boolean testMode,
                           ExecutionMode executionMode) {
        this(env, PipelineRuntimeDependencies.builder()
                .kafkaSourceFactory(kafkaSourceFactory)
                .mockSourceFactory(mockSourceFactory)
                .elasticsearchSourceFactory(new ElasticsearchSourceFactory())
                .influxDbSourceFactory(new InfluxDbSourceFactory())
                .hdfsFileSourceFactory(new HdfsFileSourceFactory())
                .jdbcSourceFactory(new JdbcSourceFactory())
                .kafkaSinkFactory(kafkaSinkFactory)
                .jdbcSinkFactory(new JdbcSinkFactory())
                .elasticsearchSinkFactory(new ElasticsearchSinkFactory())
                .influxDbSinkFactory(new InfluxDbSinkFactory())
                .hdfsFileSinkFactory(new HdfsFileSinkFactory())
                .transformFactory(transformFactory)
                .build(), testMode, executionMode);
    }

    public PipelineRuntime(StreamExecutionEnvironment env,
                           KafkaSourceFactory kafkaSourceFactory,
                           MockSourceFactory mockSourceFactory,
                           JdbcSourceFactory jdbcSourceFactory,
                           KafkaSinkFactory kafkaSinkFactory,
                           TransformOperatorFactory transformFactory,
                           boolean testMode,
                           ExecutionMode executionMode) {
        this(env, PipelineRuntimeDependencies.builder()
                .kafkaSourceFactory(kafkaSourceFactory)
                .mockSourceFactory(mockSourceFactory)
                .elasticsearchSourceFactory(new ElasticsearchSourceFactory())
                .influxDbSourceFactory(new InfluxDbSourceFactory())
                .hdfsFileSourceFactory(new HdfsFileSourceFactory())
                .jdbcSourceFactory(jdbcSourceFactory)
                .kafkaSinkFactory(kafkaSinkFactory)
                .jdbcSinkFactory(new JdbcSinkFactory())
                .elasticsearchSinkFactory(new ElasticsearchSinkFactory())
                .influxDbSinkFactory(new InfluxDbSinkFactory())
                .hdfsFileSinkFactory(new HdfsFileSinkFactory())
                .transformFactory(transformFactory)
                .build(), testMode, executionMode);
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
        this(env, PipelineRuntimeDependencies.builder()
                .kafkaSourceFactory(kafkaSourceFactory)
                .mockSourceFactory(mockSourceFactory)
                .elasticsearchSourceFactory(elasticsearchSourceFactory)
                .influxDbSourceFactory(new InfluxDbSourceFactory())
                .hdfsFileSourceFactory(new HdfsFileSourceFactory())
                .jdbcSourceFactory(jdbcSourceFactory)
                .kafkaSinkFactory(kafkaSinkFactory)
                .jdbcSinkFactory(jdbcSinkFactory)
                .elasticsearchSinkFactory(new ElasticsearchSinkFactory())
                .influxDbSinkFactory(new InfluxDbSinkFactory())
                .hdfsFileSinkFactory(new HdfsFileSinkFactory())
                .transformFactory(transformFactory)
                .build(), testMode, executionMode);
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
        this(env, PipelineRuntimeDependencies.builder()
                .kafkaSourceFactory(kafkaSourceFactory)
                .mockSourceFactory(mockSourceFactory)
                .elasticsearchSourceFactory(elasticsearchSourceFactory)
                .influxDbSourceFactory(influxDbSourceFactory)
                .hdfsFileSourceFactory(new HdfsFileSourceFactory())
                .jdbcSourceFactory(jdbcSourceFactory)
                .kafkaSinkFactory(kafkaSinkFactory)
                .jdbcSinkFactory(jdbcSinkFactory)
                .elasticsearchSinkFactory(elasticsearchSinkFactory)
                .influxDbSinkFactory(influxDbSinkFactory)
                .hdfsFileSinkFactory(new HdfsFileSinkFactory())
                .transformFactory(transformFactory)
                .build(), testMode, executionMode);
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
        this(env, PipelineRuntimeDependencies.builder()
                .kafkaSourceFactory(kafkaSourceFactory)
                .mockSourceFactory(mockSourceFactory)
                .elasticsearchSourceFactory(elasticsearchSourceFactory)
                .influxDbSourceFactory(influxDbSourceFactory)
                .hdfsFileSourceFactory(hdfsFileSourceFactory)
                .jdbcSourceFactory(jdbcSourceFactory)
                .kafkaSinkFactory(kafkaSinkFactory)
                .jdbcSinkFactory(jdbcSinkFactory)
                .elasticsearchSinkFactory(elasticsearchSinkFactory)
                .influxDbSinkFactory(influxDbSinkFactory)
                .hdfsFileSinkFactory(hdfsFileSinkFactory)
                .transformFactory(transformFactory)
                .build(), testMode, executionMode);
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
        this(env, PipelineRuntimeDependencies.builder()
                .kafkaSourceFactory(kafkaSourceFactory)
                .mockSourceFactory(mockSourceFactory)
                .elasticsearchSourceFactory(new ElasticsearchSourceFactory())
                .influxDbSourceFactory(new InfluxDbSourceFactory())
                .hdfsFileSourceFactory(new HdfsFileSourceFactory())
                .jdbcSourceFactory(jdbcSourceFactory)
                .kafkaSinkFactory(kafkaSinkFactory)
                .jdbcSinkFactory(jdbcSinkFactory)
                .elasticsearchSinkFactory(new ElasticsearchSinkFactory())
                .influxDbSinkFactory(new InfluxDbSinkFactory())
                .hdfsFileSinkFactory(new HdfsFileSinkFactory())
                .transformFactory(transformFactory)
                .build(), testMode, executionMode);
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
