package com.streamcraft.shared.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RuntimePipelineValidationSupportTest {

    private static final Set<String> SUPPORTED_TRANSFORM_OPERATORS = Set.of("FILTER", "EVAL");

    @Test
    void rejectsUnsupportedRuntimeOperators() {
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor node =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "transform-1",
                        RuntimePipelineValidationSupport.TRANSFORM_TYPE,
                        "JSON_PARSER");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimePipelineValidationSupport.validateRuntimeNode(node, SUPPORTED_TRANSFORM_OPERATORS));

        assertTrue(exception.getMessage().contains("unsupported operators"));
    }

    @Test
    void rejectsUnsupportedRuntimePorts() {
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor sourceNode =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "source-1",
                        RuntimePipelineValidationSupport.SOURCE_TYPE,
                        RuntimePipelineValidationSupport.KAFKA_SOURCE_OPERATOR);
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor targetNode =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "sink-1",
                        RuntimePipelineValidationSupport.SINK_TYPE,
                        RuntimePipelineValidationSupport.KAFKA_SINK_OPERATOR);
        RuntimePipelineValidationSupport.RuntimeEdgeDescriptor edge =
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor(
                        "edge-1",
                        "source-1",
                        "output-1",
                        "sink-1",
                        "input-0");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimePipelineValidationSupport.validateRuntimeEdge(
                        edge, Set.of("source-1", "sink-1"), sourceNode, targetNode));

        assertTrue(exception.getMessage().contains("source operator KAFKA_SOURCE"));
        assertTrue(exception.getMessage().contains("Allowed source ports"));
    }

    @Test
    void acceptsFilterTrueAndFalsePorts() {
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor sourceNode =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "filter-1",
                        RuntimePipelineValidationSupport.TRANSFORM_TYPE,
                        RuntimePipelineValidationSupport.FILTER_OPERATOR);
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor targetNode =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "sink-1",
                        RuntimePipelineValidationSupport.SINK_TYPE,
                        RuntimePipelineValidationSupport.KAFKA_SINK_OPERATOR);
        RuntimePipelineValidationSupport.RuntimeEdgeDescriptor trueEdge =
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor("edge-1", "filter-1", "true", "sink-1", "input-0");
        RuntimePipelineValidationSupport.RuntimeEdgeDescriptor falseEdge =
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor("edge-2", "filter-1", "false", "sink-1", "input-0");

        assertDoesNotThrow(() -> RuntimePipelineValidationSupport.validateRuntimePorts(trueEdge, sourceNode, targetNode));
        assertDoesNotThrow(() -> RuntimePipelineValidationSupport.validateRuntimePorts(falseEdge, sourceNode, targetNode));
    }

    @Test
    void rejectsTrueAndFalsePortsForNonFilterSources() {
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor sourceNode =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "source-1",
                        RuntimePipelineValidationSupport.SOURCE_TYPE,
                        RuntimePipelineValidationSupport.KAFKA_SOURCE_OPERATOR);
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor targetNode =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "sink-1",
                        RuntimePipelineValidationSupport.SINK_TYPE,
                        RuntimePipelineValidationSupport.KAFKA_SINK_OPERATOR);
        RuntimePipelineValidationSupport.RuntimeEdgeDescriptor trueEdge =
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor("edge-1", "source-1", "true", "sink-1", "input-0");
        RuntimePipelineValidationSupport.RuntimeEdgeDescriptor falseEdge =
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor("edge-2", "source-1", "false", "sink-1", "input-0");

        IllegalArgumentException trueException = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimePipelineValidationSupport.validateRuntimePorts(trueEdge, sourceNode, targetNode));
        IllegalArgumentException falseException = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimePipelineValidationSupport.validateRuntimePorts(falseEdge, sourceNode, targetNode));

        assertTrue(trueException.getMessage().contains("output-0"));
        assertTrue(falseException.getMessage().contains("output-0"));
    }

    @Test
    void rejectsNonDefaultTargetPorts() {
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor sourceNode =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "filter-1",
                        RuntimePipelineValidationSupport.TRANSFORM_TYPE,
                        RuntimePipelineValidationSupport.FILTER_OPERATOR);
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor targetNode =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "sink-1",
                        RuntimePipelineValidationSupport.SINK_TYPE,
                        RuntimePipelineValidationSupport.KAFKA_SINK_OPERATOR);
        RuntimePipelineValidationSupport.RuntimeEdgeDescriptor edge =
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor("edge-1", "filter-1", "true", "sink-1", "input-1");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimePipelineValidationSupport.validateRuntimePorts(edge, sourceNode, targetNode));

        assertTrue(exception.getMessage().contains("unsupported target port"));
        assertTrue(exception.getMessage().contains("input-0"));
    }

    @Test
    void rejectsMissingSourcePort() {
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor sourceNode =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "source-1",
                        RuntimePipelineValidationSupport.SOURCE_TYPE,
                        RuntimePipelineValidationSupport.KAFKA_SOURCE_OPERATOR);
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor targetNode =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "sink-1",
                        RuntimePipelineValidationSupport.SINK_TYPE,
                        RuntimePipelineValidationSupport.KAFKA_SINK_OPERATOR);
        RuntimePipelineValidationSupport.RuntimeEdgeDescriptor edge =
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor("edge-1", "source-1", null, "sink-1", "input-0");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimePipelineValidationSupport.validateRuntimePorts(edge, sourceNode, targetNode));

        assertTrue(exception.getMessage().contains("sourcePortId"));
    }

    @Test
    void rejectsMissingTargetPort() {
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor sourceNode =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "source-1",
                        RuntimePipelineValidationSupport.SOURCE_TYPE,
                        RuntimePipelineValidationSupport.KAFKA_SOURCE_OPERATOR);
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor targetNode =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "sink-1",
                        RuntimePipelineValidationSupport.SINK_TYPE,
                        RuntimePipelineValidationSupport.KAFKA_SINK_OPERATOR);
        RuntimePipelineValidationSupport.RuntimeEdgeDescriptor edge =
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor("edge-1", "source-1", "output-0", "sink-1", null);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimePipelineValidationSupport.validateRuntimePorts(edge, sourceNode, targetNode));

        assertTrue(exception.getMessage().contains("targetPortId"));
    }

    @Test
    void rejectsCyclesInRuntimeGraph() {
        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> edges = List.of(
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor("edge-1", "source-1", "output-0", "filter-1", "input-0"),
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor("edge-2", "filter-1", "true", "source-1", "input-0"));

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimePipelineValidationSupport.ensureAcyclic(Set.of("source-1", "filter-1"), edges));

        assertTrue(exception.getMessage().contains("acyclic"));
    }

    @Test
    void rejectsNodesOutsideExecutableSourceToSinkPaths() {
        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> edges = List.of(
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor("edge-1", "source-1", "output-0", "sink-1", "input-0"));

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> RuntimePipelineValidationSupport.ensureExecutablePaths(
                        Set.of("source-1"),
                        Set.of("sink-1"),
                        Set.of("source-1", "orphan-1", "sink-1"),
                        edges));

        assertTrue(exception.getMessage().contains("orphan-1"));
    }

    @Test
    void acceptsExecutableDagWithCompatibleOperators() {
        List<RuntimePipelineValidationSupport.RuntimeNodeDescriptor> nodes = List.of(
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "source-1",
                        RuntimePipelineValidationSupport.SOURCE_TYPE,
                        RuntimePipelineValidationSupport.KAFKA_SOURCE_OPERATOR),
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "filter-1",
                        RuntimePipelineValidationSupport.TRANSFORM_TYPE,
                        "FILTER"),
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "sink-1",
                        RuntimePipelineValidationSupport.SINK_TYPE,
                        RuntimePipelineValidationSupport.KAFKA_SINK_OPERATOR));
        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> edges = List.of(
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor("edge-1", "source-1", "output-0", "filter-1", "input-0"),
                new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor("edge-2", "filter-1", "true", "sink-1", "input-0"));

        assertDoesNotThrow(() -> {
            nodes.forEach(node -> RuntimePipelineValidationSupport.validateRuntimeNode(node, SUPPORTED_TRANSFORM_OPERATORS));
            RuntimePipelineValidationSupport.RuntimeNodeDescriptor sourceNode =
                    new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                            "source-1",
                            RuntimePipelineValidationSupport.SOURCE_TYPE,
                            RuntimePipelineValidationSupport.KAFKA_SOURCE_OPERATOR);
            RuntimePipelineValidationSupport.RuntimeNodeDescriptor filterNode =
                    new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                            "filter-1",
                            RuntimePipelineValidationSupport.TRANSFORM_TYPE,
                            RuntimePipelineValidationSupport.FILTER_OPERATOR);
            RuntimePipelineValidationSupport.RuntimeNodeDescriptor sinkNode =
                    new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                            "sink-1",
                            RuntimePipelineValidationSupport.SINK_TYPE,
                            RuntimePipelineValidationSupport.KAFKA_SINK_OPERATOR);
            RuntimePipelineValidationSupport.validateRuntimeEdge(
                    edges.get(0), Set.of("source-1", "filter-1", "sink-1"), sourceNode, filterNode);
            RuntimePipelineValidationSupport.validateRuntimeEdge(
                    edges.get(1), Set.of("source-1", "filter-1", "sink-1"), filterNode, sinkNode);
            RuntimePipelineValidationSupport.ensureNoOutgoingEdgesFromSinks(Set.of("sink-1"), edges);
            RuntimePipelineValidationSupport.ensureAcyclic(Set.of("source-1", "filter-1", "sink-1"), edges);
            RuntimePipelineValidationSupport.ensureExecutablePaths(
                    Set.of("source-1"),
                    Set.of("sink-1"),
                    Set.of("source-1", "filter-1", "sink-1"),
                    edges);
        });
    }

    @Test
    void acceptsJdbcSinkAsRuntimeSinkOperator() {
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor node =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "sink-1",
                        RuntimePipelineValidationSupport.SINK_TYPE,
                        RuntimePipelineValidationSupport.JDBC_SINK_OPERATOR);

        assertDoesNotThrow(
                () -> RuntimePipelineValidationSupport.validateRuntimeNode(node, SUPPORTED_TRANSFORM_OPERATORS));
    }

    @Test
    void acceptsElasticsearchSourceAsRuntimeSourceOperator() {
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor node =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "source-1",
                        RuntimePipelineValidationSupport.SOURCE_TYPE,
                        RuntimePipelineValidationSupport.ELASTICSEARCH_SOURCE_OPERATOR);

        assertDoesNotThrow(
                () -> RuntimePipelineValidationSupport.validateRuntimeNode(node, SUPPORTED_TRANSFORM_OPERATORS));
    }

    @Test
    void acceptsElasticsearchSinkAsRuntimeSinkOperator() {
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor node =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "sink-1",
                        RuntimePipelineValidationSupport.SINK_TYPE,
                        RuntimePipelineValidationSupport.ELASTICSEARCH_SINK_OPERATOR);

        assertDoesNotThrow(
                () -> RuntimePipelineValidationSupport.validateRuntimeNode(node, SUPPORTED_TRANSFORM_OPERATORS));
    }

    @Test
    void acceptsInfluxDbSourceAsRuntimeSourceOperator() {
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor node =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "source-1",
                        RuntimePipelineValidationSupport.SOURCE_TYPE,
                        RuntimePipelineValidationSupport.INFLUXDB_SOURCE_OPERATOR);

        assertDoesNotThrow(
                () -> RuntimePipelineValidationSupport.validateRuntimeNode(node, SUPPORTED_TRANSFORM_OPERATORS));
    }

    @Test
    void acceptsInfluxDbSinkAsRuntimeSinkOperator() {
        RuntimePipelineValidationSupport.RuntimeNodeDescriptor node =
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        "sink-1",
                        RuntimePipelineValidationSupport.SINK_TYPE,
                        RuntimePipelineValidationSupport.INFLUXDB_SINK_OPERATOR);

        assertDoesNotThrow(
                () -> RuntimePipelineValidationSupport.validateRuntimeNode(node, SUPPORTED_TRANSFORM_OPERATORS));
    }
}
