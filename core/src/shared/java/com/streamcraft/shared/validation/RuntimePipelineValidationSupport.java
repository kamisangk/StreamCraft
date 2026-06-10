package com.streamcraft.shared.validation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class RuntimePipelineValidationSupport {

    public static final String SOURCE_TYPE = "SOURCE";
    public static final String TRANSFORM_TYPE = "TRANSFORM";
    public static final String SINK_TYPE = "SINK";
    public static final String KAFKA_SOURCE_OPERATOR = "KAFKA_SOURCE";
    public static final String JDBC_SOURCE_OPERATOR = "JDBC_SOURCE";
    public static final String ELASTICSEARCH_SOURCE_OPERATOR = "ELASTICSEARCH_SOURCE";
    public static final String INFLUXDB_SOURCE_OPERATOR = "INFLUXDB_SOURCE";
    public static final String HDFS_FILE_SOURCE_OPERATOR = "HDFS_FILE_SOURCE";
    public static final String KAFKA_SINK_OPERATOR = "KAFKA_SINK";
    public static final String JDBC_SINK_OPERATOR = "JDBC_SINK";
    public static final String ELASTICSEARCH_SINK_OPERATOR = "ELASTICSEARCH_SINK";
    public static final String INFLUXDB_SINK_OPERATOR = "INFLUXDB_SINK";
    public static final String HDFS_FILE_SINK_OPERATOR = "HDFS_FILE_SINK";
    public static final String FILTER_OPERATOR = "FILTER";
    public static final String ROUTE_OPERATOR = "ROUTE";
    public static final String STREAM_JOIN_OPERATOR = "STREAM_JOIN";
    public static final String DEFAULT_SOURCE_PORT = "output-0";
    public static final String DEFAULT_TARGET_PORT = "input-0";

    private static final Set<String> SUPPORTED_RUNTIME_TYPES = Set.of(SOURCE_TYPE, TRANSFORM_TYPE, SINK_TYPE);

    private RuntimePipelineValidationSupport() {
    }

    public static void validateRuntimeNode(RuntimeNodeDescriptor node, Set<String> supportedTransformOperators) {
        if (!SUPPORTED_RUNTIME_TYPES.contains(node.type())) {
            throw new IllegalArgumentException(
                    "Pipeline contains unsupported node types for runtime execution: " + node.type());
        }

        Set<String> supportedRuntimeOperators = supportedRuntimeOperators(supportedTransformOperators);
        if (!supportedRuntimeOperators.contains(node.operator())) {
            throw new IllegalArgumentException(
                    "Pipeline contains unsupported operators for runtime execution: " + node.operator());
        }

        switch (node.type()) {
            case SOURCE_TYPE -> requireCompatibleOperator(
                    node,
                    Set.of(KAFKA_SOURCE_OPERATOR, JDBC_SOURCE_OPERATOR, ELASTICSEARCH_SOURCE_OPERATOR,
                            INFLUXDB_SOURCE_OPERATOR, HDFS_FILE_SOURCE_OPERATOR));
            case SINK_TYPE -> requireCompatibleOperator(
                    node,
                    Set.of(KAFKA_SINK_OPERATOR, JDBC_SINK_OPERATOR, ELASTICSEARCH_SINK_OPERATOR,
                            INFLUXDB_SINK_OPERATOR, HDFS_FILE_SINK_OPERATOR));
            case TRANSFORM_TYPE -> requireCompatibleOperator(node, supportedTransformOperators);
            default -> throw new IllegalArgumentException(
                    "Pipeline contains unsupported node types for runtime execution: " + node.type());
        }
    }

    public static void validateEdgeEndpoints(RuntimeEdgeDescriptor edge, Set<String> knownNodeIds) {
        if (!knownNodeIds.contains(edge.sourceNodeId())) {
            throw new IllegalArgumentException(
                    "Pipeline edge " + edge.edgeIdOrDefault() + " references unknown source node: " + edge.sourceNodeId());
        }
        if (!knownNodeIds.contains(edge.targetNodeId())) {
            throw new IllegalArgumentException(
                    "Pipeline edge " + edge.edgeIdOrDefault() + " references unknown target node: " + edge.targetNodeId());
        }
    }

    public static void validateRuntimePorts(
            RuntimeEdgeDescriptor edge, RuntimeNodeDescriptor sourceNode, RuntimeNodeDescriptor targetNode) {
        edge.requireExplicitPorts();
        Set<String> allowedSourcePorts = allowedSourcePortsForOperator(sourceNode.operator());
        if (!allowedSourcePorts.contains("*") && !allowedSourcePorts.contains(edge.sourcePortId())) {
            throw new IllegalArgumentException(
                    "Pipeline edge "
                            + edge.edgeIdOrDefault()
                            + " uses unsupported source port "
                            + edge.sourcePortId()
                            + " for source operator "
                            + sourceNode.operator()
                            + ". Allowed source ports: "
                            + allowedSourcePorts
                            + ".");
        }

        Set<String> allowedTargetPorts = allowedTargetPortsForOperator(targetNode.operator());
        if (!allowedTargetPorts.contains(edge.targetPortId())) {
            throw new IllegalArgumentException(
                    "Pipeline edge "
                            + edge.edgeIdOrDefault()
                            + " uses unsupported target port "
                            + edge.targetPortId()
                            + " for target operator "
                            + targetNode.operator()
                            + ". Allowed target ports: "
                            + allowedTargetPorts
                            + ".");
        }
    }

    public static void validateRuntimeEdge(
            RuntimeEdgeDescriptor edge,
            Set<String> knownNodeIds,
            RuntimeNodeDescriptor sourceNode,
            RuntimeNodeDescriptor targetNode) {
        validateEdgeEndpoints(edge, knownNodeIds);
        validateRuntimePorts(edge, sourceNode, targetNode);
    }

    public static void ensureNoOutgoingEdgesFromSinks(Set<String> sinkNodeIds, List<RuntimeEdgeDescriptor> edges) {
        for (RuntimeEdgeDescriptor edge : edges) {
            if (sinkNodeIds.contains(edge.sourceNodeId())) {
                throw new IllegalArgumentException("Sink nodes cannot have outgoing edges.");
            }
        }
    }

    public static void ensureAcyclic(Set<String> nodeIds, List<RuntimeEdgeDescriptor> edges) {
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> adjacency = new HashMap<>();

        for (String nodeId : nodeIds) {
            indegree.put(nodeId, 0);
            adjacency.put(nodeId, new ArrayList<>());
        }

        for (RuntimeEdgeDescriptor edge : edges) {
            adjacency.get(edge.sourceNodeId()).add(edge.targetNodeId());
            indegree.put(edge.targetNodeId(), indegree.get(edge.targetNodeId()) + 1);
        }

        ArrayDeque<String> queue = new ArrayDeque<>();
        for (Map.Entry<String, Integer> entry : indegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.add(entry.getKey());
            }
        }

        int visited = 0;
        while (!queue.isEmpty()) {
            String nodeId = queue.removeFirst();
            visited++;
            for (String nextNodeId : adjacency.get(nodeId)) {
                int nextIndegree = indegree.get(nextNodeId) - 1;
                indegree.put(nextNodeId, nextIndegree);
                if (nextIndegree == 0) {
                    queue.add(nextNodeId);
                }
            }
        }

        if (visited != nodeIds.size()) {
            throw new IllegalArgumentException("Pipeline graph must be acyclic.");
        }
    }

    public static void ensureExecutablePaths(
            Set<String> sourceNodeIds,
            Set<String> sinkNodeIds,
            Set<String> allNodeIds,
            List<RuntimeEdgeDescriptor> edges) {
        Map<String, List<String>> adjacency = new HashMap<>();
        Map<String, List<String>> reverseAdjacency = new HashMap<>();

        for (String nodeId : allNodeIds) {
            adjacency.put(nodeId, new ArrayList<>());
            reverseAdjacency.put(nodeId, new ArrayList<>());
        }

        for (RuntimeEdgeDescriptor edge : edges) {
            adjacency.get(edge.sourceNodeId()).add(edge.targetNodeId());
            reverseAdjacency.get(edge.targetNodeId()).add(edge.sourceNodeId());
        }

        Set<String> reachableFromSources = traverse(sourceNodeIds, adjacency);
        Set<String> reachingSinks = traverse(sinkNodeIds, reverseAdjacency);
        List<String> nonExecutableNodeIds = allNodeIds.stream()
                .filter(nodeId -> !reachableFromSources.contains(nodeId) || !reachingSinks.contains(nodeId))
                .sorted()
                .toList();

        if (!nonExecutableNodeIds.isEmpty()) {
            throw new IllegalArgumentException(
                    "Pipeline must provide an executable source-to-sink path for every node. Non-executable nodes: "
                            + nonExecutableNodeIds);
        }
    }

    public static Set<String> supportedRuntimeOperators(Set<String> supportedTransformOperators) {
        Set<String> operators = new HashSet<>();
        operators.add(KAFKA_SOURCE_OPERATOR);
        operators.add(JDBC_SOURCE_OPERATOR);
        operators.add(ELASTICSEARCH_SOURCE_OPERATOR);
        operators.add(INFLUXDB_SOURCE_OPERATOR);
        operators.add(HDFS_FILE_SOURCE_OPERATOR);
        operators.add(KAFKA_SINK_OPERATOR);
        operators.add(JDBC_SINK_OPERATOR);
        operators.add(ELASTICSEARCH_SINK_OPERATOR);
        operators.add(INFLUXDB_SINK_OPERATOR);
        operators.add(HDFS_FILE_SINK_OPERATOR);
        operators.addAll(supportedTransformOperators);
        return Set.copyOf(operators);
    }

    public static void ensureRequiredInputPorts(
            Map<String, RuntimeNodeDescriptor> runtimeNodeById,
            List<RuntimeEdgeDescriptor> edges) {
        Map<String, Set<String>> incomingPortsByNodeId = new HashMap<>();
        for (RuntimeEdgeDescriptor edge : edges) {
            incomingPortsByNodeId
                    .computeIfAbsent(edge.targetNodeId(), ignored -> new HashSet<>())
                    .add(edge.targetPortId());
        }

        for (RuntimeNodeDescriptor node : runtimeNodeById.values()) {
            if (!STREAM_JOIN_OPERATOR.equals(node.operator())) {
                continue;
            }
            Set<String> incomingPorts = incomingPortsByNodeId.getOrDefault(node.id(), Set.of());
            if (!incomingPorts.contains("left") || !incomingPorts.contains("right")) {
                throw new IllegalArgumentException(
                        "Stream join node "
                                + node.id()
                                + " must have incoming edges on target ports [left, right].");
            }
        }
    }

    private static void requireCompatibleOperator(RuntimeNodeDescriptor node, Set<String> compatibleOperators) {
        if (!compatibleOperators.contains(node.operator())) {
            throw new IllegalArgumentException(
                    "Node "
                            + node.id()
                            + " has incompatible type/operator combination: "
                            + node.type()
                            + " cannot use "
                            + node.operator()
                            + ". Expected one of "
                            + compatibleOperators
                            + ".");
        }
    }

    private static Set<String> allowedSourcePortsForOperator(String sourceOperator) {
        if (ROUTE_OPERATOR.equals(sourceOperator)) {
            return Set.of("*");
        }
        if (FILTER_OPERATOR.equals(sourceOperator)) {
            return Set.of("true", "false");
        }
        if ("DATA_QUALITY".equals(sourceOperator)) {
            return Set.of(DEFAULT_SOURCE_PORT, "dirty");
        }
        return Set.of(DEFAULT_SOURCE_PORT);
    }

    private static Set<String> allowedTargetPortsForOperator(String targetOperator) {
        if (STREAM_JOIN_OPERATOR.equals(targetOperator)) {
            return Set.of("left", "right");
        }
        return Set.of(DEFAULT_TARGET_PORT);
    }

    private static Set<String> traverse(Set<String> startNodeIds, Map<String, List<String>> adjacency) {
        ArrayDeque<String> queue = new ArrayDeque<>(startNodeIds);
        Set<String> visited = new HashSet<>();
        while (!queue.isEmpty()) {
            String nodeId = queue.removeFirst();
            if (!visited.add(nodeId)) {
                continue;
            }
            for (String nextNodeId : adjacency.getOrDefault(nodeId, List.of())) {
                if (!visited.contains(nextNodeId)) {
                    queue.addLast(nextNodeId);
                }
            }
        }
        return visited;
    }

    public record RuntimeNodeDescriptor(String id, String type, String operator) {
    }

    public record RuntimeEdgeDescriptor(
            String id,
            String sourceNodeId,
            String sourcePortId,
            String targetNodeId,
            String targetPortId) {
        public void requireExplicitPorts() {
            if (!hasText(sourcePortId)) {
                throw new IllegalArgumentException(
                        "Pipeline edge " + edgeIdOrDefault() + " must declare sourcePortId.");
            }
            if (!hasText(targetPortId)) {
                throw new IllegalArgumentException(
                        "Pipeline edge " + edgeIdOrDefault() + " must declare targetPortId.");
            }
        }

        public String edgeIdOrDefault() {
            return hasText(id) ? id : "<unnamed>";
        }

        private static boolean hasText(String value) {
            return value != null && !value.isBlank();
        }
    }
}
