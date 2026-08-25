package com.streamcraft.core.runtime.graph;

import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RuntimeGraphPlanner {

    public Plan plan(PipelineDefinition definition) {
        GraphIndexes graph = indexNodes(definition.nodes());
        indexEdges(definition.edges(), graph);
        List<String> topologicalNodeIds = topologicalSort(graph);
        return freezePlan(graph, topologicalNodeIds);
    }

    private GraphIndexes indexNodes(List<PipelineNode> nodes) {
        GraphIndexes graph = new GraphIndexes();
        for (PipelineNode node : nodes) {
            graph.nodeById.put(node.id(), node);
            graph.nodeIdsInDefinition.add(node.id());
            graph.indegree.put(node.id(), 0);
            graph.outgoingByNode.put(node.id(), new ArrayList<>());
        }
        return graph;
    }

    private void indexEdges(List<PipelineEdge> edges, GraphIndexes graph) {
        for (PipelineEdge edge : edges) {
            requireKnownNode(edge, edge.sourceNodeId(), graph.nodeById, "source");
            requireKnownNode(edge, edge.targetNodeId(), graph.nodeById, "target");
            requirePort(edge.sourcePortId(), edge, "sourcePortId");
            requirePort(edge.targetPortId(), edge, "targetPortId");

            graph.outgoingByPort.computeIfAbsent(
                    new NodePortKey(edge.sourceNodeId(), edge.sourcePortId()),
                    ignored -> new ArrayList<>())
                    .add(edge);
            graph.incomingByPort.computeIfAbsent(
                    new NodeInputKey(edge.targetNodeId(), edge.targetPortId()),
                    ignored -> new ArrayList<>())
                    .add(edge);
            graph.outgoingByNode.get(edge.sourceNodeId()).add(edge);
            graph.indegree.put(edge.targetNodeId(), graph.indegree.get(edge.targetNodeId()) + 1);
        }
    }

    private List<String> topologicalSort(GraphIndexes graph) {
        ArrayDeque<String> queue = new ArrayDeque<>();
        for (String nodeId : graph.nodeIdsInDefinition) {
            if (graph.indegree.getOrDefault(nodeId, 0) == 0) {
                queue.addLast(nodeId);
            }
        }

        List<String> topologicalNodeIds = new ArrayList<>();
        while (!queue.isEmpty()) {
            String nodeId = queue.removeFirst();
            topologicalNodeIds.add(nodeId);
            for (PipelineEdge edge : graph.outgoingByNode.getOrDefault(nodeId, List.of())) {
                int nextIndegree = graph.indegree.get(edge.targetNodeId()) - 1;
                graph.indegree.put(edge.targetNodeId(), nextIndegree);
                if (nextIndegree == 0) {
                    queue.addLast(edge.targetNodeId());
                }
            }
        }

        if (topologicalNodeIds.size() != graph.nodeById.size()) {
            throw new IllegalArgumentException("Pipeline must be a DAG without cycles.");
        }
        return topologicalNodeIds;
    }

    private Plan freezePlan(GraphIndexes graph, List<String> topologicalNodeIds) {
        return new Plan(
                Collections.unmodifiableMap(new LinkedHashMap<>(graph.nodeById)),
                immutableEdgeMap(graph.outgoingByPort),
                immutableInputMap(graph.incomingByPort),
                List.copyOf(topologicalNodeIds));
    }

    private void requireKnownNode(
            PipelineEdge edge,
            String nodeId,
            Map<String, PipelineNode> nodeById,
            String endpointRole) {
        if (!nodeById.containsKey(nodeId)) {
            throw new IllegalArgumentException(
                    "Pipeline edge "
                            + edgeId(edge)
                            + " references unknown "
                            + endpointRole
                            + " node: "
                            + nodeId);
        }
    }

    private Map<NodePortKey, List<PipelineEdge>> immutableEdgeMap(Map<NodePortKey, List<PipelineEdge>> source) {
        Map<NodePortKey, List<PipelineEdge>> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(key, List.copyOf(value)));
        return Collections.unmodifiableMap(copy);
    }

    private void requirePort(String portId, PipelineEdge edge, String fieldName) {
        if (portId == null || portId.isBlank()) {
            throw new IllegalArgumentException("Pipeline edge " + edgeId(edge) + " must declare " + fieldName + ".");
        }
    }

    private Map<NodeInputKey, List<PipelineEdge>> immutableInputMap(Map<NodeInputKey, List<PipelineEdge>> source) {
        Map<NodeInputKey, List<PipelineEdge>> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(key, List.copyOf(value)));
        return Collections.unmodifiableMap(copy);
    }

    private String edgeId(PipelineEdge edge) {
        return edge.id() == null || edge.id().isBlank() ? "<unnamed>" : edge.id();
    }

    private static final class GraphIndexes {

        private final Map<String, PipelineNode> nodeById = new LinkedHashMap<>();
        private final Map<NodePortKey, List<PipelineEdge>> outgoingByPort = new LinkedHashMap<>();
        private final Map<NodeInputKey, List<PipelineEdge>> incomingByPort = new LinkedHashMap<>();
        private final Map<String, Integer> indegree = new LinkedHashMap<>();
        private final Map<String, List<PipelineEdge>> outgoingByNode = new LinkedHashMap<>();
        private final List<String> nodeIdsInDefinition = new ArrayList<>();
    }

    public record Plan(
            Map<String, PipelineNode> nodeById,
            Map<NodePortKey, List<PipelineEdge>> outgoingByPort,
            Map<NodeInputKey, List<PipelineEdge>> incomingByPort,
            List<String> topologicalNodeIds) {

        public List<PipelineEdge> incomingEdges(NodeInputKey inputKey) {
            return incomingByPort.getOrDefault(inputKey, List.of());
        }
    }
}
