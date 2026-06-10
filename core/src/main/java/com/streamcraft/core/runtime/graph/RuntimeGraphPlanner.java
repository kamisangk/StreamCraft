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
        Map<String, PipelineNode> nodeById = new LinkedHashMap<>();
        Map<NodePortKey, List<PipelineEdge>> outgoingByPort = new LinkedHashMap<>();
        Map<NodeInputKey, List<PipelineEdge>> incomingByPort = new LinkedHashMap<>();
        Map<String, Integer> indegree = new LinkedHashMap<>();
        Map<String, List<PipelineEdge>> outgoingByNode = new LinkedHashMap<>();

        for (PipelineNode node : definition.nodes()) {
            nodeById.put(node.id(), node);
            indegree.put(node.id(), 0);
            outgoingByNode.put(node.id(), new ArrayList<>());
        }

        for (PipelineEdge edge : definition.edges()) {
            requireKnownNode(edge, edge.sourceNodeId(), nodeById, "source");
            requireKnownNode(edge, edge.targetNodeId(), nodeById, "target");
            requirePort(edge.sourcePortId(), edge, "sourcePortId");
            requirePort(edge.targetPortId(), edge, "targetPortId");

            outgoingByPort.computeIfAbsent(
                    new NodePortKey(edge.sourceNodeId(), edge.sourcePortId()),
                    ignored -> new ArrayList<>())
                    .add(edge);
            incomingByPort.computeIfAbsent(
                    new NodeInputKey(edge.targetNodeId(), edge.targetPortId()),
                    ignored -> new ArrayList<>())
                    .add(edge);
            outgoingByNode.get(edge.sourceNodeId()).add(edge);
            indegree.put(edge.targetNodeId(), indegree.get(edge.targetNodeId()) + 1);
        }

        ArrayDeque<String> queue = new ArrayDeque<>();
        for (PipelineNode node : definition.nodes()) {
            if (indegree.getOrDefault(node.id(), 0) == 0) {
                queue.addLast(node.id());
            }
        }

        List<String> topologicalNodeIds = new ArrayList<>();
        while (!queue.isEmpty()) {
            String nodeId = queue.removeFirst();
            topologicalNodeIds.add(nodeId);
            for (PipelineEdge edge : outgoingByNode.getOrDefault(nodeId, List.of())) {
                int nextIndegree = indegree.get(edge.targetNodeId()) - 1;
                indegree.put(edge.targetNodeId(), nextIndegree);
                if (nextIndegree == 0) {
                    queue.addLast(edge.targetNodeId());
                }
            }
        }

        if (topologicalNodeIds.size() != nodeById.size()) {
            throw new IllegalArgumentException("Pipeline must be a DAG without cycles.");
        }

        return new Plan(
                Collections.unmodifiableMap(new LinkedHashMap<>(nodeById)),
                immutableEdgeMap(outgoingByPort),
                immutableInputMap(incomingByPort),
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
