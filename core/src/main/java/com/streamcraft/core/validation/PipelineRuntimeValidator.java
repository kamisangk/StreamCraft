package com.streamcraft.core.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import com.streamcraft.core.runtime.ExecutionMode;
import com.streamcraft.core.runtime.transform.TransformOperatorFactory;
import com.streamcraft.shared.validation.PipelineNodeConfigValidationSupport;
import com.streamcraft.shared.validation.RuntimePipelineValidationSupport;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PipelineRuntimeValidator {

    private static final Set<PipelineOperator> SUPPORTED_TRANSFORM_OPERATORS =
            TransformOperatorFactory.supportedOperators();
    private static final Set<String> SUPPORTED_TRANSFORM_OPERATOR_NAMES = supportedTransformOperatorNames();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static Set<String> supportedTransformOperatorNames() {
        Set<String> names = SUPPORTED_TRANSFORM_OPERATORS.stream()
                .map(Enum::name)
                .collect(Collectors.toCollection(HashSet::new));
        names.add(PipelineOperator.AGGREGATE.name());
        return Set.copyOf(names);
    }

    public void validate(PipelineDefinition definition) {
        validate(definition, ExecutionMode.RUN);
    }

    public void validate(PipelineDefinition definition, ExecutionMode executionMode) {
        if (definition == null) {
            throw new IllegalArgumentException("Pipeline definition is required.");
        }
        if (definition.nodes() == null || definition.nodes().isEmpty()) {
            throw new IllegalArgumentException("Pipeline must contain at least one node.");
        }

        ExecutionMode normalizedExecutionMode = executionMode == null ? ExecutionMode.RUN : executionMode;
        Map<String, PipelineNode> nodeById = new HashMap<>();
        Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById = new HashMap<>();
        Set<String> sourceIds = new HashSet<>();
        Set<String> sinkIds = new HashSet<>();

        for (PipelineNode node : definition.nodes()) {
            requireText(node.id(), "Node id is required.");
            if (nodeById.putIfAbsent(node.id(), node) != null) {
                throw new IllegalArgumentException("Duplicate node id is not allowed: " + node.id());
            }

            validateSupportedNode(node);
            runtimeNodeById.put(
                    node.id(),
                    new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                            node.id(),
                            node.type().name(),
                            node.operator().name()));

            if (node.type() == PipelineNodeType.SOURCE) {
                sourceIds.add(node.id());
                validateSource(node, normalizedExecutionMode);
            } else if (node.type() == PipelineNodeType.SINK) {
                sinkIds.add(node.id());
                validateSink(node, normalizedExecutionMode);
            } else if (node.type() == PipelineNodeType.TRANSFORM) {
                validateTransformNode(node);
            }
        }

        if (sourceIds.isEmpty() || sinkIds.isEmpty()) {
            throw new IllegalArgumentException("Pipeline must contain at least one Source and one Sink.");
        }
        if (definition.edges() == null || definition.edges().isEmpty()) {
            throw new IllegalArgumentException("Pipeline must contain at least one edge.");
        }

        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> runtimeEdges = definition.edges().stream()
                .map(this::toRuntimeEdge)
                .toList();
        validateEdges(runtimeEdges, nodeById.keySet(), runtimeNodeById);
        RuntimePipelineValidationSupport.ensureRequiredInputPorts(runtimeNodeById, runtimeEdges);
        RuntimePipelineValidationSupport.ensureNoOutgoingEdgesFromSinks(sinkIds, runtimeEdges);
        RuntimePipelineValidationSupport.ensureAcyclic(nodeById.keySet(), runtimeEdges);
        RuntimePipelineValidationSupport.ensureExecutablePaths(sourceIds, sinkIds, nodeById.keySet(), runtimeEdges);
    }

    private void validateSupportedNode(PipelineNode node) {
        PipelineNodeType nodeType = node.type();
        if (nodeType == null || nodeType == PipelineNodeType.UNKNOWN) {
            throw new IllegalArgumentException("Node " + node.id() + " must declare a supported type.");
        }

        PipelineOperator operator = node.operator();
        if (operator == null || operator == PipelineOperator.UNKNOWN) {
            throw new IllegalArgumentException("Node " + node.id() + " must declare a supported operator.");
        }

        RuntimePipelineValidationSupport.validateRuntimeNode(
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        node.id(),
                        nodeType.name(),
                        operator.name()),
                SUPPORTED_TRANSFORM_OPERATOR_NAMES);
    }

    private void validateSource(PipelineNode node, ExecutionMode executionMode) {
        if (executionMode.forceMockSources()) {
            PipelineNodeConfigValidationSupport.validatePreviewSource(
                    node.config(), OBJECT_MAPPER, this::toValidationException);
            return;
        }
        PipelineNodeConfigValidationSupport.validateSourceConfig(
                node.operator().name(), node.config(), this::toValidationException);
    }

    private void validateSink(PipelineNode node, ExecutionMode executionMode) {
        if (executionMode.interceptSinks()) {
            PipelineNodeConfigValidationSupport.validatePreviewSink(
                    node.operator().name(),
                    node.config(),
                    this::toValidationException);
            return;
        }
        PipelineNodeConfigValidationSupport.validateSinkConfig(
                node.operator().name(), node.config(), this::toValidationException);
    }

    private void validateTransformNode(PipelineNode node) {
        PipelineNodeConfigValidationSupport.validateTransformConfig(
                node.operator().name(),
                node.config(),
                this::toValidationException);
    }

    private void validateEdges(
            List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> edges,
            Set<String> nodeIds,
            Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById) {
        for (RuntimePipelineValidationSupport.RuntimeEdgeDescriptor edge : edges) {
            RuntimePipelineValidationSupport.validateEdgeEndpoints(edge, nodeIds);
            RuntimePipelineValidationSupport.validateRuntimePorts(
                    edge,
                    runtimeNodeById.get(edge.sourceNodeId()),
                    runtimeNodeById.get(edge.targetNodeId()));
        }
    }

    private void requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
    }

    private IllegalArgumentException toValidationException(
            PipelineNodeConfigValidationSupport.ValidationError error) {
        if (error.cause() == null) {
            return new IllegalArgumentException(error.defaultMessage());
        }
        return new IllegalArgumentException(error.defaultMessage(), error.cause());
    }

    private RuntimePipelineValidationSupport.RuntimeEdgeDescriptor toRuntimeEdge(PipelineEdge edge) {
        requireText(edge.sourceNodeId(), "Edge sourceNodeId is required.");
        requireText(edge.targetNodeId(), "Edge targetNodeId is required.");
        return new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor(
                edge.id(),
                edge.sourceNodeId(),
                edge.sourcePortId(),
                edge.targetNodeId(),
                edge.targetPortId());
    }
}
