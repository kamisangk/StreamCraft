package com.streamcraft.service.pipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.shared.validation.PipelineNodeConfigValidationSupport;
import com.streamcraft.shared.validation.RuntimePipelineValidationSupport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PipelineDefinitionValidator {

    private static final String SOURCE_TYPE = RuntimePipelineValidationSupport.SOURCE_TYPE;
    private static final String TRANSFORM_TYPE = RuntimePipelineValidationSupport.TRANSFORM_TYPE;
    private static final String SINK_TYPE = RuntimePipelineValidationSupport.SINK_TYPE;

    private final ObjectMapper objectMapper;
    private final UiMessageService messages;

    @Autowired
    public PipelineDefinitionValidator(ObjectMapper objectMapper, UiMessageService messages) {
        this.objectMapper = objectMapper;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
    }

    public PipelineDefinitionValidator(ObjectMapper objectMapper) {
        this(objectMapper, UiMessageService.englishFallback());
    }

    public void validateForSave(String definitionJson) {
        JsonNode root = parse(definitionJson);
        ArrayNode nodes = requiredArray(root, "nodes");
        ArrayNode edges = requiredArray(root, "edges");
        Map<String, JsonNode> nodeById = indexNodes(nodes);
        validateSaveTransformConfigs(nodeById);
        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> runtimeEdges = toRuntimeEdges(edges);
        Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById = toRuntimeNodes(nodeById);
        validateEdges(runtimeEdges, nodeById.keySet(), runtimeNodeById, true);
        RuntimePipelineValidationSupport.ensureRequiredInputPorts(runtimeNodeById, runtimeEdges);
    }

    public void validateForRun(String definitionJson) {
        JsonNode root = parse(definitionJson);
        ArrayNode nodes = requiredArray(root, "nodes");
        ArrayNode edges = requiredArray(root, "edges");

        if (nodes.isEmpty()) {
            throw fail("pipeline.validation.sourceSink.execution");
        }

        Map<String, JsonNode> nodeById = indexNodes(nodes);
        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> runtimeEdges = toRuntimeEdges(edges);
        Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById =
                toRuntimeNodes(nodeById);
        List<String> sourceNodeIds = new ArrayList<>();
        Set<String> sinkNodeIds = new HashSet<>();
        for (RuntimePipelineValidationSupport.RuntimeNodeDescriptor node : runtimeNodeById.values()) {
            validateRuntimeNode(node, nodeById.get(node.id()).path("config"));
            String nodeType = node.type();
            if (SOURCE_TYPE.equals(nodeType)) {
                sourceNodeIds.add(node.id());
                validateSource(node, nodeById.get(node.id()), false);
            }
            if (SINK_TYPE.equals(nodeType)) {
                sinkNodeIds.add(node.id());
                validateSink(node, nodeById.get(node.id()), false);
            }
        }

        if (sourceNodeIds.isEmpty() || sinkNodeIds.isEmpty()) {
            throw fail("pipeline.validation.sourceSink.execution");
        }
        validateEdges(runtimeEdges, nodeById.keySet(), runtimeNodeById, true);
        RuntimePipelineValidationSupport.ensureRequiredInputPorts(runtimeNodeById, runtimeEdges);
        RuntimePipelineValidationSupport.ensureAcyclic(nodeById.keySet(), runtimeEdges);
        RuntimePipelineValidationSupport.ensureExecutablePaths(
                new HashSet<>(sourceNodeIds),
                sinkNodeIds,
                nodeById.keySet(),
                runtimeEdges);
        RuntimePipelineValidationSupport.ensureNoOutgoingEdgesFromSinks(sinkNodeIds, runtimeEdges);
    }

    public void validateForPreview(String definitionJson) {
        JsonNode root = parse(definitionJson);
        ArrayNode nodes = requiredArray(root, "nodes");
        ArrayNode edges = requiredArray(root, "edges");

        if (nodes.isEmpty()) {
            throw fail("pipeline.validation.sourceSink.preview");
        }

        Map<String, JsonNode> nodeById = indexNodes(nodes);
        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> runtimeEdges = toRuntimeEdges(edges);
        Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById =
                toRuntimeNodes(nodeById);
        List<String> sourceNodeIds = new ArrayList<>();
        Set<String> sinkNodeIds = new HashSet<>();
        for (RuntimePipelineValidationSupport.RuntimeNodeDescriptor node : runtimeNodeById.values()) {
            validateRuntimeNode(node, nodeById.get(node.id()).path("config"));
            String nodeType = node.type();
            if (SOURCE_TYPE.equals(nodeType)) {
                sourceNodeIds.add(node.id());
                validateSource(node, nodeById.get(node.id()), true);
            }
            if (SINK_TYPE.equals(nodeType)) {
                sinkNodeIds.add(node.id());
                validateSink(node, nodeById.get(node.id()), true);
            }
        }

        if (sourceNodeIds.isEmpty() || sinkNodeIds.isEmpty()) {
            throw fail("pipeline.validation.sourceSink.preview");
        }

        validateEdges(runtimeEdges, nodeById.keySet(), runtimeNodeById, true);
        RuntimePipelineValidationSupport.ensureRequiredInputPorts(runtimeNodeById, runtimeEdges);
        RuntimePipelineValidationSupport.ensureAcyclic(nodeById.keySet(), runtimeEdges);
        RuntimePipelineValidationSupport.ensureExecutablePaths(
                new HashSet<>(sourceNodeIds),
                sinkNodeIds,
                nodeById.keySet(),
                runtimeEdges);
        RuntimePipelineValidationSupport.ensureNoOutgoingEdgesFromSinks(sinkNodeIds, runtimeEdges);
    }

    private JsonNode parse(String definitionJson) {
        try {
            return objectMapper.readTree(definitionJson);
        } catch (IOException ex) {
            throw fail("pipeline.validation.definition.invalidJson", ex);
        }
    }

    private ArrayNode requiredArray(JsonNode node, String fieldName) {
        JsonNode value = node.path(fieldName);
        if (!(value instanceof ArrayNode arrayNode)) {
            throw fail("pipeline.validation.definition.missingArray", fieldName);
        }
        return arrayNode;
    }

    private String requiredText(JsonNode node, String fieldName) {
        String value = node.path(fieldName).asText(null);
        if (value == null || value.isBlank()) {
            throw fail("pipeline.validation.definition.missingField", fieldName);
        }
        return value;
    }

    private void validateRuntimeNode(RuntimePipelineValidationSupport.RuntimeNodeDescriptor node, JsonNode config) {
        RuntimePipelineValidationSupport.validateRuntimeNode(
                node, RuntimePipelineValidationSupport.supportedTransformOperators());
        validateRuntimeTransformConfig(node.operator(), config);
    }

    private Map<String, JsonNode> indexNodes(ArrayNode nodes) {
        Map<String, JsonNode> nodeById = new HashMap<>();
        for (JsonNode node : nodes) {
            String nodeId = requiredText(node, "id");
            if (nodeById.putIfAbsent(nodeId, node) != null) {
                throw fail("pipeline.validation.definition.duplicateNode", nodeId);
            }
        }
        return nodeById;
    }

    private Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> toRuntimeNodes(
            Map<String, JsonNode> nodeById) {
        Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById = new HashMap<>();
        for (Map.Entry<String, JsonNode> entry : nodeById.entrySet()) {
            String nodeId = entry.getKey();
            JsonNode node = entry.getValue();
            runtimeNodeById.put(
                    nodeId,
                    new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                            nodeId,
                            requiredText(node, "type"),
                            requiredText(node, "operator")));
        }
        return runtimeNodeById;
    }

    private void validateRuntimeTransformConfig(String operator, JsonNode config) {
        if ("RENAME".equals(operator)) {
            return;
        }
        PipelineNodeConfigValidationSupport.validateTransformConfig(
                operator,
                config,
                error -> fail(error.messageKey(), error.args()));
    }

    private void validateSaveTransformConfigs(Map<String, JsonNode> nodeById) {
        for (JsonNode node : nodeById.values()) {
            if (TRANSFORM_TYPE.equals(node.path("type").asText(null))) {
                String operator = node.path("operator").asText(null);
                if ("AGGREGATE".equals(operator)
                        || "DEDUPLICATE".equals(operator)
                        || "LOOKUP_ENRICH".equals(operator)
                        || "LOOKUP_JOIN".equals(operator)
                        || "STREAM_JOIN".equals(operator)
                        || "FLATTEN".equals(operator)
                        || "EXPLODE".equals(operator)
                        || "DATA_QUALITY".equals(operator)
                        || "TIME_DERIVE".equals(operator)
                        || "MASK_HASH".equals(operator)
                        || "CASE_WHEN".equals(operator)
                        || "ROUTE".equals(operator)) {
                    validateRuntimeTransformConfig(operator, node.path("config"));
                }
            }
        }
    }

    private void validateSource(
            RuntimePipelineValidationSupport.RuntimeNodeDescriptor runtimeNode,
            JsonNode node,
            boolean preview) {
        if (preview) {
            PipelineNodeConfigValidationSupport.validatePreviewSource(
                    node.path("config"), objectMapper, this::toValidationException);
            return;
        }
        PipelineNodeConfigValidationSupport.validateSourceConfig(
                runtimeNode.operator(), node.path("config"), this::toValidationException);
    }

    private void validateSink(
            RuntimePipelineValidationSupport.RuntimeNodeDescriptor runtimeNode,
            JsonNode node,
            boolean preview) {
        if (preview) {
            PipelineNodeConfigValidationSupport.validatePreviewSink(
                    runtimeNode.operator(), node.path("config"), this::toValidationException);
            return;
        }
        PipelineNodeConfigValidationSupport.validateSinkConfig(
                runtimeNode.operator(), node.path("config"), this::toValidationException);
    }

    private IllegalArgumentException toValidationException(
            PipelineNodeConfigValidationSupport.ValidationError error) {
        if (error.cause() == null) {
            return fail(error.messageKey(), error.args());
        }
        return fail(error.messageKey(), error.cause(), error.args());
    }

    private IllegalArgumentException fail(String key, Object... args) {
        return new IllegalArgumentException(messages.get(key, args));
    }

    private IllegalArgumentException fail(String key, Throwable cause, Object... args) {
        return new IllegalArgumentException(messages.get(key, args), cause);
    }

    private void validateEdges(
            List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> edges,
            Set<String> nodeIds,
            Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById,
            boolean enforceRuntimePorts) {
        for (RuntimePipelineValidationSupport.RuntimeEdgeDescriptor edge : edges) {
            RuntimePipelineValidationSupport.validateEdgeEndpoints(edge, nodeIds);
            if (enforceRuntimePorts) {
                RuntimePipelineValidationSupport.validateRuntimePorts(
                        edge, runtimeNodeById.get(edge.sourceNodeId()), runtimeNodeById.get(edge.targetNodeId()));
            }
        }
    }

    private List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> toRuntimeEdges(ArrayNode edges) {
        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> runtimeEdges = new ArrayList<>();
        for (JsonNode edge : edges) {
            runtimeEdges.add(new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor(
                    edge.path("id").asText(null),
                    requiredText(edge, "sourceNodeId"),
                    requiredText(edge, "sourcePortId"),
                    requiredText(edge, "targetNodeId"),
                    requiredText(edge, "targetPortId")));
        }
        return runtimeEdges;
    }
}

