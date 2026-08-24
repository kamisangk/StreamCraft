package com.streamcraft.service.pipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.persistence.PipelineRepository;
import com.streamcraft.service.pipeline.web.PipelinePreviewOutputResponse;
import com.streamcraft.service.pipeline.web.PipelinePreviewResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PipelineDefinitionService {

    private final PipelineRepository repository;
    private final ObjectMapper objectMapper;
    private final PipelineDefinitionValidator validator;
    private final PipelineDefinitionNormalizer definitionNormalizer;
    private final UiMessageService messages;

    @Autowired
    public PipelineDefinitionService(
            PipelineRepository repository,
            ObjectMapper objectMapper,
            PipelineDefinitionValidator validator,
            PipelineDefinitionNormalizer definitionNormalizer,
            UiMessageService messages) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.definitionNormalizer = definitionNormalizer;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
    }

    public PipelineDefinitionService(
            PipelineRepository repository,
            ObjectMapper objectMapper,
            PipelineDefinitionValidator validator,
            PipelineDefinitionNormalizer definitionNormalizer) {
        this(repository, objectMapper, validator, definitionNormalizer, UiMessageService.englishFallback());
    }

    public String normalizeAndValidateForSave(String definitionJson) {
        String normalizedDefinition = definitionNormalizer.normalize(definitionJson);
        validator.validateForSave(normalizedDefinition);
        return normalizedDefinition;
    }

    public String normalizeAndValidateForPreview(String definitionJson) {
        String normalizedDefinition = definitionNormalizer.normalize(definitionJson);
        validator.validateForPreview(normalizedDefinition);
        return normalizedDefinition;
    }

    public void validateForRun(String definitionJson) {
        validator.validateForRun(definitionJson);
    }

    @Transactional(readOnly = true)
    public JsonNode getDefinition(Long id) {
        Pipeline pipeline = repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException(messages.get("pipeline.error.notFound")));
        try {
            JsonNode definition = objectMapper.readTree(pipeline.getDefinitionJson());
            return definitionNormalizer.normalizeTree(definition);
        } catch (IOException ex) {
            throw new IllegalStateException(messages.get("pipeline.error.storedDefinitionInvalidJson"), ex);
        }
    }

    PipelinePreviewResponse toPreviewResponse(
            String normalizedDefinition,
            PipelinePreviewExecutionResult result) {
        Map<String, String> titleByNodeId = indexPreviewTitles(normalizedDefinition);
        return new PipelinePreviewResponse(result.outputs().stream()
                .map(output -> new PipelinePreviewOutputResponse(
                        output.nodeId(),
                        titleByNodeId.getOrDefault(output.nodeId(), output.nodeId()),
                        output.records()))
                .toList());
    }

    DefinitionNodes parseDefinitionNodes(String definitionJson) throws IOException {
        JsonNode definition = objectMapper.readTree(definitionJson);
        JsonNode nodes = definition.path("nodes");

        List<String> nodeIds = new ArrayList<>();
        Map<String, String> nodeNames = new HashMap<>();

        for (JsonNode node : nodes) {
            String nodeId = node.path("id").asText();
            String nodeName = node.path("name").asText();
            nodeIds.add(nodeId);
            nodeNames.put(nodeId, nodeName);
        }

        return new DefinitionNodes(nodeIds, nodeNames);
    }

    private Map<String, String> indexPreviewTitles(String definitionJson) {
        try {
            JsonNode root = objectMapper.readTree(definitionJson);
            Map<String, String> titles = new HashMap<>();
            for (JsonNode node : root.path("nodes")) {
                String nodeId = node.path("id").asText();
                String displayName = node.path("displayName").asText("");
                String fallbackName = node.path("name").asText(nodeId);
                titles.put(nodeId, displayName == null || displayName.isBlank() ? fallbackName : displayName);
            }
            return titles;
        } catch (IOException exception) {
            throw new IllegalStateException(messages.get("pipeline.error.mapPreviewTitlesFailed"), exception);
        }
    }

    record DefinitionNodes(List<String> nodeIds, Map<String, String> nodeNames) {
    }
}
