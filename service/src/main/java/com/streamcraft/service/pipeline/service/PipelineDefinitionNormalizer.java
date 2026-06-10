package com.streamcraft.service.pipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.config.UiMessageService;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PipelineDefinitionNormalizer {

    private final ObjectMapper objectMapper;
    private final UiMessageService messages;

    @Autowired
    public PipelineDefinitionNormalizer(ObjectMapper objectMapper, UiMessageService messages) {
        this.objectMapper = objectMapper;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
    }

    public PipelineDefinitionNormalizer(ObjectMapper objectMapper) {
        this(objectMapper, UiMessageService.englishFallback());
    }

    public String normalize(String definitionJson) {
        return normalizeTree(definitionJson).toString();
    }

    public JsonNode normalizeTree(String definitionJson) {
        return normalizeTree(parse(definitionJson));
    }

    public JsonNode normalizeTree(JsonNode definition) {
        return definition;
    }

    private JsonNode parse(String definitionJson) {
        try {
            return objectMapper.readTree(definitionJson);
        } catch (IOException ex) {
            throw new IllegalArgumentException(messages.get("pipeline.validation.definition.mustBeValidJson"), ex);
        }
    }
}

