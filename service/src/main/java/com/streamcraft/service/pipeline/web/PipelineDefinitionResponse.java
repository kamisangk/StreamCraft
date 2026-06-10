package com.streamcraft.service.pipeline.web;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;

public record PipelineDefinitionResponse(@JsonValue JsonNode definition) {

    public static PipelineDefinitionResponse from(JsonNode definition) {
        return new PipelineDefinitionResponse(definition);
    }
}
