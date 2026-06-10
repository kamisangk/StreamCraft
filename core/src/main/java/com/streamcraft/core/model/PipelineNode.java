package com.streamcraft.core.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PipelineNode(
        String id,
        String name,
        PipelineNodeType type,
        PipelineOperator operator,
        JsonNode config) {
}
