package com.streamcraft.core.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PipelineEdge(
        String id,
        String sourceNodeId,
        String sourcePortId,
        String targetNodeId,
        String targetPortId) {
}
