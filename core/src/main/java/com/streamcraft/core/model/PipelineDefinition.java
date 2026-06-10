package com.streamcraft.core.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PipelineDefinition(
        String pipelineId,
        List<PipelineNode> nodes,
        List<PipelineEdge> edges) {
}
