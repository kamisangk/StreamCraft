package com.streamcraft.service.pipeline.web;

public record PipelinePreviewRequest(
        String name,
        String description,
        String definitionJson) {
}
