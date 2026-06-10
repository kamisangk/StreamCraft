package com.streamcraft.service.pipeline.web;

public record RunPipelineRequest(
        Integer parallelism,
        Boolean testMode) {
}
