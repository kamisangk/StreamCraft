package com.streamcraft.service.pipeline.client;

public record SubmitFlinkJobRequest(
        String clusterBaseUrl,
        String pipelineDefinitionUrl,
        Boolean testMode,
        Integer parallelism) {
}
