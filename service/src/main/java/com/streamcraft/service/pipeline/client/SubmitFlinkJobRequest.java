package com.streamcraft.service.pipeline.client;

public record SubmitFlinkJobRequest(
        String clusterBaseUrl,
        String pipelineDefinitionUrl,
        Boolean testMode,
        Integer parallelism,
        String jobId,
        String deploymentNamespace) {

    public SubmitFlinkJobRequest(
            String clusterBaseUrl,
            String pipelineDefinitionUrl,
            Boolean testMode,
            Integer parallelism) {
        this(clusterBaseUrl, pipelineDefinitionUrl, testMode, parallelism, null, null);
    }
}
