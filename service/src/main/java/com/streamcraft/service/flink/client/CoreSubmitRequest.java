package com.streamcraft.service.flink.client;

public record CoreSubmitRequest(
        String clusterBaseUrl,
        String coreJarPath,
        String pipelineDefinitionUrl,
        boolean testMode,
        Integer parallelism,
        String definitionAuthToken,
        String jobId,
        String deploymentNamespace) {

    public CoreSubmitRequest(
            String clusterBaseUrl,
            String coreJarPath,
            String pipelineDefinitionUrl,
            boolean testMode,
            Integer parallelism,
            String definitionAuthToken) {
        this(
                clusterBaseUrl,
                coreJarPath,
                pipelineDefinitionUrl,
                testMode,
                parallelism,
                definitionAuthToken,
                null,
                null);
    }
}
