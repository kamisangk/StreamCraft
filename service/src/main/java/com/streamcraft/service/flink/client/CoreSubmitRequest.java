package com.streamcraft.service.flink.client;

public record CoreSubmitRequest(
        String clusterBaseUrl,
        String coreJarPath,
        String pipelineDefinitionUrl,
        boolean testMode,
        Integer parallelism,
        String definitionAuthToken,
        String jobId,
        String deploymentNamespace,
        String checkpointStorage,
        Long checkpointIntervalMs,
        Long checkpointTimeoutMs,
        Integer restartAttempts,
        Long restartDelayMs) {

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
                null,
                null,
                null,
                null,
                null,
                null);
    }

    public CoreSubmitRequest(
            String clusterBaseUrl,
            String coreJarPath,
            String pipelineDefinitionUrl,
            boolean testMode,
            Integer parallelism,
            String definitionAuthToken,
            String jobId,
            String deploymentNamespace) {
        this(
                clusterBaseUrl,
                coreJarPath,
                pipelineDefinitionUrl,
                testMode,
                parallelism,
                definitionAuthToken,
                jobId,
                deploymentNamespace,
                null,
                null,
                null,
                null,
                null);
    }
}
