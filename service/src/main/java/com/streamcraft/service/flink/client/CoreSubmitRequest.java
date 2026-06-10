package com.streamcraft.service.flink.client;

public record CoreSubmitRequest(
        String clusterBaseUrl,
        String coreJarPath,
        String pipelineDefinitionUrl,
        boolean testMode,
        Integer parallelism,
        String definitionAuthToken) {
}
