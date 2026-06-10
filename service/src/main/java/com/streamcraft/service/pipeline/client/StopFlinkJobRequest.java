package com.streamcraft.service.pipeline.client;

public record StopFlinkJobRequest(
        String clusterBaseUrl,
        String jobId) {
}
