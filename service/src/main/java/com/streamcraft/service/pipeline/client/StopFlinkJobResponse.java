package com.streamcraft.service.pipeline.client;

public record StopFlinkJobResponse(String jobId, String requestId, String message) {
}
