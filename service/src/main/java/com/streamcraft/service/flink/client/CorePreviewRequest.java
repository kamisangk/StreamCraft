package com.streamcraft.service.flink.client;

public record CorePreviewRequest(
        String coreJarPath,
        String definitionJson,
        Integer parallelism) {
}
