package com.streamcraft.service.pipeline.client;

public record PreviewFlinkJobRequest(
        String definitionJson,
        Integer parallelism) {
}
