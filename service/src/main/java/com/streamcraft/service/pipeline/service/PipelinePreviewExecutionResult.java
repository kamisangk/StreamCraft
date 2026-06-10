package com.streamcraft.service.pipeline.service;

import java.util.List;

public record PipelinePreviewExecutionResult(List<Output> outputs) {

    public record Output(String nodeId, List<String> records) {
    }
}
