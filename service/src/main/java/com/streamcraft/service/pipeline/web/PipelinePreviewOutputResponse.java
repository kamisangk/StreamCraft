package com.streamcraft.service.pipeline.web;

import java.util.List;

public record PipelinePreviewOutputResponse(
        String nodeId,
        String nodeName,
        List<String> records) {
}
