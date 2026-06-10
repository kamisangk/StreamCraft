package com.streamcraft.service.pipeline.web;

import java.util.List;

public record PipelinePreviewResponse(List<PipelinePreviewOutputResponse> outputs) {
}
