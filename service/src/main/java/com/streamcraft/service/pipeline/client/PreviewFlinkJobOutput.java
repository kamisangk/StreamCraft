package com.streamcraft.service.pipeline.client;

import java.util.List;

public record PreviewFlinkJobOutput(
        String nodeId,
        List<String> records) {
}
