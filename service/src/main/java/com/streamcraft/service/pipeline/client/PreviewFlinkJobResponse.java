package com.streamcraft.service.pipeline.client;

import java.util.List;

public record PreviewFlinkJobResponse(List<PreviewFlinkJobOutput> outputs) {
}
