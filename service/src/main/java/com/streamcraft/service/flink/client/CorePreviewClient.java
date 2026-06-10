package com.streamcraft.service.flink.client;

import com.streamcraft.service.pipeline.client.PreviewFlinkJobResponse;

public interface CorePreviewClient {

    PreviewFlinkJobResponse preview(CorePreviewRequest request);
}
