package com.streamcraft.service.pipeline.client;

public interface FlinkJobGateway {

    SubmitFlinkJobResponse submit(SubmitFlinkJobRequest request);

    PreviewFlinkJobResponse preview(PreviewFlinkJobRequest request);

    StopFlinkJobResponse stop(StopFlinkJobRequest request);
}
