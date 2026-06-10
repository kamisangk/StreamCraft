package com.streamcraft.service.flink.client;

import com.streamcraft.service.pipeline.client.SubmitFlinkJobResponse;

public interface CoreSubmissionClient {

    SubmitFlinkJobResponse submit(CoreSubmitRequest request);
}
