package com.streamcraft.service.flink.service;

import com.streamcraft.service.flink.client.CorePreviewClient;
import com.streamcraft.service.flink.client.CoreSubmissionClient;
import com.streamcraft.service.flink.client.FlinkJobControlClient;
import com.streamcraft.service.flink.config.FlinkGatewayProperties;
import com.streamcraft.service.security.InternalAccessProperties;

@Deprecated
public class MergedFlinkJobGateway extends CompositeFlinkJobGateway {

    public MergedFlinkJobGateway(
            CoreSubmissionClient coreSubmissionClient,
            CorePreviewClient corePreviewClient,
            FlinkJobControlClient flinkJobControlClient,
            FlinkGatewayProperties flinkGatewayProperties,
            InternalAccessProperties internalAccessProperties) {
        super(
                coreSubmissionClient,
                corePreviewClient,
                flinkJobControlClient,
                flinkGatewayProperties,
                internalAccessProperties);
    }
}
