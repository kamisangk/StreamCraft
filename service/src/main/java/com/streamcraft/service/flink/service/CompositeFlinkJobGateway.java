package com.streamcraft.service.flink.service;

import com.streamcraft.service.flink.FlinkParallelismResolver;
import com.streamcraft.service.flink.client.CorePreviewClient;
import com.streamcraft.service.flink.client.CorePreviewRequest;
import com.streamcraft.service.flink.client.CoreSubmissionClient;
import com.streamcraft.service.flink.client.CoreSubmitRequest;
import com.streamcraft.service.flink.client.FlinkJobControlClient;
import com.streamcraft.service.flink.config.FlinkGatewayProperties;
import com.streamcraft.service.pipeline.client.FlinkJobGateway;
import com.streamcraft.service.pipeline.client.PreviewFlinkJobRequest;
import com.streamcraft.service.pipeline.client.PreviewFlinkJobResponse;
import com.streamcraft.service.pipeline.client.StopFlinkJobRequest;
import com.streamcraft.service.pipeline.client.StopFlinkJobResponse;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobRequest;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobResponse;
import com.streamcraft.service.security.InternalAccessProperties;

public class CompositeFlinkJobGateway implements FlinkJobGateway {

    private final CoreSubmissionClient coreSubmissionClient;
    private final CorePreviewClient corePreviewClient;
    private final FlinkJobControlClient flinkJobControlClient;
    private final FlinkGatewayProperties flinkGatewayProperties;
    private final InternalAccessProperties internalAccessProperties;

    public CompositeFlinkJobGateway(
            CoreSubmissionClient coreSubmissionClient,
            CorePreviewClient corePreviewClient,
            FlinkJobControlClient flinkJobControlClient,
            FlinkGatewayProperties flinkGatewayProperties,
            InternalAccessProperties internalAccessProperties) {
        this.coreSubmissionClient = coreSubmissionClient;
        this.corePreviewClient = corePreviewClient;
        this.flinkJobControlClient = flinkJobControlClient;
        this.flinkGatewayProperties = flinkGatewayProperties;
        this.internalAccessProperties = internalAccessProperties;
    }

    @Override
    public SubmitFlinkJobResponse submit(SubmitFlinkJobRequest request) {
        int parallelism = FlinkParallelismResolver.resolve(request.parallelism());
        return coreSubmissionClient.submit(new CoreSubmitRequest(
                request.clusterBaseUrl(),
                flinkGatewayProperties.getCoreJarPath(),
                request.pipelineDefinitionUrl(),
                Boolean.TRUE.equals(request.testMode()),
                parallelism,
                internalAccessProperties.token()));
    }

    @Override
    public PreviewFlinkJobResponse preview(PreviewFlinkJobRequest request) {
        int parallelism = FlinkParallelismResolver.resolve(request.parallelism());
        return corePreviewClient.preview(new CorePreviewRequest(
                flinkGatewayProperties.getCoreJarPath(),
                request.definitionJson(),
                parallelism));
    }

    @Override
    public StopFlinkJobResponse stop(StopFlinkJobRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Stop request is required.");
        }
        if (request.jobId() == null || request.jobId().isBlank()) {
            throw new IllegalArgumentException("Job id is required.");
        }
        return flinkJobControlClient.stopJob(request.clusterBaseUrl(), request.jobId());
    }
}
