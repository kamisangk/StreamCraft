package com.streamcraft.service.pipeline.service;

import com.streamcraft.service.config.PipelineRuntimeProperties;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.client.FlinkJobGateway;
import com.streamcraft.service.pipeline.client.PreviewFlinkJobRequest;
import com.streamcraft.service.pipeline.client.StopFlinkJobRequest;
import com.streamcraft.service.pipeline.client.StopFlinkJobResponse;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobRequest;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobResponse;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.persistence.PipelineRepository;
import com.streamcraft.service.pipeline.web.PipelinePreviewRequest;
import com.streamcraft.service.pipeline.web.PipelinePreviewResponse;
import com.streamcraft.service.pipeline.web.RunPipelineRequest;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PipelineExecutionService {

    private final PipelineRepository repository;
    private final PipelineDefinitionService definitionService;
    private final PipelineRuntimeStateSupport runtimeStateSupport;
    private final FlinkJobGateway flinkJobGateway;
    private final PipelineRuntimeProperties runtimeProperties;
    private final UiMessageService messages;

    @Autowired
    public PipelineExecutionService(
            PipelineRepository repository,
            PipelineDefinitionService definitionService,
            PipelineRuntimeStateSupport runtimeStateSupport,
            FlinkJobGateway flinkJobGateway,
            PipelineRuntimeProperties runtimeProperties,
            UiMessageService messages) {
        this.repository = repository;
        this.definitionService = definitionService;
        this.runtimeStateSupport = runtimeStateSupport;
        this.flinkJobGateway = flinkJobGateway;
        this.runtimeProperties = runtimeProperties;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
    }

    public PipelineExecutionService(
            PipelineRepository repository,
            PipelineDefinitionService definitionService,
            PipelineRuntimeStateSupport runtimeStateSupport,
            FlinkJobGateway flinkJobGateway,
            PipelineRuntimeProperties runtimeProperties) {
        this(
                repository,
                definitionService,
                runtimeStateSupport,
                flinkJobGateway,
                runtimeProperties,
                UiMessageService.englishFallback());
    }

    public PipelinePreviewResponse preview(PipelinePreviewRequest request) {
        String normalizedDefinition = definitionService.normalizeAndValidateForPreview(request.definitionJson());
        try {
            PipelinePreviewExecutionResult result = toPreviewExecutionResult(
                    flinkJobGateway.preview(new PreviewFlinkJobRequest(
                            normalizedDefinition,
                            1)));
            return definitionService.toPreviewResponse(normalizedDefinition, result);
        } catch (Exception exception) {
            throw new IllegalStateException(messages.get("pipeline.error.previewExecutionFailed"), exception);
        }
    }

    public Pipeline run(Long id, RunPipelineRequest request) {
        Pipeline pipeline = getStored(id);
        definitionService.validateForRun(pipeline.getDefinitionJson());
        FlinkRuntimeTarget runtimeTarget = runtimeStateSupport.requireRuntimeTarget();

        SubmitFlinkJobResponse response = flinkJobGateway.submit(
                buildSubmitRequest(id, request, runtimeTarget));

        pipeline.setLastRunStatus(PipelineRunStatus.RUNNING);
        pipeline.setLastRunMessage(response.message());
        pipeline.setLastJobId(response.jobId());
        pipeline.setLastSubmittedAt(Instant.now());
        return repository.save(pipeline);
    }

    public Pipeline stop(Long id) {
        Pipeline pipeline = getStored(id);
        if (!hasText(pipeline.getLastJobId())) {
            throw new IllegalArgumentException(messages.get("pipeline.error.runningJobIdMissing"));
        }
        FlinkRuntimeTarget runtimeTarget = runtimeStateSupport.requireRuntimeTarget();

        PipelineRunStatus runtimeStatus = runtimeStateSupport.resolveRuntimeStatus(pipeline, runtimeTarget);
        if (runtimeStatus != PipelineRunStatus.RUNNING) {
            return runtimeStateSupport.copyWithRunStatus(
                    pipeline,
                    runtimeStatus,
                    runtimeStatus == PipelineRunStatus.FAILED
                            ? messages.get("pipeline.error.flinkJobNotRunning")
                            : pipeline.getLastRunMessage());
        }

        StopFlinkJobResponse response;
        try {
            response = flinkJobGateway.stop(new StopFlinkJobRequest(
                    runtimeTarget.getJobManagerUrl(),
                    pipeline.getLastJobId()));
        } catch (RuntimeException exception) {
            PipelineRunStatus refreshedStatus = runtimeStateSupport.resolveRuntimeStatus(pipeline, runtimeTarget);
            if (refreshedStatus != PipelineRunStatus.RUNNING) {
                return runtimeStateSupport.copyWithRunStatus(
                        pipeline,
                        refreshedStatus,
                        refreshedStatus == PipelineRunStatus.FAILED
                                ? messages.get("pipeline.error.flinkJobNotRunning")
                                : pipeline.getLastRunMessage());
            }
            throw exception;
        }

        pipeline.setLastRunStatus(PipelineRunStatus.STOPPED);
        pipeline.setLastRunMessage(response.message());
        return repository.save(pipeline);
    }

    private Pipeline getStored(Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException(messages.get("pipeline.error.notFound")));
    }

    private String buildDefinitionUrl(Long id) {
        String baseUrl = runtimeProperties.serviceBaseUrl();
        String normalizedBaseUrl = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
        return normalizedBaseUrl + "/api/pipelines/" + id + "/definition";
    }

    private SubmitFlinkJobRequest buildSubmitRequest(
            Long id,
            RunPipelineRequest request,
            FlinkRuntimeTarget runtimeTarget) {
        return new SubmitFlinkJobRequest(
                runtimeTarget.getJobManagerUrl(),
                buildDefinitionUrl(id),
                resolveTestMode(request),
                resolveParallelism(request));
    }

    private boolean resolveTestMode(RunPipelineRequest request) {
        return request != null && request.testMode() != null
                ? request.testMode()
                : runtimeProperties.testMode();
    }

    private int resolveParallelism(RunPipelineRequest request) {
        return request != null && request.parallelism() != null
                ? request.parallelism()
                : runtimeProperties.parallelism();
    }

    private PipelinePreviewExecutionResult toPreviewExecutionResult(
            com.streamcraft.service.pipeline.client.PreviewFlinkJobResponse response) {
        return new PipelinePreviewExecutionResult(response.outputs().stream()
                .map(output -> new PipelinePreviewExecutionResult.Output(output.nodeId(), output.records()))
                .toList());
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
