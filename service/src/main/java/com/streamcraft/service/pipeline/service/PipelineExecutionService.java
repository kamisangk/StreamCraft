package com.streamcraft.service.pipeline.service;

import com.streamcraft.service.config.PipelineRuntimeProperties;
import com.streamcraft.service.config.PipelineDeploymentProperties;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.client.FlinkJobGateway;
import com.streamcraft.service.pipeline.client.PreviewFlinkJobRequest;
import com.streamcraft.service.pipeline.client.StopFlinkJobRequest;
import com.streamcraft.service.pipeline.client.StopFlinkJobResponse;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobRequest;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobResponse;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunClaim;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.persistence.PipelineRepository;
import com.streamcraft.service.pipeline.web.PipelinePreviewRequest;
import com.streamcraft.service.pipeline.web.PipelinePreviewResponse;
import com.streamcraft.service.pipeline.web.RunPipelineRequest;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
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
    private final PipelineRunClaimService claimService;
    private final PipelineDeploymentProperties deploymentProperties;

    @Autowired
    public PipelineExecutionService(
            PipelineRepository repository,
            PipelineDefinitionService definitionService,
            PipelineRuntimeStateSupport runtimeStateSupport,
            FlinkJobGateway flinkJobGateway,
            PipelineRuntimeProperties runtimeProperties,
            UiMessageService messages,
            PipelineRunClaimService claimService,
            PipelineDeploymentProperties deploymentProperties) {
        this.repository = repository;
        this.definitionService = definitionService;
        this.runtimeStateSupport = runtimeStateSupport;
        this.flinkJobGateway = flinkJobGateway;
        this.runtimeProperties = runtimeProperties;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
        this.claimService = claimService;
        this.deploymentProperties = deploymentProperties;
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
                UiMessageService.englishFallback(),
                null,
                null);
    }

    public PipelineExecutionService(
            PipelineRepository repository,
            PipelineDefinitionService definitionService,
            PipelineRuntimeStateSupport runtimeStateSupport,
            FlinkJobGateway flinkJobGateway,
            PipelineRuntimeProperties runtimeProperties,
            UiMessageService messages) {
        this(
                repository,
                definitionService,
                runtimeStateSupport,
                flinkJobGateway,
                runtimeProperties,
                messages,
                null,
                null);
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

        if (!definitionService.containsExactlyOnceKafkaSink(pipeline.getDefinitionJson())) {
            return submitWithoutClaim(pipeline, id, request, runtimeTarget);
        }

        definitionService.requirePipelineId(pipeline.getDefinitionJson());
        String deploymentNamespace = requireDeploymentNamespace();
        String jobId = generateJobId();
        String ownerToken = UUID.randomUUID().toString();
        PipelineRunClaim claim = claimOrReconcile(
                id,
                jobId,
                ownerToken,
                runtimeTarget);
        boolean accepted = false;
        try {
            SubmitFlinkJobResponse response = flinkJobGateway.submit(
                    buildSubmitRequest(id, request, runtimeTarget, jobId, deploymentNamespace));
            requireMatchingSubmission(response, jobId);
            accepted = true;
            claimService.markSubmitted(claim);

            pipeline.setLastRunStatus(PipelineRunStatus.RUNNING);
            pipeline.setLastRunMessage(response.message());
            pipeline.setLastJobId(response.jobId());
            pipeline.setLastSubmittedAt(Instant.now());
            return repository.save(pipeline);
        } catch (RuntimeException exception) {
            compensateClaimFailure(claim, accepted, exception);
            throw exception;
        }
    }

    private Pipeline submitWithoutClaim(
            Pipeline pipeline,
            Long id,
            RunPipelineRequest request,
            FlinkRuntimeTarget runtimeTarget) {
        SubmitFlinkJobResponse response = flinkJobGateway.submit(
                buildSubmitRequest(id, request, runtimeTarget, null, null));

        pipeline.setLastRunStatus(PipelineRunStatus.RUNNING);
        pipeline.setLastRunMessage(response.message());
        pipeline.setLastJobId(response.jobId());
        pipeline.setLastSubmittedAt(Instant.now());
        return repository.save(pipeline);
    }

    private PipelineRunClaim claimOrReconcile(
            Long pipelineId,
            String jobId,
            String ownerToken,
            FlinkRuntimeTarget runtimeTarget) {
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                return claimServiceRequired().claim(pipelineId, jobId, ownerToken);
            } catch (PipelineRunClaimService.ClaimConflictException conflict) {
                PipelineRunClaim existing = conflict.claim();
                if (existing == null || !isTerminal(existing, runtimeTarget)) {
                    throw alreadyRunning();
                }
                try {
                    claimServiceRequired().releaseAfterTerminal(existing, "FLINK_JOB_TERMINAL");
                } catch (PipelineRunClaimService.ClaimConflictException raced) {
                    throw alreadyRunning();
                }
            }
        }
        throw alreadyRunning();
    }

    private boolean isTerminal(PipelineRunClaim claim, FlinkRuntimeTarget runtimeTarget) {
        if (claim.getJobId() == null || claim.getJobId().isBlank()) {
            return false;
        }
        Pipeline probe = new Pipeline();
        probe.setLastJobId(claim.getJobId());
        probe.setLastRunStatus(PipelineRunStatus.RUNNING);
        return runtimeStateSupport.resolveRuntimeStatus(probe, runtimeTarget) != PipelineRunStatus.RUNNING;
    }

    private void requireMatchingSubmission(SubmitFlinkJobResponse response, String expectedJobId) {
        if (response == null || !expectedJobId.equalsIgnoreCase(response.jobId())) {
            throw new IllegalStateException(
                    "Flink gateway returned a different JobID than the claimed submission.");
        }
    }

    private void compensateClaimFailure(PipelineRunClaim claim, boolean accepted, RuntimeException exception) {
        if (claim == null || claimService == null) {
            return;
        }
        try {
            if (accepted || isAmbiguousSubmissionFailure(exception)) {
                claimService.recordUncertainSubmission(claim, exception.getMessage());
            } else {
                claimService.releaseAfterSubmitFailure(claim, exception.getMessage());
            }
        } catch (RuntimeException compensationFailure) {
            exception.addSuppressed(compensationFailure);
        }
    }

    private boolean isAmbiguousSubmissionFailure(RuntimeException exception) {
        String message = exception.getMessage();
        if (message == null) {
            return false;
        }
        String normalized = message.toLowerCase(java.util.Locale.ROOT);
        return normalized.contains("timed out") || normalized.contains("timeout")
                || normalized.contains("different jobid");
    }

    private String requireDeploymentNamespace() {
        if (deploymentProperties == null) {
            throw new IllegalStateException(
                    "streamcraft.deployment.namespace must be configured for Kafka EXACTLY_ONCE pipelines.");
        }
        return deploymentProperties.requiredNamespace();
    }

    private PipelineRunClaimService claimServiceRequired() {
        if (claimService == null) {
            throw new IllegalStateException("Pipeline run claim service is not configured.");
        }
        return claimService;
    }

    private IllegalArgumentException alreadyRunning() {
        return new IllegalArgumentException(messages.get("pipeline.error.alreadyRunning"));
    }

    private String generateJobId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    public Pipeline stop(Long id) {
        Pipeline pipeline = getStored(id);
        if (!hasText(pipeline.getLastJobId())) {
            throw new IllegalArgumentException(messages.get("pipeline.error.runningJobIdMissing"));
        }
        FlinkRuntimeTarget runtimeTarget = runtimeStateSupport.requireRuntimeTarget();

        PipelineRunStatus runtimeStatus = runtimeStateSupport.resolveRuntimeStatus(pipeline, runtimeTarget);
        if (runtimeStatus != PipelineRunStatus.RUNNING) {
            releaseTerminalClaim(id, pipeline.getLastJobId(), "FLINK_JOB_TERMINAL");
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
                releaseTerminalClaim(id, pipeline.getLastJobId(), "FLINK_JOB_TERMINAL");
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
        releaseTerminalClaim(id, pipeline.getLastJobId(), "FLINK_JOB_STOPPED");
        return repository.save(pipeline);
    }

    private void releaseTerminalClaim(Long pipelineId, String jobId, String reason) {
        if (claimService == null || !hasText(jobId)) {
            return;
        }
        PipelineRunClaim claim = claimService.find(pipelineId)
                .filter(candidate -> candidate.isActive() && Objects.equals(jobId, candidate.getJobId()))
                .orElse(null);
        if (claim == null) {
            return;
        }
        try {
            claimService.releaseAfterTerminal(claim, reason);
        } catch (PipelineRunClaimService.ClaimConflictException ignored) {
            // A concurrent terminal reconciliation already fenced this claim.
        }
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
            FlinkRuntimeTarget runtimeTarget,
            String jobId,
            String deploymentNamespace) {
        return new SubmitFlinkJobRequest(
                runtimeTarget.getJobManagerUrl(),
                buildDefinitionUrl(id),
                resolveTestMode(request),
                resolveParallelism(request),
                jobId,
                deploymentNamespace);
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
