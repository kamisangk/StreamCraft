package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.streamcraft.service.config.PipelineDeploymentProperties;
import com.streamcraft.service.config.PipelineRuntimeProperties;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.client.FlinkJobGateway;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobRequest;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobResponse;
import com.streamcraft.service.pipeline.client.StopFlinkJobResponse;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunClaim;
import com.streamcraft.service.pipeline.model.PipelineRunClaimStatus;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.persistence.PipelineRepository;
import com.streamcraft.service.pipeline.web.RunPipelineRequest;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class PipelineExecutionServiceExactlyOnceTest {

    private static final Long PIPELINE_ID = 42L;
    private static final String DEFINITION = "{\"pipelineId\":\"pipeline-stable\",\"nodes\":[],\"edges\":[]}";

    private PipelineRepository repository;
    private PipelineDefinitionService definitionService;
    private PipelineRuntimeStateSupport runtimeStateSupport;
    private FlinkJobGateway flinkJobGateway;
    private PipelineRunClaimService claimService;
    private PipelineExecutionService service;
    private FlinkRuntimeTarget runtimeTarget;

    @BeforeEach
    void setUp() {
        repository = mock(PipelineRepository.class);
        definitionService = mock(PipelineDefinitionService.class);
        runtimeStateSupport = mock(PipelineRuntimeStateSupport.class);
        flinkJobGateway = mock(FlinkJobGateway.class);
        claimService = mock(PipelineRunClaimService.class);
        runtimeTarget = new FlinkRuntimeTarget();
        runtimeTarget.setJobManagerUrl("http://flink:8081");

        when(repository.findById(PIPELINE_ID)).thenReturn(Optional.of(storedPipeline()));
        when(definitionService.containsExactlyOnceKafkaSink(DEFINITION)).thenReturn(true);
        when(definitionService.requirePipelineId(DEFINITION)).thenReturn("pipeline-stable");
        when(runtimeStateSupport.requireRuntimeTarget()).thenReturn(runtimeTarget);
        when(repository.save(any(Pipeline.class))).thenAnswer(invocation -> invocation.getArgument(0));
        doNothing().when(definitionService).validateForRun(DEFINITION);

        service = new PipelineExecutionService(
                repository,
                definitionService,
                runtimeStateSupport,
                flinkJobGateway,
                new PipelineRuntimeProperties("http://service:8080", false, 1),
                UiMessageService.englishFallback(),
                claimService,
                new PipelineDeploymentProperties("deployment-a"));
    }

    @Test
    void submitsExactlyOnceWithStableNamespaceAndClaimedJobId() {
        PipelineRunClaim claim = claim(7L, 0L, PipelineRunClaimStatus.SUBMITTING);
        when(claimService.claim(eq(PIPELINE_ID), anyString(), anyString())).thenReturn(claim);
        when(flinkJobGateway.submit(any(SubmitFlinkJobRequest.class))).thenAnswer(invocation -> {
            SubmitFlinkJobRequest request = invocation.getArgument(0);
            return new SubmitFlinkJobResponse(request.jobId(), "accepted");
        });

        Pipeline result = service.run(PIPELINE_ID, new RunPipelineRequest(null, null));

        ArgumentCaptor<SubmitFlinkJobRequest> requestCaptor = ArgumentCaptor.forClass(SubmitFlinkJobRequest.class);
        verify(flinkJobGateway).submit(requestCaptor.capture());
        SubmitFlinkJobRequest request = requestCaptor.getValue();
        assertEquals("deployment-a", request.deploymentNamespace());
        assertEquals(request.jobId(), result.getLastJobId());
        assertEquals(PipelineRunStatus.RUNNING, result.getLastRunStatus());
        verify(claimService).markSubmitted(claim);
    }

    @Test
    void rejectsASecondRunWhileTheClaimedJobIsStillRunning() {
        PipelineRunClaim existing = claim(7L, 2L, PipelineRunClaimStatus.RUNNING);
        when(claimService.claim(eq(PIPELINE_ID), anyString(), anyString()))
                .thenThrow(new PipelineRunClaimService.ClaimConflictException(existing));
        when(runtimeStateSupport.resolveRuntimeStatus(any(Pipeline.class), eq(runtimeTarget)))
                .thenReturn(PipelineRunStatus.RUNNING);

        assertThrows(
                IllegalArgumentException.class,
                () -> service.run(PIPELINE_ID, new RunPipelineRequest(null, null)));

        verify(flinkJobGateway, never()).submit(any(SubmitFlinkJobRequest.class));
        verify(claimService, never()).releaseAfterTerminal(any(PipelineRunClaim.class), anyString());
    }

    @Test
    void releasesAClaimAfterFlinkReportsAterminalJobThenSubmits() {
        PipelineRunClaim existing = claim(7L, 2L, PipelineRunClaimStatus.RUNNING);
        PipelineRunClaim replacement = claim(7L, 3L, PipelineRunClaimStatus.SUBMITTING);
        AtomicInteger attempts = new AtomicInteger();
        when(claimService.claim(eq(PIPELINE_ID), anyString(), anyString())).thenAnswer(invocation -> {
            if (attempts.getAndIncrement() == 0) {
                throw new PipelineRunClaimService.ClaimConflictException(existing);
            }
            return replacement;
        });
        when(runtimeStateSupport.resolveRuntimeStatus(any(Pipeline.class), eq(runtimeTarget)))
                .thenReturn(PipelineRunStatus.FAILED);
        when(flinkJobGateway.submit(any(SubmitFlinkJobRequest.class))).thenAnswer(invocation -> {
            SubmitFlinkJobRequest request = invocation.getArgument(0);
            return new SubmitFlinkJobResponse(request.jobId(), "accepted");
        });

        service.run(PIPELINE_ID, new RunPipelineRequest(null, null));

        verify(claimService).releaseAfterTerminal(existing, "FLINK_JOB_TERMINAL");
        verify(claimService).markSubmitted(replacement);
        verify(flinkJobGateway).submit(any(SubmitFlinkJobRequest.class));
    }

    @Test
    void keepsTheClaimActiveWhenSubmissionFailsBeforeAcceptance() {
        PipelineRunClaim claim = claim(7L, 0L, PipelineRunClaimStatus.SUBMITTING);
        when(claimService.claim(eq(PIPELINE_ID), anyString(), anyString())).thenReturn(claim);
        when(flinkJobGateway.submit(any(SubmitFlinkJobRequest.class)))
                .thenThrow(new IllegalStateException("broker rejected"));

        assertThrows(
                IllegalStateException.class,
                () -> service.run(PIPELINE_ID, new RunPipelineRequest(null, null)));

        verify(claimService).releaseAfterSubmitFailure(eq(claim), eq("broker rejected"));
        verify(claimService, never()).recordUncertainSubmission(any(PipelineRunClaim.class), anyString());
        verify(repository, never()).save(any(Pipeline.class));
    }

    @Test
    void conservativelyRejectsWhenRuntimeStatusCannotConfirmTermination() {
        PipelineRunClaim existing = claim(7L, 2L, PipelineRunClaimStatus.RUNNING);
        when(claimService.claim(eq(PIPELINE_ID), anyString(), anyString()))
                .thenThrow(new PipelineRunClaimService.ClaimConflictException(existing));
        when(runtimeStateSupport.resolveRuntimeStatus(any(Pipeline.class), eq(runtimeTarget)))
                .thenReturn(PipelineRunStatus.RUNNING);

        assertThrows(
                IllegalArgumentException.class,
                () -> service.run(PIPELINE_ID, new RunPipelineRequest(null, null)));

        verify(claimService, never()).releaseAfterTerminal(any(PipelineRunClaim.class), anyString());
        verify(flinkJobGateway, never()).submit(any(SubmitFlinkJobRequest.class));
    }

    @Test
    void releasesTheClaimAfterStoppingTheClaimedJob() {
        Pipeline pipeline = storedPipeline();
        pipeline.setLastJobId("job-old");
        pipeline.setLastRunStatus(PipelineRunStatus.RUNNING);
        PipelineRunClaim claim = claim(7L, 2L, PipelineRunClaimStatus.RUNNING);
        when(repository.findById(PIPELINE_ID)).thenReturn(Optional.of(pipeline));
        when(runtimeStateSupport.resolveRuntimeStatus(pipeline, runtimeTarget))
                .thenReturn(PipelineRunStatus.RUNNING);
        when(claimService.find(PIPELINE_ID)).thenReturn(Optional.of(claim));
        when(flinkJobGateway.stop(any())).thenReturn(new StopFlinkJobResponse("job-old", null, "stopped"));
        when(repository.save(any(Pipeline.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.stop(PIPELINE_ID);

        verify(claimService).releaseAfterTerminal(claim, "FLINK_JOB_STOPPED");
        assertEquals(PipelineRunStatus.STOPPED, pipeline.getLastRunStatus());
    }

    @Test
    void releasesTheClaimWhenStopFailsAfterFlinkReportsTermination() {
        Pipeline pipeline = storedPipeline();
        pipeline.setLastJobId("job-old");
        pipeline.setLastRunStatus(PipelineRunStatus.RUNNING);
        PipelineRunClaim claim = claim(7L, 2L, PipelineRunClaimStatus.RUNNING);
        when(repository.findById(PIPELINE_ID)).thenReturn(Optional.of(pipeline));
        when(runtimeStateSupport.resolveRuntimeStatus(pipeline, runtimeTarget))
                .thenReturn(PipelineRunStatus.RUNNING, PipelineRunStatus.FAILED);
        when(runtimeStateSupport.copyWithRunStatus(eq(pipeline), eq(PipelineRunStatus.FAILED), anyString()))
                .thenReturn(pipeline);
        when(claimService.find(PIPELINE_ID)).thenReturn(Optional.of(claim));
        when(flinkJobGateway.stop(any()))
                .thenThrow(new IllegalStateException("stop request timed out"));

        assertEquals(pipeline, service.stop(PIPELINE_ID));

        verify(claimService).releaseAfterTerminal(claim, "FLINK_JOB_TERMINAL");
    }

    private Pipeline storedPipeline() {
        Pipeline pipeline = new Pipeline();
        pipeline.setId(PIPELINE_ID);
        pipeline.setName("exactly-once-pipeline");
        pipeline.setDefinitionJson(DEFINITION);
        return pipeline;
    }

    private PipelineRunClaim claim(Long id, Long version, PipelineRunClaimStatus status) {
        PipelineRunClaim claim = new PipelineRunClaim();
        claim.setId(id);
        claim.setPipelineId(PIPELINE_ID);
        claim.setJobId("job-old");
        claim.setOwnerToken("owner-old");
        claim.setVersion(version);
        claim.setStatus(status);
        return claim;
    }
}
