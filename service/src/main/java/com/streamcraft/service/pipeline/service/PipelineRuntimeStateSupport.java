package com.streamcraft.service.pipeline.service;

import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.client.FlinkMetricsClient;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineMetrics;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.model.RuntimeDataAvailability;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.service.FlinkRuntimeTargetService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;

@Service
public class PipelineRuntimeStateSupport {

    private final PipelineDefinitionService definitionService;
    private final FlinkRuntimeTargetService runtimeTargetService;
    private final FlinkMetricsClient flinkMetricsClient;
    private final UiMessageService messages;

    @Autowired
    public PipelineRuntimeStateSupport(
            PipelineDefinitionService definitionService,
            FlinkRuntimeTargetService runtimeTargetService,
            FlinkMetricsClient flinkMetricsClient,
            UiMessageService messages) {
        this.definitionService = definitionService;
        this.runtimeTargetService = runtimeTargetService;
        this.flinkMetricsClient = flinkMetricsClient;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
    }

    public PipelineRuntimeStateSupport(
            PipelineDefinitionService definitionService,
            FlinkRuntimeTargetService runtimeTargetService,
            FlinkMetricsClient flinkMetricsClient) {
        this(definitionService, runtimeTargetService, flinkMetricsClient, UiMessageService.englishFallback());
    }

    FlinkRuntimeTarget findRuntimeTarget() {
        return runtimeTargetService.findTarget().orElse(null);
    }

    FlinkRuntimeTarget requireRuntimeTarget() {
        return runtimeTargetService.requireTarget();
    }

    List<Pipeline> resolveRuntimeStatuses(
            List<Pipeline> storedPipelines,
            FlinkRuntimeTarget runtimeTarget) {
        List<Pipeline> resolvedPipelines = new ArrayList<>();
        for (Pipeline pipeline : storedPipelines) {
            resolvedPipelines.add(withResolvedRuntimeStatus(pipeline, runtimeTarget));
        }
        return resolvedPipelines;
    }

    Pipeline withResolvedRuntimeStatus(Pipeline pipeline) {
        return withResolvedRuntimeStatus(pipeline, findRuntimeTarget());
    }

    Pipeline withResolvedRuntimeStatus(Pipeline pipeline, FlinkRuntimeTarget runtimeTarget) {
        Pipeline pipelineSnapshot = copyPipeline(pipeline);
        pipelineSnapshot.setLastRunStatus(resolveRuntimeStatus(pipeline, runtimeTarget));
        return pipelineSnapshot;
    }

    PipelineRuntimeSnapshot buildRuntimeSnapshot(
            Pipeline pipeline,
            FlinkRuntimeTarget runtimeTarget) {
        PipelineRuntimeStatusSnapshot statusSnapshot = buildRuntimeStatusSnapshot(pipeline, runtimeTarget);
        PipelineMetrics metrics = resolveMetricsIfEligible(statusSnapshot.pipeline(), runtimeTarget);
        return new PipelineRuntimeSnapshot(
                statusSnapshot.pipeline(),
                metrics,
                statusSnapshot.statusResolution());
    }

    PipelineRuntimeStatusSnapshot buildRuntimeStatusSnapshot(
            Pipeline pipeline,
            FlinkRuntimeTarget runtimeTarget) {
        Pipeline pipelineSnapshot = copyPipeline(pipeline);
        PipelineRuntimeStatusResolution statusResolution = resolveRuntimeStatusResolution(pipeline, runtimeTarget);
        pipelineSnapshot.setLastRunStatus(statusResolution.status());
        return new PipelineRuntimeStatusSnapshot(pipelineSnapshot, statusResolution);
    }

    PipelineRunStatus resolveRuntimeStatus(Pipeline pipeline) {
        return resolveRuntimeStatus(pipeline, findRuntimeTarget());
    }

    PipelineRunStatus resolveRuntimeStatus(
            Pipeline pipeline,
            FlinkRuntimeTarget runtimeTarget) {
        return resolveRuntimeStatusResolution(pipeline, runtimeTarget).status();
    }

    PipelineRuntimeStatusResolution resolveRuntimeStatusResolution(
            Pipeline pipeline,
            FlinkRuntimeTarget runtimeTarget) {
        if (!hasResolvableRunningJob(pipeline)
                || runtimeTarget == null
                || !hasText(runtimeTarget.getJobManagerUrl())) {
            return PipelineRuntimeStatusResolution.notRequested(pipeline.getLastRunStatus());
        }

        try {
            PipelineRunStatus liveStatus = flinkMetricsClient.getJobStatus(
                    runtimeTarget.getJobManagerUrl(),
                    pipeline.getLastJobId());
            if (liveStatus == null) {
                return PipelineRuntimeStatusResolution.storedFallback(
                        pipeline.getLastRunStatus(),
                        RuntimeDataAvailability.NO_DATA,
                        "JOB_STATUS_EMPTY");
            }
            PipelineRunStatus resolvedStatus = liveStatus == PipelineRunStatus.RUNNING
                    ? PipelineRunStatus.RUNNING
                    : PipelineRunStatus.FAILED;
            return PipelineRuntimeStatusResolution.fromFlink(resolvedStatus);
        } catch (HttpClientErrorException exception) {
            return exception.getStatusCode() == HttpStatus.NOT_FOUND
                    ? PipelineRuntimeStatusResolution.missingJob()
                    : PipelineRuntimeStatusResolution.storedFallback(
                            pipeline.getLastRunStatus(),
                            "JOB_STATUS_QUERY_FAILED");
        } catch (RuntimeException exception) {
            return PipelineRuntimeStatusResolution.storedFallback(
                    pipeline.getLastRunStatus(),
                    "JOB_STATUS_QUERY_FAILED");
        }
    }

    PipelineMetrics resolveMetricsIfEligible(
            Pipeline pipeline,
            FlinkRuntimeTarget runtimeTarget) {
        if (!hasResolvableRunningJob(pipeline)) {
            return null;
        }

        if (runtimeTarget == null || !hasText(runtimeTarget.getJobManagerUrl())) {
            return null;
        }

        try {
            return resolveMetrics(pipeline, runtimeTarget);
        } catch (RuntimeException exception) {
            return PipelineMetrics.unavailable(pipeline.getLastJobId(), "JOB_METRICS_QUERY_FAILED");
        }
    }

    PipelineMetrics resolveMetrics(Pipeline pipeline, FlinkRuntimeTarget runtimeTarget) {
        try {
            PipelineDefinitionService.DefinitionNodes definitionNodes =
                    definitionService.parseDefinitionNodes(pipeline.getDefinitionJson());
            return flinkMetricsClient.getJobMetrics(
                    runtimeTarget.getJobManagerUrl(),
                    pipeline.getLastJobId(),
                    definitionNodes.nodeIds(),
                    definitionNodes.nodeNames());
        } catch (IOException ex) {
            throw new IllegalStateException(messages.get("pipeline.error.parseDefinitionFailed"), ex);
        }
    }

    boolean hasResolvableRunningJob(Pipeline pipeline) {
        return pipeline.getLastRunStatus() == PipelineRunStatus.RUNNING
                && hasText(pipeline.getLastJobId());
    }

    boolean isRunning(Pipeline pipeline) {
        return pipeline.getLastRunStatus() == PipelineRunStatus.RUNNING;
    }

    Pipeline copyPipeline(Pipeline source) {
        Pipeline copy = new Pipeline();
        copy.setId(source.getId());
        copy.setName(source.getName());
        copy.setDescription(source.getDescription());
        copy.setDefinitionJson(source.getDefinitionJson());
        copy.setLastJobId(source.getLastJobId());
        copy.setLastRunStatus(source.getLastRunStatus());
        copy.setLastRunMessage(source.getLastRunMessage());
        copy.setLastSubmittedAt(source.getLastSubmittedAt());
        copy.setCreatedAt(source.getCreatedAt());
        copy.setUpdatedAt(source.getUpdatedAt());
        return copy;
    }

    Pipeline copyWithRunStatus(
            Pipeline source,
            PipelineRunStatus runStatus,
            String runMessage) {
        Pipeline copy = copyPipeline(source);
        copy.setLastRunStatus(runStatus);
        copy.setLastRunMessage(runMessage);
        return copy;
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
