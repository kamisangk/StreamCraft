package com.streamcraft.service.pipeline.service;

import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineMetrics;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.persistence.PipelineRepository;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PipelineRuntimeQueryService {

    private final PipelineRepository repository;
    private final PipelineRuntimeStateSupport runtimeStateSupport;
    private final UiMessageService messages;

    @Autowired
    public PipelineRuntimeQueryService(
            PipelineRepository repository,
            PipelineRuntimeStateSupport runtimeStateSupport,
            UiMessageService messages) {
        this.repository = repository;
        this.runtimeStateSupport = runtimeStateSupport;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
    }

    public PipelineRuntimeQueryService(
            PipelineRepository repository,
            PipelineRuntimeStateSupport runtimeStateSupport) {
        this(repository, runtimeStateSupport, UiMessageService.englishFallback());
    }

    public List<Pipeline> list() {
        FlinkRuntimeTarget runtimeTarget = runtimeStateSupport.findRuntimeTarget();
        return runtimeStateSupport.resolveRuntimeStatuses(
                repository.findAllByOrderByUpdatedAtDesc(), runtimeTarget);
    }

    public List<Pipeline> listRunningPipelines() {
        FlinkRuntimeTarget runtimeTarget = runtimeStateSupport.findRuntimeTarget();
        return runtimeStateSupport.resolveRuntimeStatuses(
                        repository.findByLastRunStatus(PipelineRunStatus.RUNNING), runtimeTarget).stream()
                .filter(runtimeStateSupport::isRunning)
                .toList();
    }

    public List<PipelineRuntimeSnapshot> listRuntimeSnapshots() {
        List<Pipeline> storedPipelines = repository.findAllByOrderByUpdatedAtDesc();
        FlinkRuntimeTarget runtimeTarget = runtimeStateSupport.findRuntimeTarget();

        List<PipelineRuntimeSnapshot> snapshots = new ArrayList<>();
        for (Pipeline pipeline : storedPipelines) {
            snapshots.add(runtimeStateSupport.buildRuntimeSnapshot(pipeline, runtimeTarget));
        }
        return snapshots;
    }

    public List<PipelineRuntimeStatusSnapshot> listRuntimeStatusSnapshots() {
        List<Pipeline> storedPipelines = repository.findAllByOrderByUpdatedAtDesc();
        FlinkRuntimeTarget runtimeTarget = runtimeStateSupport.findRuntimeTarget();

        List<PipelineRuntimeStatusSnapshot> snapshots = new ArrayList<>();
        for (Pipeline pipeline : storedPipelines) {
            snapshots.add(runtimeStateSupport.buildRuntimeStatusSnapshot(pipeline, runtimeTarget));
        }
        return snapshots;
    }

    public Pipeline get(Long id) {
        return runtimeStateSupport.withResolvedRuntimeStatus(getStored(id));
    }

    public PipelineRuntimeStatusSnapshot getRuntimeStatusSnapshot(Long id) {
        Pipeline pipeline = getStored(id);
        return runtimeStateSupport.buildRuntimeStatusSnapshot(
                pipeline,
                runtimeStateSupport.findRuntimeTarget());
    }

    public PipelineMetrics getMetrics(Long id) {
        Pipeline pipeline = getStored(id);
        if (!hasText(pipeline.getLastJobId())) {
            throw new IllegalArgumentException(messages.get("pipeline.error.runningJobMissing"));
        }
        return runtimeStateSupport.resolveMetrics(pipeline, runtimeStateSupport.requireRuntimeTarget());
    }

    private Pipeline getStored(Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException(messages.get("pipeline.error.notFound")));
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
