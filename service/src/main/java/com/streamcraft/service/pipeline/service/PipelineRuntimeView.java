package com.streamcraft.service.pipeline.service;

import com.streamcraft.service.runtime.model.RuntimeTargetStatus;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;

public record PipelineRuntimeView(
        PipelineRuntimeSnapshot snapshot,
        FlinkRuntimeTarget runtimeTarget) {

    public static PipelineRuntimeView of(
            PipelineRuntimeSnapshot snapshot,
            FlinkRuntimeTarget runtimeTarget) {
        return new PipelineRuntimeView(snapshot, runtimeTarget);
    }

    public Pipeline pipeline() {
        return snapshot.pipeline();
    }

    public boolean running() {
        return pipeline().getLastRunStatus() == PipelineRunStatus.RUNNING;
    }

    public boolean runtimeTargetConnected() {
        return runtimeTarget != null && runtimeTarget.getStatus() == RuntimeTargetStatus.CONNECTED;
    }

    public boolean runtimeTargetUnavailable() {
        return runtimeTarget != null && !runtimeTargetConnected();
    }

    public boolean metricsEligible() {
        return running()
                && hasText(pipeline().getLastJobId())
                && runtimeTargetConnected();
    }

    public boolean metricsAvailable() {
        return snapshot.metricsAvailable();
    }

    public String runtimeTargetLabel() {
        if (runtimeTarget == null || runtimeTarget.getType() == null) {
            return "Unconfigured";
        }
        return "Flink Standalone";
    }

    public Long durationMillis() {
        return snapshot.durationMillis();
    }

    public long totalInputRecords() {
        return snapshot.totalInputRecords();
    }

    public long totalOutputRecords() {
        return snapshot.totalOutputRecords();
    }

    public int nodeCount() {
        return snapshot.nodeCount();
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
