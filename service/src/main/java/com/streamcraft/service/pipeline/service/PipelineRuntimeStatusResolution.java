package com.streamcraft.service.pipeline.service;

import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.model.PipelineRuntimeStatusSource;
import com.streamcraft.service.pipeline.model.RuntimeDataAvailability;

public record PipelineRuntimeStatusResolution(
        PipelineRunStatus status,
        PipelineRuntimeStatusSource source,
        RuntimeDataAvailability availability,
        String unavailableReason) {

    public static PipelineRuntimeStatusResolution notRequested(PipelineRunStatus status) {
        return new PipelineRuntimeStatusResolution(
                status,
                PipelineRuntimeStatusSource.STORED,
                RuntimeDataAvailability.NOT_REQUESTED,
                null);
    }

    public static PipelineRuntimeStatusResolution fromFlink(PipelineRunStatus status) {
        return new PipelineRuntimeStatusResolution(
                status,
                PipelineRuntimeStatusSource.FLINK,
                RuntimeDataAvailability.AVAILABLE,
                null);
    }

    public static PipelineRuntimeStatusResolution missingJob() {
        return new PipelineRuntimeStatusResolution(
                PipelineRunStatus.FAILED,
                PipelineRuntimeStatusSource.FLINK_NOT_FOUND,
                RuntimeDataAvailability.NO_DATA,
                "JOB_NOT_FOUND");
    }

    public static PipelineRuntimeStatusResolution storedFallback(
            PipelineRunStatus status,
            RuntimeDataAvailability availability,
            String unavailableReason) {
        return new PipelineRuntimeStatusResolution(
                status,
                PipelineRuntimeStatusSource.STORED,
                availability,
                unavailableReason);
    }

    public static PipelineRuntimeStatusResolution storedFallback(
            PipelineRunStatus status,
            String unavailableReason) {
        return storedFallback(status, RuntimeDataAvailability.UNAVAILABLE, unavailableReason);
    }
}
