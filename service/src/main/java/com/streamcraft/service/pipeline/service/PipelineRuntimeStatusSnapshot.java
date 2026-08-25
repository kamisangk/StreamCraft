package com.streamcraft.service.pipeline.service;

import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.RuntimeDataAvailability;

/**
 * Runtime status data without loading the more expensive node metrics.
 */
public record PipelineRuntimeStatusSnapshot(
        Pipeline pipeline,
        PipelineRuntimeStatusResolution statusResolution) {

    public RuntimeDataAvailability runtimeStatusAvailability() {
        return statusResolution == null || statusResolution.availability() == null
                ? RuntimeDataAvailability.NOT_REQUESTED
                : statusResolution.availability();
    }

    public String runtimeStatusSource() {
        return statusResolution == null || statusResolution.source() == null
                ? null
                : statusResolution.source().name();
    }

    public String runtimeStatusUnavailableReason() {
        return statusResolution == null ? null : statusResolution.unavailableReason();
    }
}
