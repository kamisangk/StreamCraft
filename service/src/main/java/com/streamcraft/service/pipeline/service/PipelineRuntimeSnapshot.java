package com.streamcraft.service.pipeline.service;

import com.streamcraft.service.pipeline.model.NodeMetrics;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineMetrics;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.model.RuntimeDataAvailability;

public record PipelineRuntimeSnapshot(
        Pipeline pipeline,
        PipelineMetrics metrics,
        PipelineRuntimeStatusResolution statusResolution) {

    public PipelineRuntimeSnapshot(Pipeline pipeline, PipelineMetrics metrics) {
        this(
                pipeline,
                metrics,
                PipelineRuntimeStatusResolution.notRequested(pipeline.getLastRunStatus()));
    }

    public boolean metricsAvailable() {
        return metrics != null
                && metrics.getNodeMetrics() != null
                && !metrics.getNodeMetrics().isEmpty()
                && (metrics.getCollectionStatus() == RuntimeDataAvailability.AVAILABLE
                || metrics.getCollectionStatus() == RuntimeDataAvailability.PARTIAL);
    }

    public RuntimeDataAvailability metricsCollectionStatus() {
        return metrics == null || metrics.getCollectionStatus() == null
                ? RuntimeDataAvailability.NOT_REQUESTED
                : metrics.getCollectionStatus();
    }

    public String metricsUnavailableReason() {
        return metrics == null ? null : metrics.getUnavailableReason();
    }

    public boolean runtimeStatusAvailable() {
        return statusResolution != null
                && statusResolution.availability() == RuntimeDataAvailability.AVAILABLE;
    }

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

    public Long durationMillis() {
        return metrics == null ? null : metrics.getDuration();
    }

    public long totalInputRecords() {
        if (metrics == null || metrics.getNodeMetrics() == null) {
            return 0L;
        }
        long total = 0L;
        for (NodeMetrics nodeMetrics : metrics.getNodeMetrics()) {
            total += nodeMetrics.getInputRecords() == null ? 0L : nodeMetrics.getInputRecords();
        }
        return total;
    }

    public long totalOutputRecords() {
        if (metrics == null || metrics.getNodeMetrics() == null) {
            return 0L;
        }
        long total = 0L;
        for (NodeMetrics nodeMetrics : metrics.getNodeMetrics()) {
            total += nodeMetrics.getOutputRecords() == null ? 0L : nodeMetrics.getOutputRecords();
        }
        return total;
    }

    public int nodeCount() {
        return metrics == null || metrics.getNodeMetrics() == null
                ? 0
                : metrics.getNodeMetrics().size();
    }
}
