package com.streamcraft.service.pipeline.service;

import com.streamcraft.service.pipeline.model.NodeMetrics;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineMetrics;

public record PipelineRuntimeSnapshot(
        Pipeline pipeline,
        PipelineMetrics metrics) {

    public boolean metricsAvailable() {
        return metrics != null;
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
