package com.streamcraft.service.pipeline.monitor.web;

import java.time.Instant;
import java.util.List;

public record GlobalTaskMonitorResponse(
        Summary summary,
        List<StatusBucket> statusDistribution,
        List<TaskCard> cards,
        RuntimeSnapshot runtimeSnapshot,
        Metadata metadata) {

    public record Summary(
            int totalTasks,
            int runningTasks,
            int failedTasks,
            int stoppedTasks,
            int connectedRuntimeTargets,
            int totalRuntimeTargets,
            int totalSlots,
            int usedSlots,
            int availableSlots,
            int healthScore) {
    }

    public record StatusBucket(
            String monitorStatus,
            int count) {
    }

    public record TaskCard(
            Long pipelineId,
            String pipelineName,
            String runStatus,
            String monitorStatus,
            String runtimeTargetLabel,
            String jobId,
            Long durationMillis,
            Long totalInputRecords,
            Long totalOutputRecords,
            int nodeCount,
            boolean metricsAvailable,
            String metricsUnavailableReason,
            Instant updatedAt) {
    }

    public record RuntimeSnapshot(
            long totalInputRecords,
            long totalOutputRecords,
            int includedPipelineCount,
            int missingPipelineCount) {
    }

    public record Metadata(
            Instant lastRefreshAt,
            int totalCardCount,
            int skippedLiveMetricsCount) {
    }
}
