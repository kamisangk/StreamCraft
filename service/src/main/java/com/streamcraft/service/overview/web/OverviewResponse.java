package com.streamcraft.service.overview.web;

import java.time.Instant;
import java.util.List;

public record OverviewResponse(
        Summary summary,
        RuntimeSnapshot runtimeSnapshot,
        List<RuntimeTargetCapacity> runtimeTargetCapacities,
        List<PipelineRow> pipelines) {

    public record Summary(
            int totalRuntimeTargets,
            int connectedRuntimeTargets,
            int totalPipelines,
            int runningPipelines,
            int stoppedPipelines,
            int unhealthyPipelines,
            Instant latestSubmittedAt) {
    }

    public record RuntimeSnapshot(
            long totalInputRecords,
            long totalOutputRecords,
            int includedPipelineCount,
            int missingPipelineCount) {
    }

    public record RuntimeTargetCapacity(
            Long runtimeTargetId,
            String runtimeTargetName,
            String runtimeTargetStatus,
            int totalSlots,
            int availableSlots,
            int usedSlots,
            Integer usagePercent,
            String statusMessage) {
    }

    public record PipelineRow(
            Long pipelineId,
            String pipelineName,
            List<String> sourceLabels,
            List<String> sinkLabels,
            String runStatus,
            String runtimeTargetLabel,
            Long durationMillis,
            boolean metricsAvailable,
            String metricsUnavailableReason,
            Instant updatedAt) {
    }
}
