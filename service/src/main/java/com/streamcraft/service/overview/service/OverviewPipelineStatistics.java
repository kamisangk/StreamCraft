package com.streamcraft.service.overview.service;

import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.service.PipelineRuntimeView;
import java.time.Instant;

final class OverviewPipelineStatistics {

    private long totalInputRecords;
    private long totalOutputRecords;
    private int includedPipelineCount;
    private int missingPipelineCount;
    private int unhealthyPipelineCount;
    private int totalPipelineCount;
    private int runningPipelineCount;
    private int stoppedPipelineCount;
    private Instant latestSubmittedAt;

    void include(PipelineRuntimeView runtimeView) {
        Pipeline pipeline = runtimeView.pipeline();
        totalPipelineCount++;
        if (runtimeView.running()) {
            runningPipelineCount++;
            includeRuntimeMetrics(runtimeView);
        } else {
            stoppedPipelineCount++;
        }

        updateLatestSubmittedAt(pipeline.getLastSubmittedAt());
        if (pipeline.getLastRunStatus() == PipelineRunStatus.FAILED
                || runtimeView.runtimeTargetUnavailable()
                || (runtimeView.running() && !runtimeView.metricsAvailable())) {
            unhealthyPipelineCount++;
        }
    }

    OverviewStatisticsSnapshot toSnapshot() {
        return new OverviewStatisticsSnapshot(
                totalInputRecords,
                totalOutputRecords,
                includedPipelineCount,
                missingPipelineCount,
                unhealthyPipelineCount,
                totalPipelineCount,
                runningPipelineCount,
                stoppedPipelineCount,
                latestSubmittedAt);
    }

    private void includeRuntimeMetrics(PipelineRuntimeView runtimeView) {
        if (runtimeView.metricsAvailable()) {
            totalInputRecords += runtimeView.totalInputRecords();
            totalOutputRecords += runtimeView.totalOutputRecords();
            includedPipelineCount++;
        } else {
            missingPipelineCount++;
        }
    }

    private void updateLatestSubmittedAt(Instant submittedAt) {
        if (submittedAt != null
                && (latestSubmittedAt == null || submittedAt.isAfter(latestSubmittedAt))) {
            latestSubmittedAt = submittedAt;
        }
    }

    record OverviewStatisticsSnapshot(
            long totalInputRecords,
            long totalOutputRecords,
            int includedPipelineCount,
            int missingPipelineCount,
            int unhealthyPipelineCount,
            int totalPipelineCount,
            int runningPipelineCount,
            int stoppedPipelineCount,
            Instant latestSubmittedAt) {
    }
}
