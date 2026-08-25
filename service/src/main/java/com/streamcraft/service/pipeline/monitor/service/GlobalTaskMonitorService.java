package com.streamcraft.service.pipeline.monitor.service;

import com.streamcraft.service.runtime.model.RuntimeTargetStatus;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.service.FlinkRuntimeTargetService;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.monitor.web.GlobalTaskMonitorResponse;
import com.streamcraft.service.pipeline.service.PipelineRuntimeView;
import com.streamcraft.service.pipeline.service.PipelineRuntimeSnapshot;
import com.streamcraft.service.pipeline.service.PipelineRuntimeQueryService;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class GlobalTaskMonitorService {

    private final PipelineRuntimeQueryService pipelineRuntimeQueryService;
    private final FlinkRuntimeTargetService runtimeTargetService;
    private final UiMessageService messages;

    @Autowired
    public GlobalTaskMonitorService(
            PipelineRuntimeQueryService pipelineRuntimeQueryService,
            FlinkRuntimeTargetService runtimeTargetService,
            UiMessageService messages) {
        this.pipelineRuntimeQueryService = pipelineRuntimeQueryService;
        this.runtimeTargetService = runtimeTargetService;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
    }

    public GlobalTaskMonitorService(
            PipelineRuntimeQueryService pipelineRuntimeQueryService,
            FlinkRuntimeTargetService runtimeTargetService) {
        this(pipelineRuntimeQueryService, runtimeTargetService, UiMessageService.englishFallback());
    }

    public GlobalTaskMonitorResponse getMonitor() {
        List<PipelineRuntimeSnapshot> pipelineRuntimeSnapshots = pipelineRuntimeQueryService.listRuntimeSnapshots();
        FlinkRuntimeTarget runtimeTarget = runtimeTargetService.findTarget().orElse(null);
        int connectedRuntimeTargets = runtimeTarget != null && runtimeTarget.getStatus() == RuntimeTargetStatus.CONNECTED ? 1 : 0;
        int totalRuntimeTargets = runtimeTarget == null ? 0 : 1;
        int totalSlots = runtimeTarget == null ? 0 : safeInt(runtimeTarget.getTotalSlots());
        int availableSlots = runtimeTarget == null ? 0 : safeInt(runtimeTarget.getAvailableSlots());
        int usedSlots = totalSlots - availableSlots;

        List<MonitorEntry> monitorEntries = new ArrayList<>();
        int skippedLiveMetricsCount = 0;
        long runtimeInputRecords = 0L;
        long runtimeOutputRecords = 0L;
        int includedPipelineCount = 0;
        int missingPipelineCount = 0;

        for (PipelineRuntimeSnapshot pipelineSnapshot : pipelineRuntimeSnapshots) {
            PipelineRuntimeView runtimeView = PipelineRuntimeView.of(pipelineSnapshot, runtimeTarget);
            MonitorEntry monitorEntry = buildMonitorEntry(runtimeView);
            if (runtimeView.running()) {
                if (monitorEntry.taskCard.metricsAvailable()) {
                    runtimeInputRecords += safeLong(monitorEntry.taskCard.totalInputRecords());
                    runtimeOutputRecords += safeLong(monitorEntry.taskCard.totalOutputRecords());
                    includedPipelineCount++;
                } else {
                    missingPipelineCount++;
                    skippedLiveMetricsCount++;
                }
            }
            monitorEntries.add(monitorEntry);
        }

        monitorEntries.sort(Comparator.comparingInt((MonitorEntry monitorEntry) -> monitorEntry.monitorStatus.priority())
                .thenComparing(
                        monitorEntry -> monitorEntry.taskCard.updatedAt(),
                        Comparator.nullsLast(Comparator.reverseOrder())));

        List<GlobalTaskMonitorResponse.TaskCard> taskCards = monitorEntries.stream()
                .map(monitorEntry -> monitorEntry.taskCard)
                .toList();

        int failedCount = countByStatus(monitorEntries, MonitorStatus.FAILED);
        int degradedCount = countByStatus(monitorEntries, MonitorStatus.DEGRADED);
        int runningCount = countByStatus(monitorEntries, MonitorStatus.RUNNING);
        int stoppedCount = countByStatus(monitorEntries, MonitorStatus.STOPPED);

        int totalTasks = taskCards.size();
        int healthScore = totalTasks == 0 ? 0 : (int) Math.round((runningCount * 100.0) / totalTasks);

        GlobalTaskMonitorResponse.Summary summary = new GlobalTaskMonitorResponse.Summary(
                totalTasks,
                runningCount,
                failedCount,
                stoppedCount,
                connectedRuntimeTargets,
                totalRuntimeTargets,
                totalSlots,
                usedSlots,
                availableSlots,
                healthScore);

        List<GlobalTaskMonitorResponse.StatusBucket> statusDistribution = List.of(
                new GlobalTaskMonitorResponse.StatusBucket(MonitorStatus.FAILED.name(), failedCount),
                new GlobalTaskMonitorResponse.StatusBucket(MonitorStatus.DEGRADED.name(), degradedCount),
                new GlobalTaskMonitorResponse.StatusBucket(MonitorStatus.RUNNING.name(), runningCount),
                new GlobalTaskMonitorResponse.StatusBucket(MonitorStatus.STOPPED.name(), stoppedCount));

        GlobalTaskMonitorResponse.RuntimeSnapshot monitorRuntimeSnapshot = new GlobalTaskMonitorResponse.RuntimeSnapshot(
                runtimeInputRecords,
                runtimeOutputRecords,
                includedPipelineCount,
                missingPipelineCount);

        GlobalTaskMonitorResponse.Metadata metadata = new GlobalTaskMonitorResponse.Metadata(
                Instant.now(),
                totalTasks,
                skippedLiveMetricsCount);

        return new GlobalTaskMonitorResponse(summary, statusDistribution, taskCards, monitorRuntimeSnapshot, metadata);
    }

    private MonitorEntry buildMonitorEntry(PipelineRuntimeView runtimeView) {
        Pipeline pipeline = runtimeView.pipeline();
        boolean metricsAvailable = runtimeView.metricsAvailable();
        String metricsUnavailableReason = null;

        if (runtimeView.metricsEligible() && !metricsAvailable) {
            metricsUnavailableReason = messages.get("main.metrics.unavailable");
        }

        MonitorStatus monitorStatus = determineStatus(
                pipeline.getLastRunStatus(),
                runtimeView.running(),
                metricsAvailable,
                runtimeView.runtimeTargetUnavailable());
        if (monitorStatus == MonitorStatus.DEGRADED && metricsUnavailableReason == null) {
            metricsUnavailableReason = messages.get("main.metrics.unavailable");
        }

        GlobalTaskMonitorResponse.TaskCard taskCard = new GlobalTaskMonitorResponse.TaskCard(
                pipeline.getId(),
                pipeline.getName(),
                pipeline.getLastRunStatus() == null ? null : pipeline.getLastRunStatus().name(),
                monitorStatus.name(),
                runtimeView.runtimeTargetLabel(),
                pipeline.getLastJobId(),
                runtimeView.durationMillis(),
                metricsAvailable ? runtimeView.totalInputRecords() : null,
                metricsAvailable ? runtimeView.totalOutputRecords() : null,
                runtimeView.nodeCount(),
                metricsAvailable,
                metricsUnavailableReason,
                pipeline.getUpdatedAt(),
                runtimeView.metricsCollectionStatus(),
                runtimeView.runtimeStatusAvailability(),
                runtimeView.runtimeStatusAvailable(),
                runtimeView.runtimeStatusSource(),
                runtimeView.runtimeStatusUnavailableReason());

        return new MonitorEntry(monitorStatus, taskCard);
    }

    private MonitorStatus determineStatus(
            PipelineRunStatus runStatus,
            boolean running,
            boolean metricsAvailable,
            boolean clusterUnreachable) {
        if (runStatus == PipelineRunStatus.FAILED) {
            return MonitorStatus.FAILED;
        }
        if (running && (!metricsAvailable || clusterUnreachable)) {
            return MonitorStatus.DEGRADED;
        }
        if (running && metricsAvailable) {
            return MonitorStatus.RUNNING;
        }
        return MonitorStatus.STOPPED;
    }

    private int countByStatus(List<MonitorEntry> monitorEntries, MonitorStatus status) {
        return (int) monitorEntries.stream()
                .filter(monitorEntry -> monitorEntry.monitorStatus == status)
                .count();
    }

    private int safeInt(Integer value) {
        return value == null ? 0 : value;
    }

    private long safeLong(Long value) {
        return value == null ? 0L : value;
    }

    private record MonitorEntry(MonitorStatus monitorStatus, GlobalTaskMonitorResponse.TaskCard taskCard) {
    }

    private enum MonitorStatus {
        FAILED(0),
        DEGRADED(1),
        RUNNING(2),
        STOPPED(3);

        private final int priority;

        MonitorStatus(int priority) {
            this.priority = priority;
        }

        int priority() {
            return priority;
        }
    }
}
