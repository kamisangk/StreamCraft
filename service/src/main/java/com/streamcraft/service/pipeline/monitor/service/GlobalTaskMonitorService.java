package com.streamcraft.service.pipeline.monitor.service;

import com.streamcraft.service.runtime.model.RuntimeTargetStatus;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.service.FlinkRuntimeTargetService;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.monitor.web.GlobalTaskMonitorResponse;
import com.streamcraft.service.pipeline.service.PipelineRuntimeView;
import com.streamcraft.service.pipeline.service.PipelineService;
import com.streamcraft.service.pipeline.service.PipelineRuntimeSnapshot;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class GlobalTaskMonitorService {

    private final PipelineService pipelineService;
    private final FlinkRuntimeTargetService runtimeTargetService;
    private final UiMessageService messages;

    @Autowired
    public GlobalTaskMonitorService(
            PipelineService pipelineService,
            FlinkRuntimeTargetService runtimeTargetService,
            UiMessageService messages) {
        this.pipelineService = pipelineService;
        this.runtimeTargetService = runtimeTargetService;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
    }

    public GlobalTaskMonitorService(
            PipelineService pipelineService,
            FlinkRuntimeTargetService runtimeTargetService) {
        this(pipelineService, runtimeTargetService, UiMessageService.englishFallback());
    }

    public GlobalTaskMonitorResponse getMonitor() {
        List<PipelineRuntimeSnapshot> runtimeSnapshots = pipelineService.listRuntimeSnapshots();
        FlinkRuntimeTarget runtimeTarget = runtimeTargetService.findTarget().orElse(null);
        int connectedRuntimeTargets = runtimeTarget != null && runtimeTarget.getStatus() == RuntimeTargetStatus.CONNECTED ? 1 : 0;
        int totalRuntimeTargets = runtimeTarget == null ? 0 : 1;
        int totalSlots = runtimeTarget == null ? 0 : safeInt(runtimeTarget.getTotalSlots());
        int availableSlots = runtimeTarget == null ? 0 : safeInt(runtimeTarget.getAvailableSlots());
        int usedSlots = totalSlots - availableSlots;

        List<MonitorCard> monitorCards = new ArrayList<>();
        int skippedLiveMetricsCount = 0;
        long runtimeInputRecords = 0L;
        long runtimeOutputRecords = 0L;
        int includedPipelineCount = 0;
        int missingPipelineCount = 0;

        for (PipelineRuntimeSnapshot runtimeSnapshot : runtimeSnapshots) {
            PipelineRuntimeView runtimeView = PipelineRuntimeView.of(runtimeSnapshot, runtimeTarget);
            MonitorCard monitorCard = buildCard(runtimeView);
            if (runtimeView.running()) {
                if (monitorCard.card.metricsAvailable()) {
                    runtimeInputRecords += safeLong(monitorCard.card.totalInputRecords());
                    runtimeOutputRecords += safeLong(monitorCard.card.totalOutputRecords());
                    includedPipelineCount++;
                } else {
                    missingPipelineCount++;
                    skippedLiveMetricsCount++;
                }
            }
            monitorCards.add(monitorCard);
        }

        monitorCards.sort(Comparator.comparingInt((MonitorCard card) -> card.monitorStatus.priority())
                .thenComparing(
                        card -> card.card.updatedAt(),
                        Comparator.nullsLast(Comparator.reverseOrder())));

        List<GlobalTaskMonitorResponse.TaskCard> cards = monitorCards.stream()
                .map(card -> card.card)
                .toList();

        int failedCount = countByStatus(monitorCards, MonitorStatus.FAILED);
        int degradedCount = countByStatus(monitorCards, MonitorStatus.DEGRADED);
        int runningCount = countByStatus(monitorCards, MonitorStatus.RUNNING);
        int stoppedCount = countByStatus(monitorCards, MonitorStatus.STOPPED);

        int totalTasks = cards.size();
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

        GlobalTaskMonitorResponse.RuntimeSnapshot runtimeSnapshot = new GlobalTaskMonitorResponse.RuntimeSnapshot(
                runtimeInputRecords,
                runtimeOutputRecords,
                includedPipelineCount,
                missingPipelineCount);

        GlobalTaskMonitorResponse.Metadata metadata = new GlobalTaskMonitorResponse.Metadata(
                Instant.now(),
                totalTasks,
                skippedLiveMetricsCount);

        return new GlobalTaskMonitorResponse(summary, statusDistribution, cards, runtimeSnapshot, metadata);
    }

    private MonitorCard buildCard(PipelineRuntimeView runtimeView) {
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

        GlobalTaskMonitorResponse.TaskCard card = new GlobalTaskMonitorResponse.TaskCard(
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
                pipeline.getUpdatedAt());

        return new MonitorCard(monitorStatus, card);
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

    private int countByStatus(List<MonitorCard> cards, MonitorStatus status) {
        return (int) cards.stream()
                .filter(card -> card.monitorStatus == status)
                .count();
    }

    private int safeInt(Integer value) {
        return value == null ? 0 : value;
    }

    private long safeLong(Long value) {
        return value == null ? 0L : value;
    }

    private record MonitorCard(MonitorStatus monitorStatus, GlobalTaskMonitorResponse.TaskCard card) {
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
