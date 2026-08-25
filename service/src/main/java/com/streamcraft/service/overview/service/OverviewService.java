package com.streamcraft.service.overview.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.runtime.model.RuntimeTargetStatus;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.service.FlinkRuntimeTargetService;
import com.streamcraft.service.overview.web.OverviewResponse;
import com.streamcraft.service.pipeline.service.PipelineRuntimeQueryService;
import com.streamcraft.service.pipeline.service.PipelineRuntimeSnapshot;
import com.streamcraft.service.pipeline.service.PipelineRuntimeView;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class OverviewService {

    private final FlinkRuntimeTargetService runtimeTargetService;
    private final PipelineRuntimeQueryService pipelineRuntimeQueryService;
    private final OverviewPipelineRowMapper pipelineRowMapper;

    public OverviewService(
            FlinkRuntimeTargetService runtimeTargetService,
            PipelineRuntimeQueryService pipelineRuntimeQueryService,
            ObjectMapper objectMapper) {
        this.runtimeTargetService = runtimeTargetService;
        this.pipelineRuntimeQueryService = pipelineRuntimeQueryService;
        this.pipelineRowMapper = new OverviewPipelineRowMapper(objectMapper);
    }

    public OverviewResponse getOverview() {
        FlinkRuntimeTarget runtimeTarget = runtimeTargetService.findTarget().orElse(null);
        List<PipelineRuntimeSnapshot> pipelineRuntimeSnapshots = pipelineRuntimeQueryService.listRuntimeSnapshots();
        OverviewPipelineStatistics statistics = new OverviewPipelineStatistics();
        List<OverviewResponse.PipelineRow> pipelineRows = buildPipelineRows(
                pipelineRuntimeSnapshots,
                runtimeTarget,
                statistics);
        OverviewPipelineStatistics.OverviewStatisticsSnapshot statisticsSnapshot = statistics.toSnapshot();

        OverviewResponse.RuntimeSnapshot overviewRuntimeSnapshot = new OverviewResponse.RuntimeSnapshot(
                statisticsSnapshot.totalInputRecords(),
                statisticsSnapshot.totalOutputRecords(),
                statisticsSnapshot.includedPipelineCount(),
                statisticsSnapshot.missingPipelineCount());
        OverviewResponse.Summary summary = new OverviewResponse.Summary(
                totalRuntimeTargets(runtimeTarget),
                connectedRuntimeTargets(runtimeTarget),
                statisticsSnapshot.totalPipelineCount(),
                statisticsSnapshot.runningPipelineCount(),
                statisticsSnapshot.stoppedPipelineCount(),
                statisticsSnapshot.unhealthyPipelineCount(),
                statisticsSnapshot.latestSubmittedAt());

        return new OverviewResponse(
                summary,
                overviewRuntimeSnapshot,
                runtimeTargetCapacities(runtimeTarget),
                pipelineRows);
    }

    private List<OverviewResponse.PipelineRow> buildPipelineRows(
            List<PipelineRuntimeSnapshot> pipelineRuntimeSnapshots,
            FlinkRuntimeTarget runtimeTarget,
            OverviewPipelineStatistics statistics) {
        List<OverviewResponse.PipelineRow> pipelineRows = new ArrayList<>();
        for (PipelineRuntimeSnapshot pipelineSnapshot : pipelineRuntimeSnapshots) {
            PipelineRuntimeView runtimeView = PipelineRuntimeView.of(pipelineSnapshot, runtimeTarget);
            statistics.include(runtimeView);
            pipelineRows.add(pipelineRowMapper.toResponse(runtimeView));
        }
        pipelineRows.sort(Comparator.comparing(
                        OverviewResponse.PipelineRow::updatedAt,
                        Comparator.nullsLast(Comparator.reverseOrder()))
                .thenComparing(OverviewResponse.PipelineRow::pipelineId, Comparator.nullsLast(Comparator.naturalOrder())));
        return pipelineRows;
    }

    private int totalRuntimeTargets(FlinkRuntimeTarget runtimeTarget) {
        return runtimeTarget == null ? 0 : 1;
    }

    private int connectedRuntimeTargets(FlinkRuntimeTarget runtimeTarget) {
        return runtimeTarget != null && runtimeTarget.getStatus() == RuntimeTargetStatus.CONNECTED ? 1 : 0;
    }

    private List<OverviewResponse.RuntimeTargetCapacity> runtimeTargetCapacities(
            FlinkRuntimeTarget runtimeTarget) {
        return runtimeTarget == null
                ? List.of()
                : List.of(toCapacity(runtimeTarget));
    }

    private OverviewResponse.RuntimeTargetCapacity toCapacity(FlinkRuntimeTarget runtimeTarget) {
        int totalSlots = runtimeTarget.getTotalSlots() == null ? 0 : runtimeTarget.getTotalSlots();
        int availableSlots = runtimeTarget.getAvailableSlots() == null ? 0 : runtimeTarget.getAvailableSlots();
        int usedSlots = totalSlots - availableSlots;
        Integer usagePercent = null;
        if (runtimeTarget.getStatus() == RuntimeTargetStatus.CONNECTED) {
            usagePercent = totalSlots == 0 ? 0 : (int) Math.round((usedSlots * 100.0) / totalSlots);
        }

        return new OverviewResponse.RuntimeTargetCapacity(
                runtimeTarget.getId(),
                runtimeTarget.getType() == null ? "Flink runtime" : runtimeTarget.getType().name(),
                runtimeTarget.getStatus() == null ? null : runtimeTarget.getStatus().name(),
                totalSlots,
                availableSlots,
                usedSlots,
                usagePercent,
                runtimeTarget.getStatusMessage());
    }
}
