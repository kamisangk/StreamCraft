package com.streamcraft.service.pipeline.monitor.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.streamcraft.service.runtime.model.RuntimeTargetStatus;
import com.streamcraft.service.runtime.model.RuntimeTargetType;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.service.FlinkRuntimeTargetService;
import com.streamcraft.service.pipeline.model.NodeMetrics;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineMetrics;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.monitor.web.GlobalTaskMonitorResponse;
import com.streamcraft.service.pipeline.service.PipelineRuntimeSnapshot;
import com.streamcraft.service.pipeline.service.PipelineService;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GlobalTaskMonitorServiceTest {

    @Mock
    private PipelineService pipelineService;

    @Mock
    private FlinkRuntimeTargetService runtimeTargetService;

    @InjectMocks
    private GlobalTaskMonitorService service;

    @Test
    void monitorSummaryUsesSingletonRuntimeTargetSlotSnapshotWithoutRevalidation() {
        FlinkRuntimeTarget target = target(12, 11);

        when(pipelineService.listRuntimeSnapshots()).thenReturn(List.of());
        when(runtimeTargetService.findTarget()).thenReturn(Optional.of(target));

        GlobalTaskMonitorResponse response = service.getMonitor();

        assertThat(response.summary().totalSlots()).isEqualTo(12);
        assertThat(response.summary().availableSlots()).isEqualTo(11);
        assertThat(response.summary().usedSlots()).isEqualTo(1);
        assertThat(response.summary().totalRuntimeTargets()).isEqualTo(1);
        assertThat(response.summary().connectedRuntimeTargets()).isEqualTo(1);
        verify(runtimeTargetService, never()).revalidate();
        verify(pipelineService, never()).list();
        verify(pipelineService, never()).getMetrics(anyLong());
    }

    @Test
    void monitorSummaryReturnsRuntimeSnapshotForRunningPipelinesOnly() {
        Pipeline runningWithMetricsOne = pipeline(101L, "running-1", PipelineRunStatus.RUNNING, "job-101");
        Pipeline runningWithMetricsTwo = pipeline(102L, "running-2", PipelineRunStatus.RUNNING, "job-102");
        Pipeline runningWithoutMetrics = pipeline(103L, "running-no-metrics", PipelineRunStatus.RUNNING, "job-103");
        Pipeline failed = pipeline(104L, "failed", PipelineRunStatus.FAILED, "job-104");

        when(runtimeTargetService.findTarget()).thenReturn(Optional.of(target(12, 11)));
        when(pipelineService.listRuntimeSnapshots()).thenReturn(List.of(
                snapshot(runningWithMetricsOne, metrics(List.of(
                        new NodeMetrics("n-1", "node-1", 10L, 7L),
                        new NodeMetrics("n-2", "node-2", 4L, 6L)))),
                snapshot(runningWithMetricsTwo, metrics(List.of(
                        new NodeMetrics("n-3", "node-3", 5L, 8L)))),
                snapshot(runningWithoutMetrics, null),
                snapshot(failed, null)));

        GlobalTaskMonitorResponse response = service.getMonitor();

        assertThat(response.runtimeSnapshot().totalInputRecords()).isEqualTo(19L);
        assertThat(response.runtimeSnapshot().totalOutputRecords()).isEqualTo(21L);
        assertThat(response.runtimeSnapshot().includedPipelineCount()).isEqualTo(2);
        assertThat(response.runtimeSnapshot().missingPipelineCount()).isEqualTo(1);
        assertThat(response.metadata().skippedLiveMetricsCount()).isEqualTo(1);
        verify(pipelineService, never()).list();
        verify(pipelineService, never()).getMetrics(anyLong());
    }

    @Test
    void monitorSummaryUsesZeroTotalsWhenMetricsHasNoNodeMetrics() {
        Pipeline runningWithMetrics = pipeline(201L, "running-1", PipelineRunStatus.RUNNING, "job-201");
        PipelineMetrics metricsWithoutNodes = new PipelineMetrics();
        metricsWithoutNodes.setNodeMetrics(null);

        when(runtimeTargetService.findTarget()).thenReturn(Optional.of(target(12, 11)));
        when(pipelineService.listRuntimeSnapshots()).thenReturn(List.of(snapshot(runningWithMetrics, metricsWithoutNodes)));

        GlobalTaskMonitorResponse response = service.getMonitor();

        assertThat(response.cards()).singleElement().satisfies(card -> {
            assertThat(card.metricsAvailable()).isTrue();
            assertThat(card.totalInputRecords()).isZero();
            assertThat(card.totalOutputRecords()).isZero();
        });
        assertThat(response.runtimeSnapshot().totalInputRecords()).isZero();
        assertThat(response.runtimeSnapshot().totalOutputRecords()).isZero();
        assertThat(response.runtimeSnapshot().includedPipelineCount()).isEqualTo(1);
        assertThat(response.runtimeSnapshot().missingPipelineCount()).isZero();
        verify(pipelineService, never()).list();
        verify(pipelineService, never()).getMetrics(anyLong());
    }

    private FlinkRuntimeTarget target(int totalSlots, int availableSlots) {
        FlinkRuntimeTarget target = new FlinkRuntimeTarget();
        target.setId(1L);
        target.setType(RuntimeTargetType.FLINK_STANDALONE);
        target.setStatus(RuntimeTargetStatus.CONNECTED);
        target.setTotalSlots(totalSlots);
        target.setAvailableSlots(availableSlots);
        target.setJobManagerUrl("http://192.168.217.132:8081/");
        return target;
    }

    private Pipeline pipeline(
            Long id,
            String name,
            PipelineRunStatus runStatus,
            String jobId) {
        Pipeline pipeline = new Pipeline();
        pipeline.setId(id);
        pipeline.setName(name);
        pipeline.setDefinitionJson("{}");
        pipeline.setLastRunStatus(runStatus);
        pipeline.setLastJobId(jobId);
        pipeline.setUpdatedAt(Instant.parse("2026-01-01T00:00:00Z"));
        return pipeline;
    }

    private PipelineMetrics metrics(List<NodeMetrics> nodeMetrics) {
        PipelineMetrics metrics = new PipelineMetrics();
        metrics.setNodeMetrics(nodeMetrics);
        return metrics;
    }

    private PipelineRuntimeSnapshot snapshot(Pipeline pipeline, PipelineMetrics metrics) {
        return new PipelineRuntimeSnapshot(pipeline, metrics);
    }
}
