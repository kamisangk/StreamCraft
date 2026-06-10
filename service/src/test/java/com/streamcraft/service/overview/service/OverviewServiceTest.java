package com.streamcraft.service.overview.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.runtime.model.RuntimeTargetStatus;
import com.streamcraft.service.runtime.model.RuntimeTargetType;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.service.FlinkRuntimeTargetService;
import com.streamcraft.service.overview.web.OverviewResponse;
import com.streamcraft.service.pipeline.model.NodeMetrics;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineMetrics;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.service.PipelineRuntimeSnapshot;
import com.streamcraft.service.pipeline.service.PipelineService;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OverviewServiceTest {

    @Mock
    private FlinkRuntimeTargetService runtimeTargetService;

    @Mock
    private PipelineService pipelineService;

    private OverviewService service;

    @BeforeEach
    void setUp() {
        service = new OverviewService(runtimeTargetService, pipelineService, new ObjectMapper());
    }

    @Test
    void overviewUsesRuntimeSnapshotsAndSingletonTargetWithoutPerPipelineMetricLookups() {
        Pipeline runningWithMetrics = pipeline(101L, "running-1", PipelineRunStatus.RUNNING, "job-101");
        Pipeline runningWithoutMetrics = pipeline(102L, "running-2", PipelineRunStatus.RUNNING, "job-102");
        Pipeline failed = pipeline(103L, "failed", PipelineRunStatus.FAILED, "job-103");

        when(runtimeTargetService.findTarget()).thenReturn(Optional.of(target(12, 10)));
        when(pipelineService.listRuntimeSnapshots()).thenReturn(List.of(
                snapshot(runningWithMetrics, metrics(List.of(
                        new NodeMetrics("n-1", "node-1", 10L, 8L),
                        new NodeMetrics("n-2", "node-2", 5L, 4L)))),
                snapshot(runningWithoutMetrics, null),
                snapshot(failed, null)));

        OverviewResponse response = service.getOverview();

        assertThat(response.runtimeSnapshot().totalInputRecords()).isEqualTo(15L);
        assertThat(response.runtimeSnapshot().totalOutputRecords()).isEqualTo(12L);
        assertThat(response.runtimeSnapshot().includedPipelineCount()).isEqualTo(1);
        assertThat(response.runtimeSnapshot().missingPipelineCount()).isEqualTo(1);
        assertThat(response.summary().totalRuntimeTargets()).isEqualTo(1);
        assertThat(response.summary().connectedRuntimeTargets()).isEqualTo(1);
        assertThat(response.summary().runningPipelines()).isEqualTo(2);
        assertThat(response.summary().unhealthyPipelines()).isEqualTo(2);
        assertThat(response.runtimeTargetCapacities()).singleElement().satisfies(capacity -> {
            assertThat(capacity.totalSlots()).isEqualTo(12);
            assertThat(capacity.availableSlots()).isEqualTo(10);
        });
        assertThat(response.pipelines()).extracting(OverviewResponse.PipelineRow::pipelineId)
                .containsExactly(101L, 102L, 103L);
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
        target.setStatusMessage("ok");
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
        pipeline.setDefinitionJson("""
                {
                  "nodes": []
                }
                """);
        pipeline.setLastRunStatus(runStatus);
        pipeline.setLastJobId(jobId);
        pipeline.setUpdatedAt(Instant.parse("2026-01-01T00:00:00Z"));
        return pipeline;
    }

    private PipelineMetrics metrics(List<NodeMetrics> nodeMetrics) {
        PipelineMetrics metrics = new PipelineMetrics();
        metrics.setNodeMetrics(nodeMetrics);
        metrics.setDuration(12_000L);
        return metrics;
    }

    private PipelineRuntimeSnapshot snapshot(Pipeline pipeline, PipelineMetrics metrics) {
        return new PipelineRuntimeSnapshot(pipeline, metrics);
    }
}
