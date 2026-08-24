package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.service.FlinkRuntimeTargetService;
import com.streamcraft.service.pipeline.client.FlinkMetricsClient;
import com.streamcraft.service.pipeline.model.NodeMetrics;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineMetrics;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PipelineRuntimeStateSupportTest {

    @Mock
    private PipelineDefinitionService definitionService;

    @Mock
    private FlinkRuntimeTargetService runtimeTargetService;

    @Mock
    private FlinkMetricsClient flinkMetricsClient;

    private PipelineRuntimeStateSupport support;

    @BeforeEach
    void setUp() {
        support = new PipelineRuntimeStateSupport(
                definitionService,
                runtimeTargetService,
                flinkMetricsClient);
    }

    @Test
    void runtimeSnapshotCopiesPipelineAndLoadsMetricsForLiveRunningJob() throws Exception {
        Pipeline storedPipeline = runningPipeline();
        FlinkRuntimeTarget runtimeTarget = target();
        PipelineMetrics metrics = metrics();

        when(flinkMetricsClient.getJobStatus("http://flink:8081", "job-1"))
                .thenReturn(PipelineRunStatus.RUNNING);
        when(definitionService.parseDefinitionNodes(storedPipeline.getDefinitionJson()))
                .thenReturn(new PipelineDefinitionService.DefinitionNodes(
                        List.of("node-1"),
                        Map.of("node-1", "Orders")));
        when(flinkMetricsClient.getJobMetrics(
                "http://flink:8081",
                "job-1",
                List.of("node-1"),
                Map.of("node-1", "Orders")))
                .thenReturn(metrics);

        PipelineRuntimeSnapshot snapshot = support.buildRuntimeSnapshot(storedPipeline, runtimeTarget);

        assertThat(snapshot.pipeline()).isNotSameAs(storedPipeline);
        assertThat(snapshot.pipeline().getLastRunStatus()).isEqualTo(PipelineRunStatus.RUNNING);
        assertThat(snapshot.pipeline().getName()).isEqualTo(storedPipeline.getName());
        assertThat(snapshot.metrics()).isSameAs(metrics);
        assertThat(storedPipeline.getLastRunStatus()).isEqualTo(PipelineRunStatus.RUNNING);
    }

    @Test
    void runtimeSnapshotDoesNotLoadMetricsWhenLiveStatusIsFailed() {
        Pipeline storedPipeline = runningPipeline();
        FlinkRuntimeTarget runtimeTarget = target();

        when(flinkMetricsClient.getJobStatus("http://flink:8081", "job-1"))
                .thenReturn(PipelineRunStatus.FAILED);

        PipelineRuntimeSnapshot snapshot = support.buildRuntimeSnapshot(storedPipeline, runtimeTarget);

        assertThat(snapshot.pipeline().getLastRunStatus()).isEqualTo(PipelineRunStatus.FAILED);
        assertThat(snapshot.metrics()).isNull();
        verify(flinkMetricsClient, never()).getJobMetrics(
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyList(),
                org.mockito.ArgumentMatchers.anyMap());
    }

    @Test
    void missingRuntimeTargetPreservesStoredStatusAndSkipsFlinkLookup() {
        Pipeline storedPipeline = runningPipeline();
        when(runtimeTargetService.findTarget()).thenReturn(Optional.empty());

        Pipeline resolvedPipeline = support.withResolvedRuntimeStatus(storedPipeline);

        assertThat(resolvedPipeline).isNotSameAs(storedPipeline);
        assertThat(resolvedPipeline.getLastRunStatus()).isEqualTo(PipelineRunStatus.RUNNING);
        verifyNoInteractions(flinkMetricsClient);
    }

    private Pipeline runningPipeline() {
        Pipeline pipeline = new Pipeline();
        pipeline.setId(1L);
        pipeline.setName("orders");
        pipeline.setDefinitionJson("{\"nodes\":[]}");
        pipeline.setLastJobId("job-1");
        pipeline.setLastRunStatus(PipelineRunStatus.RUNNING);
        pipeline.setLastSubmittedAt(Instant.parse("2026-01-01T00:00:00Z"));
        return pipeline;
    }

    private FlinkRuntimeTarget target() {
        FlinkRuntimeTarget target = new FlinkRuntimeTarget();
        target.setJobManagerUrl("http://flink:8081");
        return target;
    }

    private PipelineMetrics metrics() {
        PipelineMetrics metrics = new PipelineMetrics();
        metrics.setNodeMetrics(List.of(new NodeMetrics("node-1", "Orders", 3L, 2L)));
        return metrics;
    }
}
