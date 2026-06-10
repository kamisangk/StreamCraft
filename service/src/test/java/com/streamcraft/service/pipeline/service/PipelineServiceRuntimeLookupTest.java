package com.streamcraft.service.pipeline.service;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.runtime.model.RuntimeTargetType;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.service.FlinkRuntimeTargetService;
import com.streamcraft.service.config.PipelineRuntimeProperties;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.client.FlinkJobGateway;
import com.streamcraft.service.pipeline.client.FlinkMetricsClient;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.persistence.PipelineRepository;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PipelineServiceRuntimeLookupTest {

    @Mock
    private PipelineRepository repository;

    @Mock
    private PipelineDefinitionValidator validator;

    @Mock
    private PipelineDefinitionNormalizer definitionNormalizer;

    @Mock
    private FlinkRuntimeTargetService runtimeTargetService;

    @Mock
    private FlinkJobGateway flinkJobGateway;

    @Mock
    private FlinkMetricsClient flinkMetricsClient;

    @Mock
    private PipelineRuntimeProperties runtimeProperties;

    private PipelineService service;

    @BeforeEach
    void setUp() {
        service = new PipelineService(
                repository,
                new ObjectMapper(),
                validator,
                definitionNormalizer,
                runtimeTargetService,
                flinkJobGateway,
                flinkMetricsClient,
                runtimeProperties,
                UiMessageService.englishFallback());
    }

    @Test
    void listUsesSingletonRuntimeTargetForAllRunningStatusLookups() {
        Pipeline first = runningPipeline(10L, "job-1");
        Pipeline second = runningPipeline(11L, "job-2");
        FlinkRuntimeTarget target = target("http://flink-one");

        when(repository.findAllByOrderByUpdatedAtDesc()).thenReturn(List.of(first, second));
        when(runtimeTargetService.findTarget()).thenReturn(Optional.of(target));
        when(flinkMetricsClient.getJobStatus("http://flink-one", "job-1")).thenReturn(PipelineRunStatus.RUNNING);
        when(flinkMetricsClient.getJobStatus("http://flink-one", "job-2")).thenReturn(PipelineRunStatus.RUNNING);

        service.list();

        verify(runtimeTargetService).findTarget();
        verify(flinkMetricsClient).getJobStatus("http://flink-one", "job-1");
        verify(flinkMetricsClient).getJobStatus("http://flink-one", "job-2");
    }

    @Test
    void listRunningPipelinesOnlyLoadsStoredRunningPipelines() {
        Pipeline running = runningPipeline(10L, "job-1");

        when(repository.findByLastRunStatus(PipelineRunStatus.RUNNING)).thenReturn(List.of(running));
        when(runtimeTargetService.findTarget()).thenReturn(Optional.of(target("http://flink-one")));
        when(flinkMetricsClient.getJobStatus("http://flink-one", "job-1")).thenReturn(PipelineRunStatus.RUNNING);

        service.listRunningPipelines();

        verify(repository).findByLastRunStatus(PipelineRunStatus.RUNNING);
        verify(repository, never()).findAllByOrderByUpdatedAtDesc();
    }

    @Test
    void listRunningPipelinesFiltersOutCandidatesNoLongerRunningAtRuntime() {
        Pipeline running = runningPipeline(10L, "job-1");

        when(repository.findByLastRunStatus(PipelineRunStatus.RUNNING)).thenReturn(List.of(running));
        when(runtimeTargetService.findTarget()).thenReturn(Optional.of(target("http://flink-one")));
        when(flinkMetricsClient.getJobStatus("http://flink-one", "job-1")).thenReturn(PipelineRunStatus.FAILED);

        org.assertj.core.api.Assertions.assertThat(service.listRunningPipelines()).isEmpty();
    }

    @Test
    void runtimeLookupSkipsMetricsClientWhenTargetIsNotConfigured() {
        Pipeline running = runningPipeline(10L, "job-1");

        when(repository.findAllByOrderByUpdatedAtDesc()).thenReturn(List.of(running));
        when(runtimeTargetService.findTarget()).thenReturn(Optional.empty());

        service.list();

        verifyNoInteractions(flinkMetricsClient);
        verify(repository, never()).findById(anyLong());
    }

    private static Pipeline runningPipeline(Long id, String jobId) {
        Pipeline pipeline = new Pipeline();
        pipeline.setId(id);
        pipeline.setName("pipeline-" + id);
        pipeline.setLastJobId(jobId);
        pipeline.setLastRunStatus(PipelineRunStatus.RUNNING);
        return pipeline;
    }

    private static FlinkRuntimeTarget target(String jobManagerUrl) {
        FlinkRuntimeTarget target = new FlinkRuntimeTarget();
        target.setId(1L);
        target.setType(RuntimeTargetType.FLINK_STANDALONE);
        target.setJobManagerUrl(jobManagerUrl);
        return target;
    }
}
