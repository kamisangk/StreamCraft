package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.model.RuntimeTargetStatus;
import com.streamcraft.service.runtime.model.RuntimeTargetType;
import com.streamcraft.service.runtime.service.FlinkRuntimeTargetService;
import com.streamcraft.service.config.PipelineRuntimeProperties;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.client.FlinkJobGateway;
import com.streamcraft.service.pipeline.client.FlinkMetricsClient;
import com.streamcraft.service.pipeline.client.StopFlinkJobRequest;
import com.streamcraft.service.pipeline.client.StopFlinkJobResponse;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobRequest;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobResponse;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.persistence.PipelineRepository;
import com.streamcraft.service.pipeline.web.RunPipelineRequest;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PipelineServiceRuntimeTargetTypeSubmissionTest {

    @Mock
    private PipelineRepository repository;

    @Mock
    private PipelineDefinitionValidator validator;

    @Mock
    private PipelineDefinitionNormalizer definitionNormalizer;

    @Mock
    private FlinkRuntimeTargetService runtimeTargetService;

    @Mock
    private FlinkMetricsClient flinkMetricsClient;

    @Mock
    private PipelineRuntimeProperties runtimeProperties;

    private AtomicReference<SubmitFlinkJobRequest> capturedRequest;
    private AtomicReference<StopFlinkJobRequest> capturedStopRequest;
    private PipelineService service;

    @BeforeEach
    void setUp() {
        capturedRequest = new AtomicReference<>();
        capturedStopRequest = new AtomicReference<>();
        FlinkJobGateway flinkJobGateway = new CapturingFlinkJobGateway(capturedRequest, capturedStopRequest);
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
    void runPassesStandaloneRestTargetToFlinkGateway() {
        Pipeline pipeline = pipeline();
        FlinkRuntimeTarget standaloneTarget = standaloneTarget();

        when(repository.findById(10L)).thenReturn(Optional.of(pipeline));
        when(runtimeTargetService.requireTarget()).thenReturn(standaloneTarget);
        when(runtimeProperties.testMode()).thenReturn(false);
        when(runtimeProperties.parallelism()).thenReturn(4);
        when(runtimeProperties.serviceBaseUrl()).thenReturn("http://service.local");
        when(repository.save(any(Pipeline.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Pipeline saved = service.run(10L, new RunPipelineRequest(null, null));

        assertThat(capturedRequest.get()).isNotNull();
        assertThat(capturedRequest.get().clusterBaseUrl()).isEqualTo("https://secured-flink:8081");
        assertThat(capturedRequest.get().pipelineDefinitionUrl())
                .isEqualTo("http://service.local/api/pipelines/10/definition");
        assertThat(capturedRequest.get().testMode()).isFalse();
        assertThat(capturedRequest.get().parallelism()).isEqualTo(4);
        assertThat(saved.getLastJobId()).isEqualTo("job-id");
    }

    @Test
    void runUsesRequestParallelismAndTestModeWhenProvided() {
        Pipeline pipeline = pipeline();

        when(repository.findById(10L)).thenReturn(Optional.of(pipeline));
        when(runtimeTargetService.requireTarget()).thenReturn(standaloneTarget());
        when(runtimeProperties.serviceBaseUrl()).thenReturn("http://service.local");
        when(repository.save(any(Pipeline.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.run(10L, new RunPipelineRequest(8, true));

        assertThat(capturedRequest.get()).isNotNull();
        assertThat(capturedRequest.get().testMode()).isTrue();
        assertThat(capturedRequest.get().parallelism()).isEqualTo(8);
    }

    @Test
    void stopPassesStandaloneRestTargetToFlinkGateway() {
        Pipeline pipeline = pipeline();
        pipeline.setLastJobId("0123456789abcdef0123456789abcdef");
        pipeline.setLastRunStatus(PipelineRunStatus.RUNNING);

        when(repository.findById(10L)).thenReturn(Optional.of(pipeline));
        when(runtimeTargetService.requireTarget()).thenReturn(standaloneTarget());
        when(repository.save(any(Pipeline.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.stop(10L);

        assertThat(capturedStopRequest.get()).isNotNull();
        assertThat(capturedStopRequest.get().clusterBaseUrl()).isEqualTo("https://secured-flink:8081");
        assertThat(capturedStopRequest.get().jobId()).isEqualTo("0123456789abcdef0123456789abcdef");
    }

    private static Pipeline pipeline() {
        Pipeline pipeline = new Pipeline();
        pipeline.setId(10L);
        pipeline.setName("orders");
        pipeline.setDefinitionJson("{\"nodes\":[],\"edges\":[]}");
        return pipeline;
    }

    private static FlinkRuntimeTarget standaloneTarget() {
        FlinkRuntimeTarget target = new FlinkRuntimeTarget();
        target.setId(1L);
        target.setType(RuntimeTargetType.FLINK_STANDALONE);
        target.setStatus(RuntimeTargetStatus.CONNECTED);
        target.setJobManagerUrl("https://secured-flink:8081");
        return target;
    }

    private static final class CapturingFlinkJobGateway implements FlinkJobGateway {

        private final AtomicReference<SubmitFlinkJobRequest> capturedRequest;
        private final AtomicReference<StopFlinkJobRequest> capturedStopRequest;

        private CapturingFlinkJobGateway(
                AtomicReference<SubmitFlinkJobRequest> capturedRequest,
                AtomicReference<StopFlinkJobRequest> capturedStopRequest) {
            this.capturedRequest = capturedRequest;
            this.capturedStopRequest = capturedStopRequest;
        }

        @Override
        public SubmitFlinkJobResponse submit(SubmitFlinkJobRequest request) {
            capturedRequest.set(request);
            return new SubmitFlinkJobResponse("job-id", "submitted");
        }

        @Override
        public com.streamcraft.service.pipeline.client.PreviewFlinkJobResponse preview(
                com.streamcraft.service.pipeline.client.PreviewFlinkJobRequest request) {
            throw new UnsupportedOperationException("preview is not used in this test");
        }

        @Override
        public StopFlinkJobResponse stop(StopFlinkJobRequest request) {
            capturedStopRequest.set(request);
            return new StopFlinkJobResponse(request.jobId(), null, "stopped");
        }
    }
}
