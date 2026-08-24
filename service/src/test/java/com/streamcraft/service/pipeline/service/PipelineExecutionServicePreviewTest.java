package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.streamcraft.service.config.PipelineRuntimeProperties;
import com.streamcraft.service.pipeline.client.FlinkJobGateway;
import com.streamcraft.service.pipeline.client.PreviewFlinkJobOutput;
import com.streamcraft.service.pipeline.client.PreviewFlinkJobRequest;
import com.streamcraft.service.pipeline.client.PreviewFlinkJobResponse;
import com.streamcraft.service.pipeline.persistence.PipelineRepository;
import com.streamcraft.service.pipeline.web.PipelinePreviewOutputResponse;
import com.streamcraft.service.pipeline.web.PipelinePreviewRequest;
import com.streamcraft.service.pipeline.web.PipelinePreviewResponse;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PipelineExecutionServicePreviewTest {

    @Mock
    private PipelineRepository repository;

    @Mock
    private PipelineDefinitionService definitionService;

    @Mock
    private PipelineRuntimeStateSupport runtimeStateSupport;

    @Mock
    private FlinkJobGateway flinkJobGateway;

    @Mock
    private PipelineRuntimeProperties runtimeProperties;

    private PipelineExecutionService service;

    @BeforeEach
    void setUp() {
        service = new PipelineExecutionService(
                repository,
                definitionService,
                runtimeStateSupport,
                flinkJobGateway,
                runtimeProperties);
    }

    @Test
    void previewNormalizesDefinitionCallsGatewayWithSingleParallelismAndMapsResponse() {
        String rawDefinition = "{\"nodes\":[],\"edges\":[]}";
        String normalizedDefinition = "{\"nodes\":[],\"edges\":[],\"pipelineId\":\"draft\"}";
        PipelinePreviewResponse expectedResponse = new PipelinePreviewResponse(List.of(
                new PipelinePreviewOutputResponse("sink-1", "Orders", List.of("{\"ok\":true}"))));

        when(definitionService.normalizeAndValidateForPreview(rawDefinition)).thenReturn(normalizedDefinition);
        when(flinkJobGateway.preview(any(PreviewFlinkJobRequest.class))).thenReturn(
                new PreviewFlinkJobResponse(List.of(
                        new PreviewFlinkJobOutput("sink-1", List.of("{\"ok\":true}")))));
        when(definitionService.toPreviewResponse(
                eq(normalizedDefinition),
                any(PipelinePreviewExecutionResult.class)))
                .thenReturn(expectedResponse);

        PipelinePreviewResponse response = service.preview(
                new PipelinePreviewRequest("Preview", "draft", rawDefinition));

        ArgumentCaptor<PreviewFlinkJobRequest> requestCaptor =
                ArgumentCaptor.forClass(PreviewFlinkJobRequest.class);
        verify(flinkJobGateway).preview(requestCaptor.capture());
        assertThat(requestCaptor.getValue().definitionJson()).isEqualTo(normalizedDefinition);
        assertThat(requestCaptor.getValue().parallelism()).isEqualTo(1);
        assertThat(response).isSameAs(expectedResponse);
    }
}
