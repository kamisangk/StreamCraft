package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.persistence.PipelineRepository;
import com.streamcraft.service.pipeline.web.PipelinePreviewResponse;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PipelineDefinitionServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void toPreviewResponseUsesDisplayNameBeforeFallbackName() {
        PipelineDefinitionService service = newService(mock(PipelineRepository.class));
        PipelinePreviewExecutionResult result = new PipelinePreviewExecutionResult(List.of(
                new PipelinePreviewExecutionResult.Output("sink-1", List.of("{\"status\":\"ok\"}"))
        ));

        PipelinePreviewResponse response = service.toPreviewResponse("""
                {
                  "nodes": [
                    {
                      "id": "sink-1",
                      "name": "Kafka Sink",
                      "displayName": "订单输出",
                      "type": "SINK",
                      "operator": "KAFKA_SINK",
                      "config": {}
                    }
                  ],
                  "edges": []
                }
                """, result);

        assertThat(response.outputs().get(0).nodeName()).isEqualTo("订单输出");
        assertThat(response.outputs().get(0).records()).containsExactly("{\"status\":\"ok\"}");
    }

    @Test
    void getDefinitionReturnsNormalizedStoredDefinition() {
        PipelineRepository repository = mock(PipelineRepository.class);
        Pipeline storedPipeline = new Pipeline();
        storedPipeline.setDefinitionJson("{ \"nodes\": [], \"edges\": [] }");
        when(repository.findById(7L)).thenReturn(Optional.of(storedPipeline));

        JsonNode definition = newService(repository).getDefinition(7L);

        assertThat(definition.toString()).isEqualTo("{\"nodes\":[],\"edges\":[]}");
    }

    private PipelineDefinitionService newService(PipelineRepository repository) {
        return new PipelineDefinitionService(
                repository,
                objectMapper,
                new PipelineDefinitionValidator(objectMapper),
                new PipelineDefinitionNormalizer(objectMapper),
                UiMessageService.englishFallback());
    }
}
