package com.streamcraft.service.pipeline.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.config.PipelineRuntimeProperties;
import com.streamcraft.service.pipeline.web.PipelinePreviewResponse;
import java.util.List;
import org.junit.jupiter.api.Test;

class PipelinePreviewResponseMappingTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void toPreviewResponseUsesDisplayNameBeforeFallbackName() {
        PipelineService pipelineService = new PipelineService(
                null,
                objectMapper,
                new PipelineDefinitionValidator(objectMapper),
                new PipelineDefinitionNormalizer(objectMapper),
                null,
                null,
                null,
                new PipelineRuntimeProperties(
                        "http://localhost:8080",
                        false,
                        1));

        PipelinePreviewExecutionResult result = new PipelinePreviewExecutionResult(List.of(
                new PipelinePreviewExecutionResult.Output("sink-1", List.of("{\"status\":\"ok\"}"))
        ));

        PipelinePreviewResponse response = pipelineService.toPreviewResponse("""
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
}
