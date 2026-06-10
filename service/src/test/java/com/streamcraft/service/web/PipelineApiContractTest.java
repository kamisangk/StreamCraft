package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.service.PipelineDefinitionValidator;
import com.streamcraft.service.pipeline.web.PipelineApiController;
import com.streamcraft.service.pipeline.web.PipelineDetailResponse;
import com.streamcraft.service.pipeline.web.PipelineSummaryResponse;
import com.streamcraft.service.pipeline.web.RunPipelineRequest;
import com.streamcraft.service.pipeline.web.SavePipelineRequest;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.springframework.web.bind.annotation.PostMapping;

class PipelineApiContractTest {

    @Test
    void pipelineApiControllerDoesNotExposePublishEndpoint() {
        assertThat(Arrays.stream(PipelineApiController.class.getDeclaredMethods())
                .map(Method::getName))
                .doesNotContain("publish");

        assertThat(Arrays.stream(PipelineApiController.class.getDeclaredMethods())
                .flatMap(method -> Arrays.stream(method.getAnnotationsByType(PostMapping.class)))
                .flatMap(mapping -> Arrays.stream(mapping.value())))
                .doesNotContain("/{id}/publish");
    }

    @Test
    void pipelineApiControllerExposesPreviewEndpoint() {
        assertThat(Arrays.stream(PipelineApiController.class.getDeclaredMethods())
                .map(Method::getName))
                .contains("preview");

        assertThat(Arrays.stream(PipelineApiController.class.getDeclaredMethods())
                .flatMap(method -> Arrays.stream(method.getAnnotationsByType(PostMapping.class)))
                .flatMap(mapping -> Arrays.stream(mapping.value())))
                .contains("/preview");
    }

    @Test
    void pipelineResponsesDoNotExposeVersionField() {
        assertThat(Arrays.stream(PipelineSummaryResponse.class.getRecordComponents())
                .map(RecordComponent::getName))
                .doesNotContain("version");

        assertThat(Arrays.stream(PipelineDetailResponse.class.getRecordComponents())
                .map(RecordComponent::getName))
                .doesNotContain("version");
    }

    @Test
    void pipelineApiNoLongerExposesPerPipelineClusterBinding() {
        assertThat(Arrays.stream(PipelineSummaryResponse.class.getRecordComponents())
                .map(RecordComponent::getName))
                .doesNotContain("clusterConnectionId");

        assertThat(Arrays.stream(PipelineDetailResponse.class.getRecordComponents())
                .map(RecordComponent::getName))
                .doesNotContain("clusterConnectionId");

        assertThat(Arrays.stream(SavePipelineRequest.class.getRecordComponents())
                .map(RecordComponent::getName))
                .doesNotContain("clusterConnectionId");

        assertThat(Arrays.stream(RunPipelineRequest.class.getRecordComponents())
                .map(RecordComponent::getName))
                .doesNotContain("clusterConnectionId");
    }

    @Test
    void pipelineApiNoLongerExposesYarnRuntimeFields() {
        assertThat(Arrays.stream(PipelineSummaryResponse.class.getRecordComponents())
                .map(RecordComponent::getName))
                .doesNotContain("lastYarnApplicationId");

        assertThat(Arrays.stream(PipelineDetailResponse.class.getRecordComponents())
                .map(RecordComponent::getName))
                .doesNotContain("lastYarnApplicationId");

        assertThat(Arrays.stream(RunPipelineRequest.class.getRecordComponents())
                .map(RecordComponent::getName))
                .containsExactly("parallelism", "testMode");
    }

    @Test
    void pipelineBackendDoesNotKeepLegacyVersionOrPublishTypes() throws Exception {
        assertThat(Arrays.stream(Pipeline.class.getDeclaredFields())
                .map(Field::getName))
                .doesNotContain("version", "status", "clusterConnectionId", "lastYarnApplicationId");

        assertThat(Arrays.stream(PipelineDefinitionValidator.class.getDeclaredMethods())
                .map(Method::getName))
                .doesNotContain("validateForPublish");

        assertThatThrownBy(() -> Class.forName("com.streamcraft.service.pipeline.model.PipelineStatus"))
                .isInstanceOf(ClassNotFoundException.class);
    }
}
