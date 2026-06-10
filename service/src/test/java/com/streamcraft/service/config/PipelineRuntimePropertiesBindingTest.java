package com.streamcraft.service.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class PipelineRuntimePropertiesBindingTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class);

    @Test
    void bindsDefaultRuntimePropertiesInSpringContext() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(PipelineRuntimeProperties.class);

            PipelineRuntimeProperties properties = context.getBean(PipelineRuntimeProperties.class);
            assertThat(properties.serviceBaseUrl()).isEqualTo("http://localhost:8080");
            assertThat(properties.testMode()).isFalse();
            assertThat(properties.parallelism()).isEqualTo(1);
        });
    }

    @Test
    void bindsRuntimePropertyOverrides() {
        contextRunner
                .withPropertyValues(
                        "streamcraft.pipeline.runtime.service-base-url=http://service.internal",
                        "streamcraft.pipeline.runtime.test-mode=true",
                        "streamcraft.pipeline.runtime.parallelism=4")
                .run(context -> {
                    assertThat(context).hasSingleBean(PipelineRuntimeProperties.class);

                    PipelineRuntimeProperties properties = context.getBean(PipelineRuntimeProperties.class);
                    assertThat(properties.serviceBaseUrl()).isEqualTo("http://service.internal");
                    assertThat(properties.testMode()).isTrue();
                    assertThat(properties.parallelism()).isEqualTo(4);
                });
    }

    @Test
    void runtimePropertiesDoNotExposeYarnResourceConfiguration() {
        assertThat(Arrays.stream(PipelineRuntimeProperties.class.getRecordComponents())
                .map(RecordComponent::getName))
                .containsExactly("serviceBaseUrl", "testMode", "parallelism");

        assertThat(Arrays.stream(PipelineRuntimeProperties.class.getDeclaredClasses())
                .map(Class::getSimpleName))
                .doesNotContain("Yarn");
    }

    @EnableConfigurationProperties(PipelineRuntimeProperties.class)
    static class TestConfig {
    }
}
