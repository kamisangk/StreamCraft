package com.streamcraft.service.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamcraft.service.runtime.client.RuntimeTargetValidationGateway;
import com.streamcraft.service.flink.config.FlinkClientConfiguration;
import com.streamcraft.service.flink.config.FlinkGatewayProperties;
import com.streamcraft.service.pipeline.client.FlinkJobGateway;
import com.streamcraft.service.security.InternalAccessProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.web.client.RestTemplateBuilder;

class MergedFlinkGatewayConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of())
            .withBean(RestTemplateBuilder.class, RestTemplateBuilder::new)
            .withUserConfiguration(TestConfig.class, FlinkClientConfiguration.class)
            .withPropertyValues(
                    "streamcraft.flink.core-jar-path=../core/target/streamcraft-core.jar",
                    "streamcraft.flink.connect-timeout=2s",
                    "streamcraft.flink.read-timeout=3s",
                    "streamcraft.internal.token=test-internal-token",
                    "streamcraft.internal.header-name=X-Test-Internal-Token");

    @Test
    void mergedGatewayConfigExposesInProcessGatewayBeans() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(FlinkJobGateway.class);
            assertThat(context).hasSingleBean(RuntimeTargetValidationGateway.class);
            assertThat(context).hasSingleBean(FlinkGatewayProperties.class);
        });
    }

    @EnableConfigurationProperties({FlinkGatewayProperties.class, InternalAccessProperties.class})
    static class TestConfig {
    }
}
