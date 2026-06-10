package com.streamcraft.service.flink.config;

import com.streamcraft.service.runtime.client.RuntimeTargetValidationGateway;
import com.streamcraft.service.flink.client.CorePreviewClient;
import com.streamcraft.service.flink.client.CoreSubmissionClient;
import com.streamcraft.service.flink.client.FlinkJobControlClient;
import com.streamcraft.service.flink.client.FlinkOverviewClient;
import com.streamcraft.service.flink.client.LocalCorePreviewClient;
import com.streamcraft.service.flink.client.LocalCoreProcessClient;
import com.streamcraft.service.flink.service.MergedRuntimeTargetValidationGateway;
import com.streamcraft.service.flink.service.MergedFlinkJobGateway;
import com.streamcraft.service.pipeline.client.FlinkJobGateway;
import com.streamcraft.service.pipeline.client.FlinkMetricsClient;
import com.streamcraft.service.security.InternalAccessProperties;
import java.net.http.HttpClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
@EnableConfigurationProperties(FlinkGatewayProperties.class)
public class FlinkClientConfiguration {

    @Bean
    RestTemplate flinkRestTemplate(RestTemplateBuilder builder, FlinkGatewayProperties properties) {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(properties.getConnectTimeout())
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(properties.getReadTimeout());
        return builder.requestFactory(() -> requestFactory).build();
    }

    @Bean
    FlinkOverviewClient flinkOverviewClient(RestTemplate flinkRestTemplate) {
        return new FlinkOverviewClient(flinkRestTemplate);
    }

    @Bean
    FlinkJobControlClient flinkJobControlClient(RestTemplate flinkRestTemplate) {
        return new FlinkJobControlClient(flinkRestTemplate);
    }

    @Bean
    FlinkMetricsClient flinkMetricsClient(RestTemplate flinkRestTemplate) {
        return new FlinkMetricsClient(flinkRestTemplate);
    }

    @Bean
    CoreSubmissionClient coreSubmissionClient() {
        return new LocalCoreProcessClient();
    }

    @Bean
    CorePreviewClient corePreviewClient() {
        return new LocalCorePreviewClient();
    }

    @Bean
    FlinkJobGateway flinkJobGateway(
            CoreSubmissionClient coreSubmissionClient,
            CorePreviewClient corePreviewClient,
            FlinkJobControlClient flinkJobControlClient,
            FlinkGatewayProperties properties,
            InternalAccessProperties internalAccessProperties) {
        return new MergedFlinkJobGateway(
                coreSubmissionClient,
                corePreviewClient,
                flinkJobControlClient,
                properties,
                internalAccessProperties);
    }

    @Bean
    RuntimeTargetValidationGateway clusterValidationGateway(FlinkOverviewClient flinkOverviewClient) {
        return new MergedRuntimeTargetValidationGateway(flinkOverviewClient);
    }
}
