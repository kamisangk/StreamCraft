package com.streamcraft.service.flink.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamcraft.service.flink.client.CorePreviewClient;
import com.streamcraft.service.flink.client.CorePreviewRequest;
import com.streamcraft.service.flink.client.CoreSubmitRequest;
import com.streamcraft.service.flink.client.FlinkJobControlClient;
import com.streamcraft.service.flink.config.FlinkGatewayProperties;
import com.streamcraft.service.pipeline.client.PreviewFlinkJobResponse;
import com.streamcraft.service.pipeline.client.StopFlinkJobRequest;
import com.streamcraft.service.pipeline.client.StopFlinkJobResponse;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobRequest;
import com.streamcraft.service.pipeline.client.SubmitFlinkJobResponse;
import com.streamcraft.service.security.InternalAccessProperties;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

class MergedFlinkJobGatewayRestOnlyTest {

    @Test
    void constructorDependsOnRestSubmissionPreviewAndControlOnly() {
        assertThat(Arrays.stream(MergedFlinkJobGateway.class.getConstructors())
                .flatMap(constructor -> Arrays.stream(constructor.getParameterTypes()))
                .map(Class::getSimpleName)
                .toList())
                .contains(
                        "CoreSubmissionClient",
                        "CorePreviewClient",
                        "FlinkJobControlClient",
                        "FlinkGatewayProperties",
                        "InternalAccessProperties")
                .doesNotContain(
                        "StandaloneCliSubmissionClient",
                        "YarnSubmissionClient");
    }

    @Test
    void submitsStandaloneJobsThroughCoreSubmissionClient() {
        AtomicReference<CoreSubmitRequest> capturedRequest = new AtomicReference<>();
        FlinkGatewayProperties properties = new FlinkGatewayProperties();
        properties.setCoreJarPath("../core/target/streamcraft-core.jar");
        MergedFlinkJobGateway gateway = new MergedFlinkJobGateway(
                request -> {
                    capturedRequest.set(request);
                    return new SubmitFlinkJobResponse("standalone-job", "standalone");
                },
                new NoopCorePreviewClient(),
                new FlinkJobControlClient(new RestTemplate()),
                properties,
                internalAccessProperties());

        SubmitFlinkJobResponse response = gateway.submit(new SubmitFlinkJobRequest(
                "http://flink:8081",
                "http://service.local/api/pipelines/1/definition",
                false,
                2));

        assertThat(response.jobId()).isEqualTo("standalone-job");
        assertThat(capturedRequest.get()).isNotNull();
        assertThat(capturedRequest.get().clusterBaseUrl()).isEqualTo("http://flink:8081");
        assertThat(capturedRequest.get().coreJarPath()).isEqualTo("../core/target/streamcraft-core.jar");
        assertThat(capturedRequest.get().pipelineDefinitionUrl())
                .isEqualTo("http://service.local/api/pipelines/1/definition");
        assertThat(capturedRequest.get().parallelism()).isEqualTo(2);
        assertThat(capturedRequest.get().definitionAuthToken()).isEqualTo("test-token");
    }

    @Test
    void stopsStandaloneJobsThroughRestJobControlClient() {
        AtomicReference<String> capturedClusterBaseUrl = new AtomicReference<>();
        AtomicReference<String> capturedJobId = new AtomicReference<>();
        MergedFlinkJobGateway gateway = new MergedFlinkJobGateway(
                request -> new SubmitFlinkJobResponse("standalone-job", "standalone"),
                new NoopCorePreviewClient(),
                new CapturingFlinkJobControlClient(capturedClusterBaseUrl, capturedJobId),
                new FlinkGatewayProperties(),
                internalAccessProperties());

        StopFlinkJobResponse response = gateway.stop(new StopFlinkJobRequest(
                "http://flink:8081",
                "0123456789abcdef0123456789abcdef"));

        assertThat(response.jobId()).isEqualTo("0123456789abcdef0123456789abcdef");
        assertThat(response.message()).isEqualTo("rest cancelled");
        assertThat(capturedClusterBaseUrl.get()).isEqualTo("http://flink:8081");
        assertThat(capturedJobId.get()).isEqualTo("0123456789abcdef0123456789abcdef");
    }

    private static InternalAccessProperties internalAccessProperties() {
        return new InternalAccessProperties("test-token", "X-Test-Internal-Token");
    }

    private static final class NoopCorePreviewClient implements CorePreviewClient {

        @Override
        public PreviewFlinkJobResponse preview(CorePreviewRequest request) {
            return new PreviewFlinkJobResponse(java.util.List.of());
        }
    }

    private static final class CapturingFlinkJobControlClient extends FlinkJobControlClient {

        private final AtomicReference<String> capturedClusterBaseUrl;
        private final AtomicReference<String> capturedJobId;

        private CapturingFlinkJobControlClient(
                AtomicReference<String> capturedClusterBaseUrl,
                AtomicReference<String> capturedJobId) {
            super(new RestTemplate());
            this.capturedClusterBaseUrl = capturedClusterBaseUrl;
            this.capturedJobId = capturedJobId;
        }

        @Override
        public StopFlinkJobResponse stopJob(String clusterBaseUrl, String jobId) {
            capturedClusterBaseUrl.set(clusterBaseUrl);
            capturedJobId.set(jobId);
            return new StopFlinkJobResponse(jobId, null, "rest cancelled");
        }
    }
}
