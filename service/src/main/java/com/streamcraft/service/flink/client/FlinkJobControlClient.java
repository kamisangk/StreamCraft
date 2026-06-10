package com.streamcraft.service.flink.client;

import com.streamcraft.service.pipeline.client.StopFlinkJobResponse;
import java.net.URI;
import java.util.Map;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

public class FlinkJobControlClient {

    private final RestTemplate restTemplate;

    public FlinkJobControlClient(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public StopFlinkJobResponse stopJob(String clusterBaseUrl, String jobId) {
        URI cancelUri = UriComponentsBuilder
                .fromUriString(normalizeBaseUrl(clusterBaseUrl) + "/jobs/" + jobId)
                .queryParam("mode", "cancel")
                .build(true)
                .toUri();

        restTemplate.exchange(
                cancelUri,
                HttpMethod.PATCH,
                new HttpEntity<>(Map.of()),
                Void.class);

        return new StopFlinkJobResponse(jobId, null, "Flink cancel request accepted.");
    }

    private String normalizeBaseUrl(String clusterBaseUrl) {
        return clusterBaseUrl.endsWith("/")
                ? clusterBaseUrl.substring(0, clusterBaseUrl.length() - 1)
                : clusterBaseUrl;
    }
}
