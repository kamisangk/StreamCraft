package com.streamcraft.service.flink.client;

import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class FlinkOverviewClient {

    private final RestTemplate restTemplate;

    public FlinkOverviewClient(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public FlinkOverviewResponse fetchOverview(String jobManagerUrl) {
        String normalizedUrl = jobManagerUrl.endsWith("/")
                ? jobManagerUrl.substring(0, jobManagerUrl.length() - 1)
                : jobManagerUrl;
        ResponseEntity<FlinkOverviewResponse> response = restTemplate.exchange(
                normalizedUrl + "/overview",
                HttpMethod.GET,
                null,
                FlinkOverviewResponse.class);
        return response.getBody();
    }
}
