package com.streamcraft.service.client;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public abstract class InternalGatewayClientSupport {

    private final RestTemplate restTemplate;
    private final String baseUrl;
    private final String headerName;
    private final String token;

    protected InternalGatewayClientSupport(
            RestTemplate restTemplate,
            String baseUrl,
            String headerName,
            String token) {
        this.restTemplate = restTemplate;
        this.baseUrl = baseUrl;
        this.headerName = headerName;
        this.token = token;
    }

    protected final <T, R> ResponseEntity<R> post(String path, T request, Class<R> responseType) {
        return restTemplate.postForEntity(baseUrl + path, withInternalAuth(request), responseType);
    }

    protected final <R> R requireBody(ResponseEntity<R> response, String failureMessage) {
        if (response.getBody() == null) {
            throw new IllegalStateException(failureMessage);
        }
        return response.getBody();
    }

    private <T> HttpEntity<T> withInternalAuth(T request) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(headerName, token);
        return new HttpEntity<>(request, headers);
    }
}
