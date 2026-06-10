package com.streamcraft.core.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class FlinkClusterConnectionResolver {

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public FlinkClusterConnectionResolver() {
        this(HttpClient.newBuilder()
                .connectTimeout(REQUEST_TIMEOUT)
                .build(), new ObjectMapper());
    }

    FlinkClusterConnectionResolver(HttpClient httpClient, ObjectMapper objectMapper) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }

    public ResolvedFlinkEndpoint resolve(String clusterBaseUrl) {
        String normalizedBaseUrl = normalizeBaseUrl(clusterBaseUrl);
        URI clusterUri = URI.create(normalizedBaseUrl);

        HttpRequest request = HttpRequest.newBuilder(URI.create(normalizedBaseUrl + "/jobmanager/config"))
                .GET()
                .timeout(REQUEST_TIMEOUT)
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("Unable to read Flink jobmanager/config, status: " + response.statusCode());
            }

            JsonNode configItems = objectMapper.readTree(response.body());
            String host = findValue(configItems, "jobmanager.rpc.address");
            String portValue = findValue(configItems, "jobmanager.rpc.port");
            if (host == null || host.isBlank()) {
                host = clusterUri.getHost();
            }
            if (host == null || host.isBlank()) {
                throw new IllegalStateException("Flink jobmanager.rpc.address is missing.");
            }
            if (portValue == null || portValue.isBlank()) {
                throw new IllegalStateException("Flink jobmanager.rpc.port is missing.");
            }

            return new ResolvedFlinkEndpoint(host, Integer.parseInt(portValue));
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to parse Flink jobmanager/config response.", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while resolving Flink jobmanager endpoint.", exception);
        } catch (NumberFormatException exception) {
            throw new IllegalStateException("Invalid Flink jobmanager.rpc.port value.", exception);
        }
    }

    private String normalizeBaseUrl(String clusterBaseUrl) {
        if (clusterBaseUrl == null || clusterBaseUrl.isBlank()) {
            throw new IllegalArgumentException("--cluster-base-url is required for remote submission.");
        }
        return clusterBaseUrl.endsWith("/")
                ? clusterBaseUrl.substring(0, clusterBaseUrl.length() - 1)
                : clusterBaseUrl;
    }

    private String findValue(JsonNode configItems, String key) {
        for (JsonNode item : configItems) {
            if (key.equals(item.path("key").asText())) {
                return item.path("value").asText();
            }
        }
        return null;
    }

    public record ResolvedFlinkEndpoint(String host, int port) {
    }
}
