package com.streamcraft.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.cli.CoreCommandLineOptions;
import com.streamcraft.core.model.PipelineDefinition;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

public class PipelineDefinitionLoader {

    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    public PipelineDefinitionLoader() {
        this(new ObjectMapper(), HttpClient.newHttpClient());
    }

    public PipelineDefinitionLoader(ObjectMapper objectMapper, HttpClient httpClient) {
        this.objectMapper = objectMapper;
        this.httpClient = httpClient;
    }

    public PipelineDefinition load(CoreCommandLineOptions options) throws IOException, InterruptedException {
        String raw = hasText(options.url())
                ? readFromUrl(options.url(), options.definitionAuthToken())
                : readFromFile(options.file());
        return objectMapper.readValue(raw, PipelineDefinition.class);
    }

    private String readFromUrl(String url, String definitionAuthToken) throws IOException, InterruptedException {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(URI.create(url)).GET();
        if (hasText(definitionAuthToken)) {
            requestBuilder.header("X-StreamCraft-Internal-Token", definitionAuthToken);
        }
        HttpRequest request = requestBuilder.build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("Failed to load pipeline definition from url: " + url);
        }
        return response.body();
    }

    private String readFromFile(String file) throws IOException {
        return Files.readString(Path.of(file));
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
