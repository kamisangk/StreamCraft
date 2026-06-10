package com.streamcraft.service.flink.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.pipeline.client.PreviewFlinkJobResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class LocalCorePreviewClient implements CorePreviewClient {

    private final ObjectMapper objectMapper;
    private final String javaCommand;
    private final LocalCoreProcessClient.ProcessRunner processRunner;
    private final long timeoutSeconds;

    public LocalCorePreviewClient() {
        this(new ObjectMapper(),
                LocalCoreProcessClient.resolveJavaCommand(),
                new LocalCoreProcessClient.DefaultProcessRunner(),
                LocalCoreProcessClient.PROCESS_TIMEOUT_SECONDS);
    }

    LocalCorePreviewClient(
            ObjectMapper objectMapper,
            String javaCommand,
            LocalCoreProcessClient.ProcessRunner processRunner,
            long timeoutSeconds) {
        this.objectMapper = objectMapper;
        this.javaCommand = javaCommand;
        this.processRunner = processRunner;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    public PreviewFlinkJobResponse preview(CorePreviewRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Preview request is required.");
        }
        if (request.coreJarPath() == null || request.coreJarPath().isBlank()) {
            throw new IllegalArgumentException("Core jar path is required.");
        }
        if (request.definitionJson() == null || request.definitionJson().isBlank()) {
            throw new IllegalArgumentException("Pipeline definition is required.");
        }

        Path definitionFile = createFile("streamcraft-service-preview-definition-", ".json");
        Path outputFile = createOutputFile();
        try {
            Files.writeString(definitionFile, request.definitionJson(), StandardCharsets.UTF_8);

            List<String> command = new ArrayList<>();
            command.add(javaCommand);
            command.add("-jar");
            command.add(request.coreJarPath());
            command.add("--file");
            command.add(definitionFile.toString());
            command.add("--execution-mode");
            command.add("PREVIEW");
            command.add("--preview-output-file");
            command.add(outputFile.toString());
            command.add("--parallelism");
            command.add(String.valueOf(request.parallelism() == null || request.parallelism() < 1 ? 1 : request.parallelism()));

            LocalCoreProcessClient.ProcessExecutionResult result = processRunner.execute(command, timeoutSeconds);
            if (result.exitCode() != 0) {
                throw new IllegalStateException("Core preview process exited with code "
                        + result.exitCode() + ". Output: " + result.output());
            }

            return objectMapper.readValue(Files.readString(outputFile, StandardCharsets.UTF_8), PreviewFlinkJobResponse.class);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to execute local core preview process.", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for local core preview process.", exception);
        } finally {
            try {
                Files.deleteIfExists(definitionFile);
            } catch (IOException ignored) {
            }
            try {
                Files.deleteIfExists(outputFile);
            } catch (IOException ignored) {
            }
        }
    }

    private Path createOutputFile() {
        return createFile("streamcraft-service-preview-output-", ".json");
    }

    private Path createFile(String prefix, String suffix) {
        try {
            return Files.createTempFile(prefix, suffix);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create preview temp file.", exception);
        }
    }
}
