package com.streamcraft.service.flink.client;

import com.streamcraft.service.pipeline.client.SubmitFlinkJobResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class LocalCoreProcessClient implements CoreSubmissionClient {

    static final long PROCESS_TIMEOUT_SECONDS = 60L;
    private static final String JOB_ID_MARKER = "STREAMCRAFT_JOB_ID=";

    private final String javaCommand;
    private final ProcessRunner processRunner;

    public LocalCoreProcessClient() {
        this(resolveJavaCommand(), new DefaultProcessRunner());
    }

    LocalCoreProcessClient(String javaCommand, ProcessRunner processRunner) {
        this.javaCommand = javaCommand;
        this.processRunner = processRunner;
    }

    @Override
    public SubmitFlinkJobResponse submit(CoreSubmitRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Submit request is required.");
        }
        if (request.coreJarPath() == null || request.coreJarPath().isBlank()) {
            throw new IllegalArgumentException("Core jar path is required.");
        }
        if (request.clusterBaseUrl() == null || request.clusterBaseUrl().isBlank()) {
            throw new IllegalArgumentException("Cluster base URL is required.");
        }
        if (request.pipelineDefinitionUrl() == null || request.pipelineDefinitionUrl().isBlank()) {
            throw new IllegalArgumentException("Pipeline definition URL is required.");
        }

        List<String> command = new ArrayList<>();
        command.add(javaCommand);
        command.add("-jar");
        command.add(request.coreJarPath());
        command.add("--url");
        command.add(request.pipelineDefinitionUrl());
        command.add("--test-mode");
        command.add(String.valueOf(request.testMode()));
        if (request.definitionAuthToken() != null && !request.definitionAuthToken().isBlank()) {
            command.add("--definition-auth-token");
            command.add(request.definitionAuthToken());
        }
        command.add("--cluster-base-url");
        command.add(request.clusterBaseUrl());
        command.add("--parallelism");
        command.add(String.valueOf(request.parallelism() == null || request.parallelism() < 1 ? 1 : request.parallelism()));
        command.add("--submit-only");
        command.add("true");
        command.add("--ship-jar");
        command.add(request.coreJarPath());

        try {
            ProcessExecutionResult result = processRunner.execute(command);
            if (result.exitCode() != 0) {
                throw new IllegalStateException("Core submission process exited with code "
                        + result.exitCode() + ". Output: " + result.output());
            }

            String jobId = extractJobId(result.output());
            if (jobId == null || jobId.isBlank()) {
                throw new IllegalStateException("Core submission process did not return " + JOB_ID_MARKER + " in output.");
            }
            return new SubmitFlinkJobResponse(jobId, "Flink job submitted via local core client.");
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to start local core submission process.", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for local core submission process.", exception);
        }
    }

    private String extractJobId(String output) {
        for (String line : output.split("\\R")) {
            if (line.startsWith(JOB_ID_MARKER)) {
                return line.substring(JOB_ID_MARKER.length()).trim();
            }
        }
        return null;
    }

    static String resolveJavaCommand() {
        Path javaHome = Path.of(System.getProperty("java.home"), "bin", isWindows() ? "java.exe" : "java");
        return javaHome.toString();
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().contains("win");
    }

    interface ProcessRunner {
        ProcessExecutionResult execute(List<String> command) throws IOException, InterruptedException;

        default ProcessExecutionResult execute(List<String> command, long timeoutSeconds)
                throws IOException, InterruptedException {
            return execute(command);
        }
    }

    record ProcessExecutionResult(int exitCode, String output) {
    }

    static final class DefaultProcessRunner implements ProcessRunner {

        @Override
        public ProcessExecutionResult execute(List<String> command) throws IOException, InterruptedException {
            return execute(command, PROCESS_TIMEOUT_SECONDS);
        }

        @Override
        public ProcessExecutionResult execute(List<String> command, long timeoutSeconds) throws IOException, InterruptedException {
            Process process = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .start();
            ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
            AtomicReference<IOException> readFailure = new AtomicReference<>();
            Thread outputReader = new Thread(() -> drainProcessOutput(process.getInputStream(), outputBuffer, readFailure),
                    "streamcraft-core-output-reader");
            outputReader.setDaemon(true);
            outputReader.start();

            boolean finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                outputReader.join(TimeUnit.SECONDS.toMillis(5));
                throw new IllegalStateException("Timed out waiting for local core submission process.");
            }

            outputReader.join(TimeUnit.SECONDS.toMillis(5));
            if (readFailure.get() != null) {
                throw readFailure.get();
            }
            String output = outputBuffer.toString(StandardCharsets.UTF_8);
            return new ProcessExecutionResult(process.exitValue(), output);
        }

        private void drainProcessOutput(
                InputStream inputStream,
                ByteArrayOutputStream outputBuffer,
                AtomicReference<IOException> readFailure) {
            try (InputStream processOutput = inputStream; ByteArrayOutputStream buffer = outputBuffer) {
                processOutput.transferTo(buffer);
            } catch (IOException exception) {
                readFailure.compareAndSet(null, exception);
            }
        }
    }
}
