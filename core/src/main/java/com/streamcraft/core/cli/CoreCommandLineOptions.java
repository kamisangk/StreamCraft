package com.streamcraft.core.cli;

import com.streamcraft.core.config.CheckpointRuntimeOptions;
import com.streamcraft.core.runtime.ExecutionMode;

public record CoreCommandLineOptions(
        String url,
        String file,
        boolean testMode,
        String definitionAuthToken,
        String clusterBaseUrl,
        Integer parallelism,
        boolean submitOnly,
        String shipJar,
        ExecutionMode executionMode,
        String previewOutputFile,
        String deploymentNamespace,
        String jobId,
        CheckpointRuntimeOptions checkpointOptions) {

    /** Keeps callers compiled against the pre-JobID option shape source-compatible. */
    public CoreCommandLineOptions(
            String url,
            String file,
            boolean testMode,
            String definitionAuthToken,
            String clusterBaseUrl,
            Integer parallelism,
            boolean submitOnly,
            String shipJar,
            ExecutionMode executionMode,
            String previewOutputFile,
            String deploymentNamespace) {
        this(
                url,
                file,
                testMode,
                definitionAuthToken,
                clusterBaseUrl,
                parallelism,
                submitOnly,
                shipJar,
                executionMode,
                previewOutputFile,
                deploymentNamespace,
                null,
                CheckpointRuntimeOptions.defaults());
    }

    public CoreCommandLineOptions(
            String url,
            String file,
            boolean testMode,
            String definitionAuthToken,
            String clusterBaseUrl,
            Integer parallelism,
            boolean submitOnly,
            String shipJar,
            ExecutionMode executionMode,
            String previewOutputFile,
            String deploymentNamespace,
            String jobId) {
        this(
                url,
                file,
                testMode,
                definitionAuthToken,
                clusterBaseUrl,
                parallelism,
                submitOnly,
                shipJar,
                executionMode,
                previewOutputFile,
                deploymentNamespace,
                jobId,
                CheckpointRuntimeOptions.defaults());
    }

    public CoreCommandLineOptions {
        checkpointOptions = checkpointOptions == null
                ? CheckpointRuntimeOptions.defaults()
                : checkpointOptions;
    }

    public static CoreCommandLineOptions parse(String[] args) {
        String url = null;
        String file = null;
        boolean testMode = false;
        String definitionAuthToken = null;
        String clusterBaseUrl = null;
        Integer parallelism = null;
        boolean submitOnly = false;
        String shipJar = null;
        ExecutionMode executionMode = ExecutionMode.RUN;
        String previewOutputFile = null;
        String deploymentNamespace = null;
        String jobId = null;
        CheckpointRuntimeOptions environmentCheckpointOptions =
                CheckpointRuntimeOptions.fromEnvironment(System.getenv());
        String checkpointStorage = environmentCheckpointOptions.storage();
        long checkpointIntervalMs = environmentCheckpointOptions.intervalMs();
        long checkpointTimeoutMs = environmentCheckpointOptions.timeoutMs();
        int restartAttempts = environmentCheckpointOptions.restartAttempts();
        long restartDelayMs = environmentCheckpointOptions.restartDelayMs();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--url" -> {
                    ensureValueExists(args, i, "--url");
                    url = args[++i];
                }
                case "--file" -> {
                    ensureValueExists(args, i, "--file");
                    file = args[++i];
                }
                case "--test-mode" -> {
                    ensureValueExists(args, i, "--test-mode");
                    testMode = Boolean.parseBoolean(args[++i]);
                }
                case "--definition-auth-token" -> {
                    ensureValueExists(args, i, "--definition-auth-token");
                    definitionAuthToken = args[++i];
                }
                case "--cluster-base-url" -> {
                    ensureValueExists(args, i, "--cluster-base-url");
                    clusterBaseUrl = args[++i];
                }
                case "--parallelism" -> {
                    ensureValueExists(args, i, "--parallelism");
                    parallelism = Integer.parseInt(args[++i]);
                }
                case "--submit-only" -> {
                    ensureValueExists(args, i, "--submit-only");
                    submitOnly = Boolean.parseBoolean(args[++i]);
                }
                case "--ship-jar" -> {
                    ensureValueExists(args, i, "--ship-jar");
                    shipJar = args[++i];
                }
                case "--execution-mode" -> {
                    ensureValueExists(args, i, "--execution-mode");
                    executionMode = ExecutionMode.parse(args[++i]);
                }
                case "--preview-output-file" -> {
                    ensureValueExists(args, i, "--preview-output-file");
                    previewOutputFile = args[++i];
                }
                case "--deployment-namespace" -> {
                    ensureValueExists(args, i, "--deployment-namespace");
                    deploymentNamespace = args[++i];
                }
                case "--job-id" -> {
                    ensureValueExists(args, i, "--job-id");
                    jobId = args[++i];
                }
                case "--checkpoint-storage" -> {
                    ensureValueExists(args, i, "--checkpoint-storage");
                    checkpointStorage = args[++i];
                }
                case "--checkpoint-interval-ms" -> {
                    ensureValueExists(args, i, "--checkpoint-interval-ms");
                    checkpointIntervalMs = parseLong(args[++i], "--checkpoint-interval-ms");
                }
                case "--checkpoint-timeout-ms" -> {
                    ensureValueExists(args, i, "--checkpoint-timeout-ms");
                    checkpointTimeoutMs = parseLong(args[++i], "--checkpoint-timeout-ms");
                }
                case "--restart-attempts" -> {
                    ensureValueExists(args, i, "--restart-attempts");
                    restartAttempts = parseInt(args[++i], "--restart-attempts");
                }
                case "--restart-delay-ms" -> {
                    ensureValueExists(args, i, "--restart-delay-ms");
                    restartDelayMs = parseLong(args[++i], "--restart-delay-ms");
                }
                default -> throw new IllegalArgumentException("Unsupported option: " + arg);
            }
        }

        if ((url == null || url.isBlank()) && (file == null || file.isBlank())) {
            throw new IllegalArgumentException("Either --url or --file must be provided.");
        }
        if (parallelism != null && parallelism < 1) {
            throw new IllegalArgumentException("--parallelism must be greater than 0.");
        }
        if (jobId != null && !jobId.matches("[0-9a-fA-F]{32}")) {
            throw new IllegalArgumentException("--job-id must be a 32-character hexadecimal Flink JobID.");
        }
        return new CoreCommandLineOptions(
                url,
                file,
                testMode,
                definitionAuthToken,
                clusterBaseUrl,
                parallelism,
                submitOnly,
                shipJar,
                executionMode,
                previewOutputFile,
                deploymentNamespace,
                jobId,
                new CheckpointRuntimeOptions(
                        checkpointIntervalMs,
                        checkpointTimeoutMs,
                        checkpointStorage,
                        restartAttempts,
                        restartDelayMs));
    }

    private static long parseLong(String raw, String option) {
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(option + " must be a whole number.", exception);
        }
    }

    private static int parseInt(String raw, String option) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(option + " must be a whole number.", exception);
        }
    }

    private static void ensureValueExists(String[] args, int index, String option) {
        if (index + 1 >= args.length) {
            throw new IllegalArgumentException("Missing value for option " + option);
        }
    }
}
