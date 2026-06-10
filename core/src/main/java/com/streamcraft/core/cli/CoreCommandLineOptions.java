package com.streamcraft.core.cli;

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
        String previewOutputFile) {

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
                default -> throw new IllegalArgumentException("Unsupported option: " + arg);
            }
        }

        if ((url == null || url.isBlank()) && (file == null || file.isBlank())) {
            throw new IllegalArgumentException("Either --url or --file must be provided.");
        }
        if (parallelism != null && parallelism < 1) {
            throw new IllegalArgumentException("--parallelism must be greater than 0.");
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
                previewOutputFile);
    }

    private static void ensureValueExists(String[] args, int index, String option) {
        if (index + 1 >= args.length) {
            throw new IllegalArgumentException("Missing value for option " + option);
        }
    }
}
