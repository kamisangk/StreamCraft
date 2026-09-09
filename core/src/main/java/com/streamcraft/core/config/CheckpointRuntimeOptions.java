package com.streamcraft.core.config;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;

/**
 * Runtime checkpoint and restart settings shared by local and remote jobs.
 *
 * <p>The checkpoint directory is intentionally optional for local execution,
 * but remote submission must provide one so recovery does not depend on the
 * JobManager or TaskManager process filesystem.</p>
 */
public record CheckpointRuntimeOptions(
        String checkpointStorage,
        Duration checkpointInterval,
        Duration checkpointTimeout,
        int restartAttempts,
        Duration restartDelay) {

    public static final String CHECKPOINT_STORAGE_ENV = "STREAMCRAFT_CHECKPOINT_STORAGE";
    public static final String CHECKPOINT_INTERVAL_ENV = "STREAMCRAFT_CHECKPOINT_INTERVAL_MS";
    public static final String CHECKPOINT_TIMEOUT_ENV = "STREAMCRAFT_CHECKPOINT_TIMEOUT_MS";
    public static final String RESTART_ATTEMPTS_ENV = "STREAMCRAFT_RESTART_ATTEMPTS";
    public static final String RESTART_DELAY_ENV = "STREAMCRAFT_RESTART_DELAY_MS";

    private static final Duration DEFAULT_CHECKPOINT_INTERVAL = Duration.ofSeconds(10);
    private static final Duration DEFAULT_CHECKPOINT_TIMEOUT = Duration.ofMinutes(10);
    private static final int DEFAULT_RESTART_ATTEMPTS = 3;
    private static final Duration DEFAULT_RESTART_DELAY = Duration.ofSeconds(10);

    public CheckpointRuntimeOptions {
        checkpointStorage = normalizeStorage(checkpointStorage);
        checkpointInterval = requirePositive(checkpointInterval, "checkpoint interval");
        checkpointTimeout = requirePositive(checkpointTimeout, "checkpoint timeout");
        if (restartAttempts < 0) {
            throw new IllegalArgumentException("restart attempts must be non-negative.");
        }
        restartDelay = requireNonNegative(restartDelay, "restart delay");
    }

    public static CheckpointRuntimeOptions defaults() {
        return new CheckpointRuntimeOptions(
                null,
                DEFAULT_CHECKPOINT_INTERVAL,
                DEFAULT_CHECKPOINT_TIMEOUT,
                DEFAULT_RESTART_ATTEMPTS,
                DEFAULT_RESTART_DELAY);
    }

    public static CheckpointRuntimeOptions fromEnvironment(Map<String, String> environment) {
        Map<String, String> values = environment == null ? Map.of() : environment;
        CheckpointRuntimeOptions defaults = defaults();

        return new CheckpointRuntimeOptions(
                textOrNull(values.get(CHECKPOINT_STORAGE_ENV)),
                durationMillis(values, CHECKPOINT_INTERVAL_ENV, defaults.checkpointInterval()),
                durationMillis(values, CHECKPOINT_TIMEOUT_ENV, defaults.checkpointTimeout()),
                integer(values, RESTART_ATTEMPTS_ENV, defaults.restartAttempts()),
                durationMillis(values, RESTART_DELAY_ENV, defaults.restartDelay()));
    }

    /**
     * Converts these settings to Flink's job configuration options.
     *
     * <p>When no storage directory is configured, the directory option is
     * omitted so local/test execution retains Flink's existing behavior.</p>
     */
    public Configuration toFlinkConfiguration() {
        Configuration configuration = new Configuration();
        if (checkpointStorage != null) {
            configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointStorage);
        }
        configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, checkpointInterval);
        configuration.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, checkpointTimeout);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, restartAttempts);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, restartDelay);
        return configuration;
    }

    /**
     * Ensures a remote job has an explicitly configured, addressable storage
     * location for checkpoint files.
     */
    public void validateForRemoteSubmission() {
        if (checkpointStorage == null) {
            throw new IllegalArgumentException(
                    "A durable checkpoint storage URI is required for remote submission.");
        }

        final URI uri;
        try {
            uri = URI.create(checkpointStorage);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "Checkpoint storage must be a valid URI for remote submission.", exception);
        }
        String scheme = uri.getScheme();
        boolean hasAuthority = uri.getAuthority() != null && !uri.getAuthority().isBlank();
        boolean hasPath = uri.getPath() != null && !uri.getPath().isBlank();
        if (scheme == null || scheme.isBlank() || (!hasAuthority && !hasPath)) {
            throw new IllegalArgumentException(
                    "Checkpoint storage must include a URI scheme and path for remote submission.");
        }
        if ("memory".equalsIgnoreCase(scheme) || "none".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException(
                    "Checkpoint storage must be durable for remote submission.");
        }
    }

    private static String normalizeStorage(String value) {
        return textOrNull(value);
    }

    private static String textOrNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static Duration durationMillis(
            Map<String, String> environment, String variable, Duration defaultValue) {
        String raw = textOrNull(environment.get(variable));
        if (raw == null) {
            return defaultValue;
        }
        long millis;
        try {
            millis = Long.parseLong(raw);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(variable + " must be a whole number of milliseconds.", exception);
        }
        if (millis <= 0) {
            throw new IllegalArgumentException(variable + " must be greater than zero.");
        }
        try {
            return Duration.ofMillis(millis);
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException(variable + " is outside the supported duration range.", exception);
        }
    }

    private static int integer(Map<String, String> environment, String variable, int defaultValue) {
        String raw = textOrNull(environment.get(variable));
        if (raw == null) {
            return defaultValue;
        }
        try {
            int value = Integer.parseInt(raw);
            if (value < 0) {
                throw new IllegalArgumentException(variable + " must be non-negative.");
            }
            return value;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(variable + " must be a whole number.", exception);
        }
    }

    private static Duration requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be greater than zero.");
        }
        return value;
    }

    private static Duration requireNonNegative(Duration value, String name) {
        if (value == null || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be non-negative.");
        }
        return value;
    }
}
