package com.streamcraft.service.flink.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "streamcraft.flink")
public class FlinkGatewayProperties {

    private String coreJarPath = "../core/target/streamcraft-core.jar";
    private Duration connectTimeout = Duration.ofSeconds(2);
    private Duration readTimeout = Duration.ofSeconds(3);
    private String checkpointStorage;
    private long checkpointIntervalMs = 10_000L;
    private long checkpointTimeoutMs = 600_000L;
    private int restartAttempts = 3;
    private long restartDelayMs = 10_000L;

    public String getCoreJarPath() {
        return coreJarPath;
    }

    public void setCoreJarPath(String coreJarPath) {
        this.coreJarPath = coreJarPath;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Duration getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(Duration readTimeout) {
        this.readTimeout = readTimeout;
    }

    public String getCheckpointStorage() {
        return checkpointStorage;
    }

    public void setCheckpointStorage(String checkpointStorage) {
        this.checkpointStorage = checkpointStorage;
    }

    public long getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }

    public void setCheckpointIntervalMs(long checkpointIntervalMs) {
        this.checkpointIntervalMs = checkpointIntervalMs;
    }

    public long getCheckpointTimeoutMs() {
        return checkpointTimeoutMs;
    }

    public void setCheckpointTimeoutMs(long checkpointTimeoutMs) {
        this.checkpointTimeoutMs = checkpointTimeoutMs;
    }

    public int getRestartAttempts() {
        return restartAttempts;
    }

    public void setRestartAttempts(int restartAttempts) {
        this.restartAttempts = restartAttempts;
    }

    public long getRestartDelayMs() {
        return restartDelayMs;
    }

    public void setRestartDelayMs(long restartDelayMs) {
        this.restartDelayMs = restartDelayMs;
    }
}
