package com.streamcraft.service.flink.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "streamcraft.flink")
public class FlinkGatewayProperties {

    private String coreJarPath = "../core/target/streamcraft-core.jar";
    private Duration connectTimeout = Duration.ofSeconds(2);
    private Duration readTimeout = Duration.ofSeconds(3);

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
}
