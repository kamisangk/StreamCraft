package com.streamcraft.service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "streamcraft.pipeline.runtime")
public record PipelineRuntimeProperties(
        @DefaultValue("http://localhost:8080") String serviceBaseUrl,
        @DefaultValue("false") boolean testMode,
        @DefaultValue("1") int parallelism) {

    @ConstructorBinding
    public PipelineRuntimeProperties {
    }
}
