package com.streamcraft.service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Deployment-scoped identity shared by all transactional pipeline runs. */
@ConfigurationProperties(prefix = "streamcraft.deployment")
public record PipelineDeploymentProperties(@DefaultValue("") String namespace) {

    @ConstructorBinding
    public PipelineDeploymentProperties {
    }

    public String requiredNamespace() {
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalStateException(
                    "streamcraft.deployment.namespace must be configured for Kafka EXACTLY_ONCE pipelines.");
        }
        return namespace.trim();
    }
}
