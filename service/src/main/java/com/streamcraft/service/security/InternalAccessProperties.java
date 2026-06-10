package com.streamcraft.service.security;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "streamcraft.internal")
public record InternalAccessProperties(
        String token,
        @DefaultValue("X-StreamCraft-Internal-Token") String headerName) {
}
