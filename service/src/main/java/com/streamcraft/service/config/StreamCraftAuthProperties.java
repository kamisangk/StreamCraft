package com.streamcraft.service.config;

import java.security.SecureRandom;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.StringUtils;

@ConfigurationProperties(prefix = "streamcraft.auth")
public record StreamCraftAuthProperties(
        String rememberMeKey,
        @DefaultValue("1209600") int rememberMeValiditySeconds) {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamCraftAuthProperties.class);
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String ADMIN_USERNAME = "admin";

    public StreamCraftAuthProperties {
        boolean generatedRememberMeKey = !StringUtils.hasText(rememberMeKey);

        rememberMeKey = generatedRememberMeKey ? generateSecret(32) : rememberMeKey;

        if (generatedRememberMeKey) {
            LOGGER.warn("streamcraft.auth.remember-me-key is not configured. Generated a runtime remember-me key.");
        }
    }

    public String username() {
        return ADMIN_USERNAME;
    }

    private static String generateSecret(int byteLength) {
        byte[] buffer = new byte[byteLength];
        SECURE_RANDOM.nextBytes(buffer);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(buffer);
    }
}
