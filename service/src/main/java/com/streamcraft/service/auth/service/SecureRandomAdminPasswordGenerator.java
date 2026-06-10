package com.streamcraft.service.auth.service;

import java.security.SecureRandom;
import java.util.Base64;
import org.springframework.stereotype.Component;

@Component
public class SecureRandomAdminPasswordGenerator implements AdminPasswordGenerator {

    private static final int PASSWORD_BYTES = 18;

    private final SecureRandom secureRandom = new SecureRandom();

    @Override
    public String generate() {
        byte[] buffer = new byte[PASSWORD_BYTES];
        secureRandom.nextBytes(buffer);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(buffer);
    }
}
