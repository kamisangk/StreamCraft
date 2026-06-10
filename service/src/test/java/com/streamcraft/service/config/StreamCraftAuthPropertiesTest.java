package com.streamcraft.service.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class StreamCraftAuthPropertiesTest {

    @Test
    void defaultsUsernameAndGeneratesRememberMeKeyWhenSensitiveValuesAreMissing() {
        StreamCraftAuthProperties properties = new StreamCraftAuthProperties(null, 1209600);

        assertTrue("admin".equals(properties.username()));
        assertFalse(properties.rememberMeKey().isBlank());
        assertFalse("streamcraft-local-dev-key".equals(properties.rememberMeKey()));
    }

    @Test
    void applicationPropertiesDoNotShipHardCodedSecrets() throws Exception {
        String properties = Files.readString(Path.of("src/main/resources/application.properties"));

        assertFalse(properties.contains("admin123"));
        assertFalse(properties.contains("admin123456"));
        assertFalse(properties.contains("streamcraft.auth.username"));
        assertFalse(properties.contains("streamcraft-local-dev-key"));
        assertTrue(properties.contains("streamcraft.auth.remember-me-key="));
    }
}
