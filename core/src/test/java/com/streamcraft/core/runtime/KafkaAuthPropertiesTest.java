package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class KafkaAuthPropertiesTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void noneAuthDoesNotInjectSaslProperties() {
        Properties properties = new Properties();

        KafkaAuthProperties.apply(jsonNode("""
                {
                  "authType": "NONE"
                }
                """), properties);

        assertFalse(properties.containsKey("security.protocol"));
        assertFalse(properties.containsKey("sasl.mechanism"));
        assertFalse(properties.containsKey("sasl.jaas.config"));
    }

    @Test
    void saslPlainInjectsPlaintextAndPlainLoginModule() {
        Properties properties = new Properties();

        KafkaAuthProperties.apply(jsonNode("""
                {
                  "authType": "SASL_PLAIN",
                  "username": "alice",
                  "password": "secret"
                }
                """), properties);

        assertEquals("SASL_PLAINTEXT", properties.getProperty("security.protocol"));
        assertEquals("PLAIN", properties.getProperty("sasl.mechanism"));
        assertTrue(properties.getProperty("sasl.jaas.config").contains("PlainLoginModule"));
    }

    @Test
    void saslScramInjectsConfiguredMechanismAndScramLoginModule() {
        Properties properties = new Properties();

        KafkaAuthProperties.apply(jsonNode("""
                {
                  "authType": "SASL_SCRAM",
                  "username": "alice",
                  "password": "secret",
                  "scramMechanism": "SCRAM-SHA-512"
                }
                """), properties);

        assertEquals("SASL_PLAINTEXT", properties.getProperty("security.protocol"));
        assertEquals("SCRAM-SHA-512", properties.getProperty("sasl.mechanism"));
        assertTrue(properties.getProperty("sasl.jaas.config").contains("ScramLoginModule"));
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
