package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Properties;

final class KafkaAuthProperties {

    private static final String SECURITY_PROTOCOL = "security.protocol";
    private static final String SASL_MECHANISM = "sasl.mechanism";
    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    private static final String SASL_PLAINTEXT = "SASL_PLAINTEXT";
    private static final String AUTH_TYPE_NONE = "NONE";
    private static final String AUTH_TYPE_SASL_PLAIN = "SASL_PLAIN";
    private static final String AUTH_TYPE_SASL_SCRAM = "SASL_SCRAM";

    private KafkaAuthProperties() {}

    static void apply(JsonNode config, Properties properties) {
        String authType = config == null ? AUTH_TYPE_NONE : config.path("authType").asText(AUTH_TYPE_NONE);
        switch (authType) {
            case AUTH_TYPE_NONE -> {
                return;
            }
            case AUTH_TYPE_SASL_PLAIN -> {
                String username = config.path("username").asText();
                String password = config.path("password").asText();
                properties.put(SECURITY_PROTOCOL, SASL_PLAINTEXT);
                properties.put(SASL_MECHANISM, "PLAIN");
                properties.put(
                        SASL_JAAS_CONFIG,
                        "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                + "username=\""
                                + escape(username)
                                + "\" password=\""
                                + escape(password)
                                + "\";");
            }
            case AUTH_TYPE_SASL_SCRAM -> {
                String username = config.path("username").asText();
                String password = config.path("password").asText();
                String mechanism = config.path("scramMechanism").asText();
                properties.put(SECURITY_PROTOCOL, SASL_PLAINTEXT);
                properties.put(SASL_MECHANISM, mechanism);
                properties.put(
                        SASL_JAAS_CONFIG,
                        "org.apache.kafka.common.security.scram.ScramLoginModule required "
                                + "username=\""
                                + escape(username)
                                + "\" password=\""
                                + escape(password)
                                + "\";");
            }
            default -> throw new IllegalArgumentException("Unsupported authType: " + authType);
        }
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
