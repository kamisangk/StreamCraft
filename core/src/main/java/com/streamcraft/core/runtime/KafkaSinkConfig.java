package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;
import java.util.Locale;
import java.util.Set;
import org.apache.flink.connector.base.DeliveryGuarantee;

/** Immutable, validated configuration shared by the production and preview Kafka sinks. */
final class KafkaSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Set<String> SUPPORTED_FORMATS = Set.of("JSON", "TEXT");
    private static final Set<String> SUPPORTED_AUTH_TYPES = Set.of("NONE", "SASL_PLAIN", "SASL_SCRAM");
    private static final Set<String> SUPPORTED_SCRAM_MECHANISMS = Set.of("SCRAM-SHA-256", "SCRAM-SHA-512");

    private final String bootstrapServers;
    private final String topic;
    private final DeliveryGuarantee deliveryGuarantee;
    private final String format;
    private final String messageField;
    private final String authType;
    private final String username;
    private final String password;
    private final String scramMechanism;

    private KafkaSinkConfig(
            String bootstrapServers,
            String topic,
            DeliveryGuarantee deliveryGuarantee,
            String format,
            String messageField,
            String authType,
            String username,
            String password,
            String scramMechanism) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.deliveryGuarantee = deliveryGuarantee;
        this.format = format;
        this.messageField = messageField;
        this.authType = authType;
        this.username = username;
        this.password = password;
        this.scramMechanism = scramMechanism;
    }

    static KafkaSinkConfig from(JsonNode config) {
        if (config == null || !config.isObject()) {
            throw new IllegalArgumentException("Kafka Sink config must be a JSON object.");
        }

        String bootstrapServers = requireText(config, "bootstrapServers");
        String topic = requireText(config, "topic");
        String guaranteeText = requireText(config, "deliveryGuarantee").toUpperCase(Locale.ROOT);
        DeliveryGuarantee deliveryGuarantee;
        try {
            deliveryGuarantee = DeliveryGuarantee.valueOf(guaranteeText);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "Kafka Sink deliveryGuarantee must be NONE, AT_LEAST_ONCE, or EXACTLY_ONCE.", exception);
        }

        String format = config.path("format").asText("JSON").trim().toUpperCase(Locale.ROOT);
        if (!SUPPORTED_FORMATS.contains(format)) {
            throw new IllegalArgumentException("Kafka Sink format must be JSON or TEXT.");
        }
        String messageField = config.path("messageField").asText("_streamcraft_message");
        if ("TEXT".equals(format) && messageField.isBlank()) {
            throw new IllegalArgumentException("Kafka Sink TEXT format requires messageField.");
        }

        String authType = readAuthType(config);
        String scramMechanism = validateAuth(config, authType);
        return new KafkaSinkConfig(
                bootstrapServers,
                topic,
                deliveryGuarantee,
                format,
                messageField,
                authType,
                config.path("username").asText(),
                config.path("password").asText(),
                scramMechanism);
    }

    static KafkaSinkConfig fromPreview(JsonNode config) {
        JsonNode safeConfig = config == null || !config.isObject()
                ? com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode()
                : config;
        String format = safeConfig.path("format").asText("JSON").trim().toUpperCase(Locale.ROOT);
        if (!SUPPORTED_FORMATS.contains(format)) {
            throw new IllegalArgumentException("Kafka Sink format must be JSON or TEXT.");
        }
        String messageField = safeConfig.path("messageField").asText("_streamcraft_message");
        if ("TEXT".equals(format) && messageField.isBlank()) {
            throw new IllegalArgumentException("Kafka Sink TEXT format requires messageField.");
        }
        return new KafkaSinkConfig(
                safeConfig.path("bootstrapServers").asText("preview"),
                safeConfig.path("topic").asText("preview"),
                DeliveryGuarantee.AT_LEAST_ONCE,
                format,
                messageField,
                "NONE",
                "",
                "",
                "");
    }

    String bootstrapServers() {
        return bootstrapServers;
    }

    String topic() {
        return topic;
    }

    DeliveryGuarantee deliveryGuarantee() {
        return deliveryGuarantee;
    }

    String format() {
        return format;
    }

    String messageField() {
        return messageField;
    }

    String authType() {
        return authType;
    }

    String username() {
        return username;
    }

    String password() {
        return password;
    }

    String scramMechanism() {
        return scramMechanism;
    }

    private static String requireText(JsonNode config, String fieldName) {
        JsonNode value = config.get(fieldName);
        String text = value == null || value.isNull() ? null : value.asText(null);
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("Kafka Sink " + fieldName + " must not be blank.");
        }
        return text.trim();
    }

    private static String readAuthType(JsonNode config) {
        String authType = config.path("authType").asText("NONE").trim().toUpperCase(Locale.ROOT);
        if (!SUPPORTED_AUTH_TYPES.contains(authType)) {
            throw new IllegalArgumentException("Unsupported Kafka Sink authType: " + authType);
        }
        return authType;
    }

    private static String validateAuth(JsonNode config, String authType) {
        if ("NONE".equals(authType)) {
            return config.path("scramMechanism").asText();
        }
        requireText(config, "username");
        requireText(config, "password");
        if ("SASL_SCRAM".equals(authType)) {
            String mechanism = requireText(config, "scramMechanism").toUpperCase(Locale.ROOT);
            if (!SUPPORTED_SCRAM_MECHANISMS.contains(mechanism)) {
                throw new IllegalArgumentException(
                        "Kafka Sink scramMechanism must be SCRAM-SHA-256 or SCRAM-SHA-512.");
            }
            return mechanism;
        }
        return config.path("scramMechanism").asText();
    }
}
