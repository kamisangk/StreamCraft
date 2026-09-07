package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Immutable Kafka Source configuration that is safe to serialize into a Flink job graph. */
final class KafkaSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Set<String> SUPPORTED_FORMATS = Set.of("JSON", "TEXT");
    private static final Set<String> SUPPORTED_AUTH_TYPES = Set.of("NONE", "SASL_PLAIN", "SASL_SCRAM");
    private static final Set<String> SUPPORTED_SCRAM_MECHANISMS = Set.of("SCRAM-SHA-256", "SCRAM-SHA-512");

    private final String bootstrapServers;
    private final List<String> topics;
    private final String groupId;
    private final String consumeMode;
    private final String format;
    private final boolean commitOnCheckpoint;
    private final String authType;
    private final String username;
    private final String password;
    private final String scramMechanism;

    private KafkaSourceConfig(
            String bootstrapServers,
            List<String> topics,
            String groupId,
            String consumeMode,
            String format,
            boolean commitOnCheckpoint,
            String authType,
            String username,
            String password,
            String scramMechanism) {
        this.bootstrapServers = bootstrapServers;
        this.topics = List.copyOf(topics);
        this.groupId = groupId;
        this.consumeMode = consumeMode;
        this.format = format;
        this.commitOnCheckpoint = commitOnCheckpoint;
        this.authType = authType;
        this.username = username;
        this.password = password;
        this.scramMechanism = scramMechanism;
    }

    static KafkaSourceConfig from(JsonNode config) {
        if (config == null || !config.isObject()) {
            throw new IllegalArgumentException("Kafka Source config must be a JSON object.");
        }

        String bootstrapServers = requireNonBlank(config, "bootstrapServers");
        String groupId = requireNonBlank(config, "groupId");
        JsonNode topicsNode = config.get("topics");
        if (topicsNode == null || !topicsNode.isArray() || topicsNode.isEmpty()) {
            throw new IllegalArgumentException("Kafka Source topics must contain at least one topic.");
        }
        List<String> topics = new ArrayList<>(topicsNode.size());
        for (JsonNode topic : topicsNode) {
            String value = topic == null ? null : topic.asText(null);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Kafka Source topic names must not be blank.");
            }
            topics.add(value);
        }

        String authType = readAuthType(config);
        String scramMechanism = validateAuth(config, authType);
        String format = readFormat(config);

        return new KafkaSourceConfig(
                bootstrapServers,
                topics,
                groupId,
                validateConsumeMode(normalizeConsumeMode(config.path("consumeMode").asText())),
                format,
                resolveCommitOnCheckpoint(config),
                authType,
                config.path("username").asText(),
                config.path("password").asText(),
                scramMechanism);
    }

    String bootstrapServers() {
        return bootstrapServers;
    }

    List<String> topics() {
        return topics;
    }

    String groupId() {
        return groupId;
    }

    String consumeMode() {
        return consumeMode;
    }

    String format() {
        return format;
    }

    boolean commitOnCheckpoint() {
        return commitOnCheckpoint;
    }

    static boolean resolveCommitOnCheckpoint(JsonNode config) {
        JsonNode value = config == null ? null : config.get("commitOnCheckpoint");
        if (value != null && !value.isNull() && !value.isBoolean()) {
            throw new IllegalArgumentException("Kafka Source commitOnCheckpoint must be a boolean.");
        }
        return value == null || value.isNull() || value.booleanValue();
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

    String autoOffsetReset() {
        return autoOffsetResetFor(consumeMode);
    }

    static String autoOffsetResetFor(String consumeMode) {
        String normalizedMode = normalizeConsumeMode(consumeMode);
        return switch (normalizedMode) {
            case "earliest" -> "earliest";
            case "latest" -> "latest";
            case "committed" -> "none";
            default -> throw new IllegalArgumentException("Unsupported consumeMode: " + consumeMode);
        };
    }

    private static String normalizeConsumeMode(String consumeMode) {
        return consumeMode == null ? "" : consumeMode.trim().toLowerCase(Locale.ROOT);
    }

    private static String validateConsumeMode(String consumeMode) {
        if (!"earliest".equals(consumeMode) && !"latest".equals(consumeMode) && !"committed".equals(consumeMode)) {
            throw new IllegalArgumentException("Unsupported consumeMode: " + consumeMode);
        }
        return consumeMode;
    }

    private static String readFormat(JsonNode config) {
        JsonNode value = config.get("format");
        return normalizeFormat(value == null || value.isNull() ? "JSON" : value.asText(null));
    }

    private static String normalizeFormat(String format) {
        String normalized = format == null ? "" : format.trim().toUpperCase(Locale.ROOT);
        if (!SUPPORTED_FORMATS.contains(normalized)) {
            throw new IllegalArgumentException("Unsupported Kafka Source format: " + format);
        }
        return normalized;
    }

    private static String requireNonBlank(JsonNode config, String fieldName) {
        JsonNode value = config.get(fieldName);
        String text = value == null || value.isNull() ? null : value.asText(null);
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("Kafka Source " + fieldName + " must not be blank.");
        }
        return text;
    }

    private static String readAuthType(JsonNode config) {
        JsonNode value = config.get("authType");
        return normalizeAuthType(value == null || value.isNull() ? "NONE" : value.asText(null));
    }

    private static String normalizeAuthType(String authType) {
        String normalized = authType == null ? "" : authType.trim().toUpperCase(Locale.ROOT);
        if (!SUPPORTED_AUTH_TYPES.contains(normalized)) {
            throw new IllegalArgumentException("Unsupported Kafka Source authType: " + authType);
        }
        return normalized;
    }

    private static String validateAuth(JsonNode config, String authType) {
        if ("NONE".equals(authType)) {
            return config.path("scramMechanism").asText();
        }
        requireNonBlank(config, "username");
        requireNonBlank(config, "password");
        if ("SASL_SCRAM".equals(authType)) {
            String mechanism = requireNonBlank(config, "scramMechanism").trim().toUpperCase(Locale.ROOT);
            if (!SUPPORTED_SCRAM_MECHANISMS.contains(mechanism)) {
                throw new IllegalArgumentException(
                        "Kafka Source scramMechanism must be SCRAM-SHA-256 or SCRAM-SHA-512.");
            }
            return mechanism;
        }
        return config.path("scramMechanism").asText();
    }
}
