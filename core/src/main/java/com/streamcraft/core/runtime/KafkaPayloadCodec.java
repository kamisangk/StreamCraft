package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import java.io.IOException;
import java.util.Map;

/** Encodes Kafka Sink payloads without owning any Kafka producer lifecycle. */
final class KafkaPayloadCodec {

    private KafkaPayloadCodec() {}

    static String encode(KafkaSinkConfig config, DataEntity value, ObjectMapper objectMapper) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException("Kafka Sink config is required.");
        }
        if (value == null) {
            throw new IllegalArgumentException("Kafka Sink cannot encode a null record.");
        }
        if (objectMapper == null) {
            throw new IllegalArgumentException("Kafka Sink object mapper is required.");
        }
        if ("JSON".equals(config.format())) {
            return objectMapper.writeValueAsString(value.fields());
        }

        Object fieldValue = resolveMessageFieldValue(value.fields(), config.messageField());
        if (fieldValue == null) {
            throw new IllegalArgumentException("Kafka Sink TEXT format requires a non-null messageField value.");
        }
        if (fieldValue instanceof String stringValue) {
            return stringValue;
        }
        if (fieldValue instanceof Number || fieldValue instanceof Boolean) {
            return String.valueOf(fieldValue);
        }
        return objectMapper.writeValueAsString(fieldValue);
    }

    static String encode(String format, String messageField, DataEntity value, ObjectMapper objectMapper)
            throws IOException {
        if (objectMapper == null) {
            throw new IllegalArgumentException("Kafka Sink object mapper is required.");
        }
        if ("JSON".equals(format)) {
            if (value == null) {
                throw new IllegalArgumentException("Kafka Sink cannot encode a null record.");
            }
            return objectMapper.writeValueAsString(value.fields());
        }
        Object fieldValue = resolveMessageFieldValue(value == null ? null : value.fields(), messageField);
        if (fieldValue == null) {
            throw new IllegalArgumentException("Kafka Sink TEXT format requires a non-null messageField value.");
        }
        if (fieldValue instanceof String stringValue) {
            return stringValue;
        }
        if (fieldValue instanceof Number || fieldValue instanceof Boolean) {
            return String.valueOf(fieldValue);
        }
        return objectMapper.writeValueAsString(fieldValue);
    }

    static Object resolveMessageFieldValue(Map<String, Object> fields, String messageField) {
        if (fields == null || messageField == null || messageField.isBlank()) {
            return null;
        }
        if (fields.containsKey(messageField)) {
            return fields.get(messageField);
        }

        Object current = fields;
        for (String segment : messageField.split("\\.", -1)) {
            if (segment.isEmpty() || !(current instanceof Map<?, ?> currentMap)) {
                return null;
            }
            current = currentMap.get(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }
}
