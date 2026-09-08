package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaPayloadCodecTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void preservesJsonPayloadShape() throws Exception {
        KafkaSinkConfig config = KafkaSinkConfig.from(objectMapper.readTree("""
                {"bootstrapServers":"broker:9092","topic":"out","deliveryGuarantee":"AT_LEAST_ONCE","format":"JSON"}
                """));

        assertEquals("{\"name\":\"Ada\"}", KafkaPayloadCodec.encode(
                config,
                new DataEntity("1", 1L, Map.of("name", "Ada"), Map.of()),
                objectMapper));
    }

    @Test
    void prefersLiteralDottedFieldBeforeNestedLookup() throws Exception {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("payload.text", "literal");
        fields.put("payload", Map.of("text", "nested"));
        KafkaSinkConfig config = KafkaSinkConfig.from(objectMapper.readTree("""
                {"bootstrapServers":"broker:9092","topic":"out","deliveryGuarantee":"NONE","format":"TEXT","messageField":"payload.text"}
                """));

        assertEquals("literal", KafkaPayloadCodec.encode(
                config,
                new DataEntity("1", 1L, fields, Map.of()),
                objectMapper));
    }

    @Test
    void rejectsMissingTextField() throws Exception {
        KafkaSinkConfig config = KafkaSinkConfig.from(objectMapper.readTree("""
                {"bootstrapServers":"broker:9092","topic":"out","deliveryGuarantee":"EXACTLY_ONCE","format":"TEXT","messageField":"payload"}
                """));

        assertThrows(IllegalArgumentException.class, () -> KafkaPayloadCodec.encode(
                config,
                new DataEntity("1", 1L, Map.of(), Map.of()),
                objectMapper));
    }
}
