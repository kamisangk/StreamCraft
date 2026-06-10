package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaSinkFactoryContractTest {

    @Test
    void kafkaSinkDoesNotBlockOnPerRecordFutureGet() throws Exception {
        String source = Files.readString(Path.of("src/main/java/com/streamcraft/core/runtime/KafkaSinkFactory.java"));

        assertFalse(source.contains("producer.send(record).get()"));
    }

    @Test
    void kafkaSinkTextFormatUsesNamedFieldAsOutputPayload() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        DataEntity entity = new DataEntity(
                "id-1",
                1L,
                Map.of("_streamcraft_message", "hello world", "body", 42),
                Map.of());

        String payload = KafkaSinkFactory.buildPayload("TEXT", "_streamcraft_message", entity, objectMapper);
        String namedPayload = KafkaSinkFactory.buildPayload("TEXT", "body", entity, objectMapper);

        assertEquals("hello world", payload);
        assertEquals("42", namedPayload);
        assertEquals(
                objectMapper.readTree("{\"_streamcraft_message\":\"hello world\",\"body\":42}"),
                objectMapper.readTree(KafkaSinkFactory.buildPayload("JSON", "_streamcraft_message", entity, objectMapper)));
    }

    @Test
    void kafkaSinkTextFormatSerializesObjectsToJsonText() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        DataEntity entity = new DataEntity("id-1", 1L, Map.of("body", Map.of("status", "ok")), Map.of());

        assertEquals("{\"status\":\"ok\"}", KafkaSinkFactory.buildPayload("TEXT", "body", entity, objectMapper));
    }

    @Test
    void kafkaSinkTextFormatResolvesNestedFieldPath() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        DataEntity entity = new DataEntity(
                "id-1",
                1L,
                Map.of("test", Map.of("test1", "23333")),
                Map.of());

        assertEquals("23333", KafkaSinkFactory.buildPayload("TEXT", "test.test1", entity, objectMapper));
    }

    @Test
    void kafkaSinkTextFormatSerializesNestedObjectPathToJsonText() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        DataEntity entity = new DataEntity(
                "id-1",
                1L,
                Map.of("test", Map.of("test1", "23333")),
                Map.of());

        assertEquals("{\"test1\":\"23333\"}", KafkaSinkFactory.buildPayload("TEXT", "test", entity, objectMapper));
    }

    @Test
    void kafkaSinkTextFormatPrefersLiteralTopLevelDottedKey() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        DataEntity entity = new DataEntity(
                "id-1",
                1L,
                Map.of(
                        "test", Map.of("test1", "23333"),
                        "test.test1", "literal"),
                Map.of());

        assertEquals("literal", KafkaSinkFactory.buildPayload("TEXT", "test.test1", entity, objectMapper));
    }

    @Test
    void kafkaSinkTextFormatRejectsMissingField() {
        ObjectMapper objectMapper = new ObjectMapper();
        DataEntity entity = new DataEntity("id-1", 1L, Map.of(), Map.of());

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> KafkaSinkFactory.buildPayload("TEXT", "body", entity, objectMapper));

        assertTrue(exception.getMessage().contains("messageField"));
    }

    @Test
    void kafkaSinkTextFormatRejectsMalformedTrailingDotFieldPath() {
        ObjectMapper objectMapper = new ObjectMapper();
        DataEntity entity = new DataEntity(
                "id-1",
                1L,
                Map.of("test", Map.of("test1", "23333")),
                Map.of());

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> KafkaSinkFactory.buildPayload("TEXT", "test.", entity, objectMapper));

        assertTrue(exception.getMessage().contains("messageField"));
    }

    @Test
    void kafkaSinkTextFormatRejectsMalformedDoubleDotFieldPath() {
        ObjectMapper objectMapper = new ObjectMapper();
        DataEntity entity = new DataEntity(
                "id-1",
                1L,
                Map.of("test", Map.of("test1", "23333")),
                Map.of());

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> KafkaSinkFactory.buildPayload("TEXT", "test..x", entity, objectMapper));

        assertTrue(exception.getMessage().contains("messageField"));
    }

    @Test
    void kafkaSinkTextFormatRejectsMissingNestedFieldPath() {
        ObjectMapper objectMapper = new ObjectMapper();
        DataEntity entity = new DataEntity(
                "id-1",
                1L,
                Map.of("test", Map.of("test1", "23333")),
                Map.of());

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> KafkaSinkFactory.buildPayload("TEXT", "test.missing", entity, objectMapper));

        assertTrue(exception.getMessage().contains("messageField"));
    }

    @Test
    void kafkaSinkTextFormatRejectsNullMessageField() {
        ObjectMapper objectMapper = new ObjectMapper();
        DataEntity entity = new DataEntity(
                "id-1",
                1L,
                Map.of("test", Map.of("test1", "23333")),
                Map.of());

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> KafkaSinkFactory.buildPayload("TEXT", null, entity, objectMapper));

        assertTrue(exception.getMessage().contains("messageField"));
    }

    @Test
    void kafkaSinkTextPayloadOrSkipReturnsPayloadWithoutLoggingForValidText() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        DataEntity entity = new DataEntity("id-1", 1L, Map.of("body", "hello world"), Map.of());
        List<String> skipLogs = new ArrayList<>();

        String payload = KafkaSinkFactory.buildPayloadOrSkip(
                "TEXT", "body", "orders", entity, objectMapper, skipLogs::add);

        assertEquals("hello world", payload);
        assertTrue(skipLogs.isEmpty());
    }

    @Test
    void kafkaSinkTextPayloadOrSkipReturnsNullAndLogsForInvalidText() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        DataEntity entity = new DataEntity("id-42", 1L, Map.of(), Map.of());
        List<String> skipLogs = new ArrayList<>();

        String payload = KafkaSinkFactory.buildPayloadOrSkip(
                "TEXT", "body", "orders", entity, objectMapper, skipLogs::add);

        assertEquals(null, payload);
        assertEquals(1, skipLogs.size());
        assertTrue(skipLogs.get(0).contains("orders"));
        assertTrue(skipLogs.get(0).contains("body"));
        assertTrue(skipLogs.get(0).contains("id-42"));
        assertTrue(skipLogs.get(0).contains("messageField"));
    }

    @Test
    void kafkaSinkJsonPayloadOrSkipDoesNotSwallowIllegalArgumentException() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper() {
            @Override
            public String writeValueAsString(Object value) {
                throw new IllegalArgumentException("boom");
            }
        };
        DataEntity entity = new DataEntity("id-1", 1L, Map.of("body", "hello world"), Map.of());
        List<String> skipLogs = new ArrayList<>();

        String payload = KafkaSinkFactory.buildPayloadOrSkip(
                "JSON", "body", "orders", entity, objectMapper, skipLogs::add);

        assertEquals(null, payload);
        assertEquals(1, skipLogs.size());
        assertTrue(skipLogs.get(0).contains("orders"));
        assertTrue(skipLogs.get(0).contains("id-1"));
        assertTrue(skipLogs.get(0).contains("boom"));
    }
}
