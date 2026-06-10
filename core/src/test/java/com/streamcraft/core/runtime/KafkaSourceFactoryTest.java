package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaSourceFactoryTest {

    @Test
    void mapsJsonPayloadIntoFields() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        assertEquals(
                Map.of("status", "ok"),
                KafkaSourceFactory.decodeFields("JSON", "{\"status\":\"ok\"}", objectMapper));
    }

    @Test
    void mapsTextPayloadIntoStreamcraftMessageField() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        assertEquals(
                Map.of("_streamcraft_message", "hello world"),
                KafkaSourceFactory.decodeFields("TEXT", "hello world", objectMapper));
    }

    @Test
    void mapsConsumeModesToKafkaAutoOffsetResetValues() throws Exception {
        Method method = KafkaSourceFactory.class.getDeclaredMethod("resolveAutoOffsetReset", String.class);
        method.setAccessible(true);

        assertEquals("earliest", method.invoke(null, "earliest"));
        assertEquals("latest", method.invoke(null, "latest"));
        assertEquals("none", method.invoke(null, "committed"));
    }

    @Test
    void rejectsUnsupportedConsumeModes() throws Exception {
        Method method = KafkaSourceFactory.class.getDeclaredMethod("resolveAutoOffsetReset", String.class);
        method.setAccessible(true);

        InvocationTargetException exception =
                assertThrows(InvocationTargetException.class, () -> method.invoke(null, "from-nowhere"));

        IllegalArgumentException cause = assertInstanceOf(IllegalArgumentException.class, exception.getCause());
        assertTrue(cause.getMessage().contains("consumeMode"));
    }
}
