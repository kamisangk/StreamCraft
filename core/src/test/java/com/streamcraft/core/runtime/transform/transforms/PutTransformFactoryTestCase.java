package com.streamcraft.core.runtime.transform.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.streamcraft.core.model.DataEntity;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PutTransformFactoryTestCase {

    @Test
    void resolvesNestedReferenceAsValue() {
        DataEntity entity = entityWithNestedValue();

        assertEquals(42, PutTransformFactory.resolveValue(entity, "${a.b}"));
    }

    @Test
    void resolvesNestedReferenceInsideText() {
        DataEntity entity = entityWithNestedValue();

        assertEquals("value=42", PutTransformFactory.resolveValue(entity, "value=${a.b}"));
    }

    @Test
    void exactFlatKeyTakesPrecedenceOverNestedPath() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("a.b", "flat");
        fields.put("a", Map.of("b", "nested"));
        DataEntity entity = new DataEntity("id", 1L, fields, Map.of());

        assertEquals("flat", PutTransformFactory.resolveValue(entity, "${a.b}"));
        assertEquals("value=flat", PutTransformFactory.resolveValue(entity, "value=${a.b}"));
    }

    private static DataEntity entityWithNestedValue() {
        return new DataEntity("id", 1L, Map.of("a", Map.of("b", 42)), Map.of());
    }
}
