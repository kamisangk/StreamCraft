package com.streamcraft.core.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DataEntityOrderTest {

    @Test
    void preservesFieldOrderThroughConstructionAndCopyMutation() {
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("second", 2);
        input.put("first", 1);
        DataEntity entity = new DataEntity("id", 1L, input, Map.of());

        assertEquals(List.of("second", "first"), List.copyOf(entity.fields().keySet()));

        DataEntity extended = entity.withField("third", 3);
        assertEquals(List.of("second", "first", "third"),
                List.copyOf(extended.fields().keySet()));

        DataEntity withoutFirst = extended.withoutField("first");
        assertEquals(List.of("second", "third"), List.copyOf(withoutFirst.fields().keySet()));
    }
}
