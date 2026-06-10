package com.streamcraft.core.model;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Map;

public record DataEntity(
        String id,
        long timestamp,
        Map<String, Object> fields,
        Map<String, String> headers) implements Serializable {

    public DataEntity {
        fields = fields == null ? new HashMap<>() : new HashMap<>(fields);
        headers = headers == null ? new HashMap<>() : new HashMap<>(headers);
    }

    public DataEntity withField(String key, Object value) {
        Map<String, Object> newFields = new HashMap<>(fields);
        putNestedValue(newFields, key, value);
        return new DataEntity(id, timestamp, newFields, headers);
    }

    public DataEntity withoutField(String key) {
        Map<String, Object> newFields = new HashMap<>(fields);
        removeNestedValue(newFields, key);
        return new DataEntity(id, timestamp, newFields, headers);
    }

    @SuppressWarnings("unchecked")
    private static void putNestedValue(Map<String, Object> fields, String key, Object value) {
        if (key == null || key.isBlank() || !key.contains(".")) {
            fields.put(key, value);
            return;
        }

        String[] path = key.split("\\.");
        Map<String, Object> current = fields;
        for (int index = 0; index < path.length - 1; index++) {
            String segment = path[index];
            Object existing = current.get(segment);
            if (!(existing instanceof Map<?, ?> existingMap)) {
                Map<String, Object> nested = new LinkedHashMap<>();
                current.put(segment, nested);
                current = nested;
                continue;
            }
            current = new LinkedHashMap<>((Map<String, Object>) existingMap);
            fields.put(segment, current);
            fields = current;
        }
        current.put(path[path.length - 1], value);
    }

    @SuppressWarnings("unchecked")
    private static void removeNestedValue(Map<String, Object> fields, String key) {
        if (key == null || key.isBlank()) {
            return;
        }
        if (!key.contains(".")) {
            fields.remove(key);
            return;
        }
        if (fields.containsKey(key)) {
            fields.remove(key);
            return;
        }

        String[] path = key.split("\\.");
        removeNestedValue(fields, path, 0);
    }

    @SuppressWarnings("unchecked")
    private static boolean removeNestedValue(Map<String, Object> current, String[] path, int index) {
        if (current == null || index >= path.length) {
            return false;
        }

        String segment = path[index];
        if (segment == null || segment.isBlank()) {
            return false;
        }

        if (index == path.length - 1) {
            current.remove(segment);
            return current.isEmpty();
        }

        Object next = current.get(segment);
        if (!(next instanceof Map<?, ?> nextMap)) {
            return false;
        }

        Map<String, Object> mutableChild = new LinkedHashMap<>((Map<String, Object>) nextMap);
        boolean shouldRemoveChild = removeNestedValue(mutableChild, path, index + 1);
        if (shouldRemoveChild) {
            current.remove(segment);
        } else {
            current.put(segment, mutableChild);
        }
        return current.isEmpty();
    }
}
