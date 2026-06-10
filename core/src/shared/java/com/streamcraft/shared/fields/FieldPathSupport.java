package com.streamcraft.shared.fields;

import java.util.LinkedHashMap;
import java.util.Map;

public final class FieldPathSupport {

    private FieldPathSupport() {
    }

    public static Lookup lookup(Map<String, Object> fields, String path) {
        if (fields == null || path == null || path.isBlank()) {
            return Lookup.notFound();
        }
        if (fields.containsKey(path)) {
            return Lookup.found(fields.get(path));
        }
        if (!path.contains(".")) {
            return Lookup.notFound();
        }

        Object current = fields;
        for (String segment : path.split("\\.", -1)) {
            if (segment.isEmpty() || !(current instanceof Map<?, ?> currentMap)) {
                return Lookup.notFound();
            }
            if (!currentMap.containsKey(segment)) {
                return Lookup.notFound();
            }
            current = currentMap.get(segment);
        }
        return Lookup.found(current);
    }

    public static Map<String, Object> withField(Map<String, Object> fields, String path, Object value) {
        Map<String, Object> copy = deepCopy(fields);
        if (path == null || path.isBlank()) {
            return copy;
        }
        if (!path.contains(".")) {
            copy.put(path, value);
            return copy;
        }

        String[] segments = path.split("\\.");
        Map<String, Object> current = copy;
        for (int index = 0; index < segments.length - 1; index++) {
            String segment = segments[index];
            Object next = current.get(segment);
            if (!(next instanceof Map<?, ?> nextMap)) {
                Map<String, Object> replacement = new LinkedHashMap<>();
                current.put(segment, replacement);
                current = replacement;
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> typedMap = (Map<String, Object>) nextMap;
            current = typedMap;
        }
        current.put(segments[segments.length - 1], value);
        return copy;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> deepCopy(Map<String, Object> source) {
        Map<String, Object> copy = new LinkedHashMap<>();
        if (source == null) {
            return copy;
        }
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> nestedMap) {
                copy.put(entry.getKey(), deepCopy((Map<String, Object>) nestedMap));
                continue;
            }
            copy.put(entry.getKey(), value);
        }
        return copy;
    }

    public record Lookup(boolean found, Object value) {

        public static Lookup found(Object value) {
            return new Lookup(true, value);
        }

        public static Lookup notFound() {
            return new Lookup(false, null);
        }
    }
}
