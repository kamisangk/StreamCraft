package com.streamcraft.shared.lookupjoin;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.lookupjoin.LookupJoinConfig.JoinType;
import com.streamcraft.shared.lookupjoin.LookupJoinConfig.LookupJoinEntry;
import com.streamcraft.shared.lookupjoin.LookupJoinConfig.MissingStrategy;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public final class LookupJoinConfigParser {

    private LookupJoinConfigParser() {
    }

    public static LookupJoinConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static LookupJoinConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        String sourceField = text(safeConfig, "sourceField", "");
        if (sourceField.isBlank()) {
            throw error.apply(ValidationError.sourceFieldRequired());
        }
        String targetField = text(safeConfig, "targetField", "");
        if (targetField.isBlank()) {
            throw error.apply(ValidationError.targetFieldRequired());
        }
        List<LookupJoinEntry> entries = entries(safeConfig, error);
        if (entries.isEmpty()) {
            throw error.apply(ValidationError.entriesRequired());
        }

        return new LookupJoinConfig(
                sourceField,
                targetField,
                parseEnum(text(safeConfig, "joinType", "LEFT"), JoinType.class, "joinType", error),
                parseEnum(text(safeConfig, "missingStrategy", "KEEP_ORIGINAL"), MissingStrategy.class, "missingStrategy", error),
                booleanValue(safeConfig, "overwriteTargetField", false),
                entries);
    }

    private static List<LookupJoinEntry> entries(JsonNode config, ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path("entries");
        if (value == null || !value.isArray()) {
            return List.of();
        }
        List<LookupJoinEntry> result = new ArrayList<>();
        Set<String> keys = new HashSet<>();
        for (JsonNode item : value) {
            String key = text(item, "key", "");
            if (key.isBlank()) {
                throw error.apply(ValidationError.entryRequired());
            }
            if (!keys.add(key)) {
                throw error.apply(ValidationError.duplicateKey(key));
            }
            JsonNode fields = item.path("fields");
            if (!fields.isObject() || fields.isEmpty()) {
                throw error.apply(ValidationError.fieldsRequired(key));
            }
            result.add(new LookupJoinEntry(key, objectValue(fields)));
        }
        return result;
    }

    private static Map<String, Object> objectValue(JsonNode node) {
        Map<String, Object> result = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            result.put(field.getKey(), value(field.getValue()));
        }
        return result;
    }

    private static Object value(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        if (node.isIntegralNumber()) {
            return node.asLong();
        }
        if (node.isFloatingPointNumber()) {
            return node.asDouble();
        }
        if (node.isObject()) {
            return objectValue(node);
        }
        if (node.isArray()) {
            List<Object> values = new ArrayList<>();
            node.forEach(item -> values.add(value(item)));
            return values;
        }
        return node.asText();
    }

    private static String text(JsonNode config, String fieldName, String fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        String text = value == null || value.isMissingNode() || value.isNull() ? fallback : value.asText(fallback);
        return text == null ? "" : text.trim();
    }

    private static boolean booleanValue(JsonNode config, String fieldName, boolean fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        return value == null || value.isMissingNode() || value.isNull() ? fallback : value.asBoolean(fallback);
    }

    private static <T extends Enum<T>> T parseEnum(
            String value,
            Class<T> enumType,
            String fieldName,
            ValidationErrorFactory error) {
        try {
            return Enum.valueOf(enumType, value.trim().toUpperCase(Locale.ROOT));
        } catch (Exception exception) {
            throw error.apply(ValidationError.unsupportedValue(fieldName, value));
        }
    }

    @FunctionalInterface
    public interface ValidationErrorFactory {
        IllegalArgumentException apply(ValidationError error);
    }

    public record ValidationError(String messageKey, String defaultMessage, Object... args) {

        static ValidationError sourceFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.lookupJoin.sourceFieldRequired",
                    "Lookup join config sourceField is required.");
        }

        static ValidationError targetFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.lookupJoin.targetFieldRequired",
                    "Lookup join config targetField is required.");
        }

        static ValidationError entriesRequired() {
            return new ValidationError(
                    "pipeline.validation.lookupJoin.entriesRequired",
                    "Lookup join config entries must contain at least one item.");
        }

        static ValidationError entryRequired() {
            return new ValidationError(
                    "pipeline.validation.lookupJoin.entryRequired",
                    "Lookup join config entry key is required.");
        }

        static ValidationError fieldsRequired(String key) {
            return new ValidationError(
                    "pipeline.validation.lookupJoin.fieldsRequired",
                    "Lookup join config entry fields are required for key: " + key,
                    key);
        }

        static ValidationError duplicateKey(String key) {
            return new ValidationError(
                    "pipeline.validation.lookupJoin.duplicateKey",
                    "Lookup join config entries must use unique keys: " + key,
                    key);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.lookupJoin.unsupportedValue",
                    "Lookup join config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
