package com.streamcraft.shared.lookup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.shared.lookup.LookupEnrichConfig.LookupEnrichEntry;
import com.streamcraft.shared.lookup.LookupEnrichConfig.MissingStrategy;
import com.streamcraft.shared.lookup.LookupEnrichConfig.ValueType;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public final class LookupEnrichConfigParser {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private LookupEnrichConfigParser() {
    }

    public static LookupEnrichConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static LookupEnrichConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        String sourceField = text(safeConfig, "sourceField", "");
        if (sourceField.isBlank()) {
            throw error.apply(ValidationError.sourceFieldRequired());
        }

        String targetField = text(safeConfig, "targetField", "");
        if (targetField.isBlank()) {
            throw error.apply(ValidationError.targetFieldRequired());
        }

        List<LookupEnrichEntry> entries = entries(safeConfig, error);
        if (entries.isEmpty()) {
            throw error.apply(ValidationError.entriesRequired());
        }

        MissingStrategy missingStrategy = parseEnum(
                text(safeConfig, "missingStrategy", "KEEP_ORIGINAL"),
                MissingStrategy.class,
                "missingStrategy",
                error);

        return new LookupEnrichConfig(
                sourceField,
                targetField,
                entries,
                missingStrategy,
                booleanValue(safeConfig, "overwriteTargetField", false));
    }

    private static List<LookupEnrichEntry> entries(JsonNode config, ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path("entries");
        if (value == null || !value.isArray()) {
            return List.of();
        }

        List<LookupEnrichEntry> result = new ArrayList<>();
        Set<String> seenKeys = new HashSet<>();
        for (JsonNode item : value) {
            String key = text(item, "key", "");
            JsonNode entryValue = item.path("value");
            if (key.isBlank() || missingOrBlank(entryValue)) {
                throw error.apply(ValidationError.entryRequired());
            }
            if (!seenKeys.add(key)) {
                throw error.apply(ValidationError.duplicateKey(key));
            }
            ValueType valueType = parseEnum(text(item, "valueType", "STRING"), ValueType.class, "valueType", error);
            result.add(new LookupEnrichEntry(key, typedValue(entryValue, valueType, error), valueType));
        }
        return result;
    }

    private static Object typedValue(JsonNode node, ValueType valueType, ValidationErrorFactory error) {
        try {
            return switch (valueType) {
                case STRING -> textValue(node);
                case NUMBER -> numberValue(node, error);
                case BOOLEAN -> booleanValue(node, error);
                case JSON -> jsonValue(node, error);
            };
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw error.apply(ValidationError.unsupportedValue("value", node.asText()));
        }
    }

    private static String textValue(JsonNode node) {
        return node == null || node.isMissingNode() || node.isNull() ? "" : node.asText("").trim();
    }

    private static Object numberValue(JsonNode node, ValidationErrorFactory error) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            throw error.apply(ValidationError.unsupportedValue("value", ""));
        }
        if (node.isIntegralNumber()) {
            return node.asLong();
        }
        if (node.isFloatingPointNumber()) {
            return node.asDouble();
        }
        String text = textValue(node);
        try {
            return text.contains(".") ? Double.parseDouble(text) : Long.parseLong(text);
        } catch (NumberFormatException exception) {
            throw error.apply(ValidationError.unsupportedValue("value", text));
        }
    }

    private static Object booleanValue(JsonNode node, ValidationErrorFactory error) {
        if (node != null && node.isBoolean()) {
            return node.asBoolean();
        }
        String text = textValue(node).toLowerCase(Locale.ROOT);
        if ("true".equals(text)) {
            return true;
        }
        if ("false".equals(text)) {
            return false;
        }
        throw error.apply(ValidationError.unsupportedValue("value", text));
    }

    private static Object jsonValue(JsonNode node, ValidationErrorFactory error) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            try {
                return value(OBJECT_MAPPER.readTree(node.asText()));
            } catch (JsonProcessingException exception) {
                throw error.apply(ValidationError.unsupportedValue("value", node.asText()));
            }
        }
        return value(node);
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
            Map<String, Object> result = new LinkedHashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                result.put(field.getKey(), value(field.getValue()));
            }
            return result;
        }
        if (node.isArray()) {
            List<Object> values = new ArrayList<>();
            node.forEach(item -> values.add(value(item)));
            return values;
        }
        return node.asText();
    }

    private static boolean missingOrBlank(JsonNode value) {
        if (value == null || value.isMissingNode() || value.isNull()) {
            return true;
        }
        return value.isTextual() && value.asText().trim().isBlank();
    }

    private static String text(JsonNode config, String fieldName, String fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        String text = value == null || value.isMissingNode() || value.isNull() ? fallback : value.asText(fallback);
        return text == null ? "" : text.trim();
    }

    private static boolean booleanValue(JsonNode config, String fieldName, boolean fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || value.isMissingNode() || value.isNull()) {
            return fallback;
        }
        return value.asBoolean(fallback);
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
                    "pipeline.validation.lookupEnrich.sourceFieldRequired",
                    "Lookup enrich config sourceField is required.");
        }

        static ValidationError targetFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.lookupEnrich.targetFieldRequired",
                    "Lookup enrich config targetField is required.");
        }

        static ValidationError entriesRequired() {
            return new ValidationError(
                    "pipeline.validation.lookupEnrich.entriesRequired",
                    "Lookup enrich config entries must contain at least one key/value pair.");
        }

        static ValidationError entryRequired() {
            return new ValidationError(
                    "pipeline.validation.lookupEnrich.entryRequired",
                    "Lookup enrich config entries must include both key and value.");
        }

        static ValidationError duplicateKey(String key) {
            return new ValidationError(
                    "pipeline.validation.lookupEnrich.duplicateKey",
                    "Lookup enrich config entries must use unique keys: " + key,
                    key);
        }

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.lookupEnrich.unsupportedValue",
                    "Lookup enrich config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }
    }
}
