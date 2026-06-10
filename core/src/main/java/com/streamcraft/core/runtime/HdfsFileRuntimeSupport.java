package com.streamcraft.core.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.shared.file.HdfsFileFormatType;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class HdfsFileRuntimeSupport {

    static final String TEXT_MESSAGE_FIELD = "_streamcraft_message";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private HdfsFileRuntimeSupport() {
    }

    static org.apache.flink.core.fs.Path resolvePath(String defaultFs, String configuredPath) {
        String path = configuredPath == null ? "" : configuredPath.trim();
        if (hasScheme(path)) {
            return new org.apache.flink.core.fs.Path(path);
        }
        String base = defaultFs == null ? "" : defaultFs.trim();
        String separator = path.startsWith("/") || base.endsWith("/") ? "" : "/";
        return new org.apache.flink.core.fs.Path(base + separator + path);
    }

    static Map<String, Object> parseRecord(
            HdfsFileFormatType format,
            String line,
            List<String> fieldNames,
            String fieldDelimiter,
            ObjectMapper objectMapper) throws IOException {
        return switch (format) {
            case JSON -> parseJson(line, objectMapper);
            case TEXT -> new LinkedHashMap<>(Map.of(TEXT_MESSAGE_FIELD, line == null ? "" : line));
            case CSV -> parseCsv(line, fieldNames, fieldDelimiter);
            default -> throw new UnsupportedOperationException(
                    "HDFS File runtime currently supports JSON, CSV and TEXT. Unsupported format: " + format);
        };
    }

    static String formatRecord(
            HdfsFileFormatType format,
            Map<String, Object> fields,
            List<String> columns,
            String fieldDelimiter,
            ObjectMapper objectMapper) throws IOException {
        return switch (format) {
            case JSON -> objectMapper.writeValueAsString(selectFields(fields, columns));
            case TEXT -> formatText(fields, columns, objectMapper);
            case CSV -> formatCsv(fields, columns, fieldDelimiter, objectMapper);
            default -> throw new UnsupportedOperationException(
                    "HDFS File runtime currently supports JSON, CSV and TEXT. Unsupported format: " + format);
        };
    }

    static List<String> resolveColumns(Map<String, Object> fields, List<String> configuredColumns) {
        if (configuredColumns != null && !configuredColumns.isEmpty()) {
            return configuredColumns;
        }
        return fields.keySet().stream().sorted().toList();
    }

    static String extension(HdfsFileFormatType format) {
        return switch (format) {
            case JSON -> ".json";
            case TEXT -> ".txt";
            case CSV -> ".csv";
            case PARQUET -> ".parquet";
            case ORC -> ".orc";
            case EXCEL -> ".xlsx";
            case XML -> ".xml";
            case BINARY -> ".bin";
        };
    }

    static String normalizeFileName(String fileName) {
        String normalized = fileName == null ? "" : fileName.trim();
        if (normalized.isBlank()) {
            normalized = "part";
        }
        return normalized.replaceAll("[\\\\/:*?\"<>|]", "_");
    }

    static List<String> parseCsvLine(String line, String delimiter) {
        String separator = delimiter == null || delimiter.isEmpty() ? "," : delimiter;
        if (separator.length() != 1) {
            return List.of(line.split(java.util.regex.Pattern.quote(separator), -1));
        }

        char delimiterChar = separator.charAt(0);
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int index = 0; index < line.length(); index++) {
            char character = line.charAt(index);
            if (character == '"') {
                if (quoted && index + 1 < line.length() && line.charAt(index + 1) == '"') {
                    current.append('"');
                    index++;
                } else {
                    quoted = !quoted;
                }
                continue;
            }
            if (character == delimiterChar && !quoted) {
                values.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(character);
        }
        values.add(current.toString());
        return values;
    }

    private static Map<String, Object> parseJson(String line, ObjectMapper objectMapper) throws IOException {
        JsonNode node = objectMapper.readTree(line == null || line.isBlank() ? "{}" : line);
        if (!node.isObject()) {
            throw new IllegalArgumentException("HDFS File Source JSON record must be a JSON object.");
        }
        return new LinkedHashMap<>(objectMapper.convertValue(node, MAP_TYPE));
    }

    private static Map<String, Object> parseCsv(String line, List<String> fieldNames, String delimiter) {
        List<String> values = parseCsvLine(line == null ? "" : line, delimiter);
        Map<String, Object> fields = new LinkedHashMap<>();
        for (int index = 0; index < values.size(); index++) {
            String fieldName = index < fieldNames.size() ? fieldNames.get(index) : "column_" + index;
            fields.put(fieldName, values.get(index));
        }
        return fields;
    }

    private static String formatText(Map<String, Object> fields, List<String> columns, ObjectMapper objectMapper)
            throws IOException {
        List<String> resolvedColumns = resolveColumns(fields, columns);
        if (resolvedColumns.size() == 1) {
            Object value = fields.get(resolvedColumns.get(0));
            return value instanceof String stringValue ? stringValue : String.valueOf(value);
        }
        return objectMapper.writeValueAsString(selectFields(fields, resolvedColumns));
    }

    private static String formatCsv(
            Map<String, Object> fields,
            List<String> columns,
            String fieldDelimiter,
            ObjectMapper objectMapper) throws IOException {
        List<String> resolvedColumns = resolveColumns(fields, columns);
        List<String> values = new ArrayList<>();
        for (String column : resolvedColumns) {
            Object value = fields.get(column);
            values.add(escapeCsvValue(value, fieldDelimiter, objectMapper));
        }
        return String.join(fieldDelimiter, values);
    }

    private static String escapeCsvValue(Object value, String fieldDelimiter, ObjectMapper objectMapper)
            throws IOException {
        String text;
        if (value == null) {
            text = "";
        } else if (value instanceof String stringValue) {
            text = stringValue;
        } else if (value instanceof Number || value instanceof Boolean) {
            text = String.valueOf(value);
        } else {
            text = objectMapper.writeValueAsString(value);
        }
        if (text.contains("\"") || text.contains("\n") || text.contains("\r") || text.contains(fieldDelimiter)) {
            return "\"" + text.replace("\"", "\"\"") + "\"";
        }
        return text;
    }

    private static Map<String, Object> selectFields(Map<String, Object> fields, List<String> columns) {
        List<String> resolvedColumns = resolveColumns(fields, columns);
        Map<String, Object> selected = new LinkedHashMap<>();
        for (String column : resolvedColumns) {
            if (fields.containsKey(column)) {
                selected.put(column, fields.get(column));
            }
        }
        return selected;
    }

    private static boolean hasScheme(String path) {
        try {
            URI uri = URI.create(path);
            return uri.getScheme() != null && uri.getScheme().matches("[A-Za-z][A-Za-z0-9+.-]*");
        } catch (IllegalArgumentException exception) {
            String lower = path.toLowerCase(Locale.ROOT);
            return lower.startsWith("hdfs://") || lower.startsWith("viewfs://") || lower.startsWith("file://");
        }
    }
}
