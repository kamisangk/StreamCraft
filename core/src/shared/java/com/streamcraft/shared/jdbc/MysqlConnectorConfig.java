package com.streamcraft.shared.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Locale;
import java.util.function.Function;
import java.util.regex.Pattern;

public final class MysqlConnectorConfig {
    private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_$]+");

    private MysqlConnectorConfig() {
    }

    public static JdbcSourceConfig parseSource(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseSourceValidated(config, issue -> error.apply(issue.defaultMessage()));
    }

    public static JdbcSourceConfig parseSourceValidated(JsonNode config, JdbcSourceConfigParser.ValidationErrorFactory error) {
        ObjectNode jdbc = normalizeConnection(config, (field, value) -> error.apply(invalidSourceField(field, value)));
        String table = jdbc.path("tablePath").asText("").trim();
        if (!table.isEmpty()) {
            String[] parts = table.split("\\.", -1);
            if (parts.length > 2 || java.util.Arrays.stream(parts).anyMatch(part -> !IDENTIFIER.matcher(part).matches())) {
                throw error.apply(invalidSourceField("tablePath", table));
            }
            jdbc.put("tablePath", parts.length == 1 ? quote(parts[0]) : quote(parts[0]) + "." + quote(parts[1]));
        }
        int partitions = jdbc.path("partitionNum").asInt(1);
        if (jdbc.hasNonNull("partitionNum") && (!jdbc.get("partitionNum").isIntegralNumber()
                || !jdbc.get("partitionNum").canConvertToInt() || partitions < 1)) {
            throw error.apply(invalidSourceField("partitionNum", jdbc.path("partitionNum").asText()));
        }
        jdbc.put("splitCount", partitions);
        jdbc.set("splitColumn", jdbc.path("partitionColumn"));
        jdbc.set("splitLowerBound", jdbc.path("partitionLowerBound"));
        jdbc.set("splitUpperBound", jdbc.path("partitionUpperBound"));
        if (partitions > 1) {
            jdbc.put("cursorType", "LONG");
        }
        JdbcSourceConfig parsed = JdbcSourceConfigParser.parseValidated(jdbc, error);
        return new JdbcSourceConfig(parsed.url(), parsed.driver(), parsed.username(), jdbc.path("password").asText(""),
                parsed.query(), parsed.tablePath(), parsed.readMode(), parsed.cursorField(), parsed.cursorType(),
                parsed.initialCursorValue(), parsed.pollIntervalMillis(), parsed.fetchSize(), parsed.maxPolls(),
                parsed.idField(), parsed.timestampField(), parsed.splitCount(), parsed.splitColumn(),
                parsed.splitLowerBound(), parsed.splitUpperBound());
    }

    private static JdbcSourceConfigParser.ValidationError invalidSourceField(String field, String value) {
        return new JdbcSourceConfigParser.ValidationError("pipeline.validation.mysql.invalidField",
                "MySQL connector config " + field + " is invalid.", field);
    }

    private static ObjectNode normalizeConnection(JsonNode config, ConnectionError error) {
        ObjectNode jdbc = config instanceof ObjectNode object ? object.deepCopy() : JsonNodeFactory.instance.objectNode();
        String url = jdbc.path("url").asText("").trim();
        if (!url.toLowerCase(Locale.ROOT).startsWith("jdbc:mysql://") || url.length() <= "jdbc:mysql://".length()) {
            throw error.create("url", url);
        }
        jdbc.put("driver", DRIVER);
        return jdbc;
    }

    private static String quote(String identifier) {
        return "`" + identifier + "`";
    }

    @FunctionalInterface
    private interface ConnectionError {
        IllegalArgumentException create(String field, String value);
    }
}
