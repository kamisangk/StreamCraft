package com.streamcraft.shared.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class JdbcSourceConfigParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesFullQuerySource() throws Exception {
        JdbcSourceConfig config = JdbcSourceConfigParser.parse(json("""
                {
                  "url": "jdbc:postgresql://127.0.0.1:5432/dw",
                  "driver": "org.postgresql.Driver",
                  "username": "dw_user",
                  "password": "secret",
                  "query": "select id, name from dim_customer",
                  "readMode": "FULL",
                  "fetchSize": 500
                }
                """), IllegalArgumentException::new);

        assertEquals("jdbc:postgresql://127.0.0.1:5432/dw", config.url());
        assertEquals("org.postgresql.Driver", config.driver());
        assertEquals("dw_user", config.username());
        assertEquals("select id, name from dim_customer", config.query());
        assertEquals(JdbcSourceConfig.ReadMode.FULL, config.readMode());
        assertEquals(500, config.fetchSize());
    }

    @Test
    void parsesIncrementalSourceWithCursorSettings() throws Exception {
        JdbcSourceConfig config = JdbcSourceConfigParser.parse(json("""
                {
                  "url": "jdbc:mysql://127.0.0.1:3306/ods",
                  "driver": "com.mysql.cj.jdbc.Driver",
                  "tablePath": "orders",
                  "readMode": "INCREMENTAL",
                  "cursorField": "updated_at",
                  "cursorType": "TIMESTAMP",
                  "initialCursorValue": "2026-05-16T00:00:00Z",
                  "pollIntervalMillis": 3000,
                  "maxPolls": 7
                }
                """), IllegalArgumentException::new);

        assertEquals("select * from orders", config.resolvedQuery());
        assertEquals(JdbcSourceConfig.ReadMode.INCREMENTAL, config.readMode());
        assertEquals("updated_at", config.cursorField());
        assertEquals(JdbcSourceConfig.CursorType.TIMESTAMP, config.cursorType());
        assertEquals("2026-05-16T00:00:00Z", config.initialCursorValue());
        assertEquals(3000, config.pollIntervalMillis());
        assertEquals(7, config.maxPolls());
    }

    @Test
    void rejectsIncrementalSourceWithoutCursorField() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> JdbcSourceConfigParser.parse(json("""
                        {
                          "url": "jdbc:mysql://127.0.0.1:3306/ods",
                          "driver": "com.mysql.cj.jdbc.Driver",
                          "query": "select id from orders",
                          "readMode": "INCREMENTAL"
                        }
                        """), IllegalArgumentException::new));

        assertEquals("JDBC Source config cursorField is required for INCREMENTAL read mode.", exception.getMessage());
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
