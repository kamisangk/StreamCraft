package com.streamcraft.shared.influxdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class InfluxDbSourceConfigParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesSourceWithCurrentStudioOptions() throws Exception {
        InfluxDbSourceConfig config = InfluxDbSourceConfigParser.parse(json("""
                {
                  "url": "localhost:8086",
                  "database": "metrics",
                  "sql": "select * from cpu",
                  "schema": {"fields": {"host": "string", "usage": "double"}},
                  "epoch": "ms",
                  "queryTimeoutSeconds": 30,
                  "connectTimeoutMillis": 2000,
                  "readMode": "INCREMENTAL",
                  "cursorField": "time",
                  "cursorType": "LONG",
                  "initialCursorValue": "1000",
                  "pollIntervalMillis": 10,
                  "maxPolls": 1,
                  "username": "root",
                  "password": "secret"
                }
                """), IllegalArgumentException::new);

        assertEquals("http://localhost:8086", config.url());
        assertEquals("metrics", config.database());
        assertEquals("select * from cpu", config.sql());
        assertTrue(config.schemaJson().contains("\"usage\":\"double\""));
        assertEquals("ms", config.epoch());
        assertEquals(30, config.queryTimeoutSeconds());
        assertEquals(2000, config.connectTimeoutMillis());
        assertEquals(InfluxDbSourceConfig.ReadMode.INCREMENTAL, config.readMode());
        assertEquals("time", config.cursorField());
        assertEquals(InfluxDbSourceConfig.CursorType.LONG, config.cursorType());
        assertEquals("1000", config.initialCursorValue());
        assertEquals(10, config.pollIntervalMillis());
        assertEquals(1, config.maxPolls());
        assertEquals("root", config.username());
        assertEquals("secret", config.password());
    }

    @Test
    void rejectsSourceWithoutSql() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> InfluxDbSourceConfigParser.parse(json("""
                        {
                          "url": "http://localhost:8086",
                          "database": "metrics"
                        }
                        """), IllegalArgumentException::new));

        assertEquals("InfluxDB Source config sql is required.", exception.getMessage());
    }

    @Test
    void rejectsMultiStatementSql() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> InfluxDbSourceConfigParser.parse(json("""
                        {
                          "url": "http://localhost:8086",
                          "database": "metrics",
                          "sql": "select * from cpu; drop measurement cpu"
                        }
                        """), IllegalArgumentException::new));

        assertEquals("InfluxDB Source config sql must be a single SELECT statement without semicolons.",
                exception.getMessage());
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
