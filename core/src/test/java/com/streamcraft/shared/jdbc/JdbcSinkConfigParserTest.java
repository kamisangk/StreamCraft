package com.streamcraft.shared.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class JdbcSinkConfigParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesInsertSink() throws Exception {
        JdbcSinkConfig config = JdbcSinkConfigParser.parse(json("""
                {
                  "url": "jdbc:postgresql://127.0.0.1:5432/dw",
                  "driver": "org.postgresql.Driver",
                  "username": "dw_user",
                  "password": "secret",
                  "tablePath": "dwd_orders",
                  "writeMode": "INSERT",
                  "fields": ["id", "customer_name", "amount"],
                  "batchSize": 500,
                  "flushIntervalMillis": 3000
                }
                """), IllegalArgumentException::new);

        assertEquals("jdbc:postgresql://127.0.0.1:5432/dw", config.url());
        assertEquals("org.postgresql.Driver", config.driver());
        assertEquals("dw_user", config.username());
        assertEquals("dwd_orders", config.tablePath());
        assertEquals(JdbcSinkConfig.WriteMode.INSERT, config.writeMode());
        assertEquals(List.of("id", "customer_name", "amount"), config.fields());
        assertEquals(500, config.batchSize());
        assertEquals(3000, config.flushIntervalMillis());
    }

    @Test
    void parsesUpsertSinkWithKeyFields() throws Exception {
        JdbcSinkConfig config = JdbcSinkConfigParser.parse(json("""
                {
                  "url": "jdbc:mysql://127.0.0.1:3306/dw",
                  "driver": "com.mysql.cj.jdbc.Driver",
                  "tablePath": "dwd_orders",
                  "writeMode": "UPSERT",
                  "fields": ["id", "customer_name", "amount"],
                  "keyFields": ["id"]
                }
                """), IllegalArgumentException::new);

        assertEquals(JdbcSinkConfig.WriteMode.UPSERT, config.writeMode());
        assertEquals(List.of("id"), config.keyFields());
        assertEquals(JdbcSinkConfig.DEFAULT_BATCH_SIZE, config.batchSize());
        assertEquals(JdbcSinkConfig.DEFAULT_FLUSH_INTERVAL_MILLIS, config.flushIntervalMillis());
    }

    @Test
    void rejectsUpsertWithoutKeyFields() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> JdbcSinkConfigParser.parse(json("""
                        {
                          "url": "jdbc:mysql://127.0.0.1:3306/dw",
                          "driver": "com.mysql.cj.jdbc.Driver",
                          "tablePath": "dwd_orders",
                          "writeMode": "UPSERT",
                          "fields": ["id", "customer_name", "amount"]
                        }
                        """), IllegalArgumentException::new));

        assertEquals("JDBC Sink config keyFields is required for UPSERT write mode.", exception.getMessage());
    }

    @Test
    void rejectsUnsafeFieldName() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> JdbcSinkConfigParser.parse(json("""
                        {
                          "url": "jdbc:mysql://127.0.0.1:3306/dw",
                          "driver": "com.mysql.cj.jdbc.Driver",
                          "tablePath": "dwd_orders",
                          "writeMode": "INSERT",
                          "fields": ["id;drop table orders"]
                        }
                        """), IllegalArgumentException::new));

        assertEquals("JDBC Sink config field is invalid: id;drop table orders", exception.getMessage());
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
