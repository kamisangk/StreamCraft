package com.streamcraft.shared.influxdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class InfluxDbSinkConfigParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesSinkWithCurrentStudioOptions() throws Exception {
        InfluxDbSinkConfig config = InfluxDbSinkConfigParser.parse(json("""
                {
                  "url": "localhost:8086",
                  "database": "metrics",
                  "measurement": "cpu_${region}",
                  "keyTime": "event_time",
                  "keyTags": ["host", "region"],
                  "fields": ["usage", "load"],
                  "batchSize": 200,
                  "maxRetries": 5,
                  "retryBackoffMultiplierMillis": 200,
                  "maxRetryBackoffMillis": 2000,
                  "connectTimeoutMillis": 3000,
                  "flushIntervalMillis": 1000,
                  "username": "root",
                  "password": "secret"
                }
                """), IllegalArgumentException::new);

        assertEquals("http://localhost:8086", config.url());
        assertEquals("metrics", config.database());
        assertEquals("cpu_${region}", config.measurement());
        assertEquals("event_time", config.keyTime());
        assertEquals(List.of("host", "region"), config.keyTags());
        assertEquals(List.of("usage", "load"), config.fields());
        assertEquals(200, config.batchSize());
        assertEquals(5, config.maxRetries());
        assertEquals(200, config.retryBackoffMultiplierMillis());
        assertEquals(2000, config.maxRetryBackoffMillis());
        assertEquals(3000, config.connectTimeoutMillis());
        assertEquals(1000, config.flushIntervalMillis());
        assertEquals("root", config.username());
        assertEquals("secret", config.password());
    }

    @Test
    void rejectsSinkWithoutMeasurement() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> InfluxDbSinkConfigParser.parse(json("""
                        {
                          "url": "http://localhost:8086",
                          "database": "metrics"
                        }
                        """), IllegalArgumentException::new));

        assertEquals("InfluxDB Sink config measurement is required.", exception.getMessage());
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
