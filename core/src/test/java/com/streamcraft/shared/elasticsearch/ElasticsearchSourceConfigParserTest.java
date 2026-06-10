package com.streamcraft.shared.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class ElasticsearchSourceConfigParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesFullSourceWithCurrentStudioOptions() throws Exception {
        ElasticsearchSourceConfig config = ElasticsearchSourceConfigParser.parse(json("""
                {
                  "hosts": ["http://127.0.0.1:9200"],
                  "index": "orders-*",
                  "source": ["_id", "customerName", "amount"],
                  "query": {"term": {"status": "paid"}},
                  "readMode": "FULL",
                  "scrollSize": 500,
                  "scrollTime": "2m"
                }
                """), IllegalArgumentException::new);

        assertEquals(List.of("http://127.0.0.1:9200"), config.hosts());
        assertEquals("orders-*", config.index());
        assertEquals(List.of("_id", "customerName", "amount"), config.sourceFields());
        assertEquals(ElasticsearchSourceConfig.ReadMode.FULL, config.readMode());
        assertEquals(500, config.scrollSize());
        assertEquals("2m", config.scrollTime());
        assertEquals("paid", config.query().path("term").path("status").asText());
    }

    @Test
    void parsesIncrementalSourceWithCursorSettings() throws Exception {
        ElasticsearchSourceConfig config = ElasticsearchSourceConfigParser.parse(json("""
                {
                  "hosts": "http://127.0.0.1:9200",
                  "index": "orders",
                  "readMode": "INCREMENTAL",
                  "cursorField": "updated_at",
                  "cursorType": "LONG",
                  "initialCursorValue": "100",
                  "pollIntervalMillis": 3000,
                  "maxPolls": 7
                }
                """), IllegalArgumentException::new);

        assertEquals(List.of("http://127.0.0.1:9200"), config.hosts());
        assertEquals(ElasticsearchSourceConfig.ReadMode.INCREMENTAL, config.readMode());
        assertEquals("updated_at", config.cursorField());
        assertEquals(ElasticsearchSourceConfig.CursorType.LONG, config.cursorType());
        assertEquals("100", config.initialCursorValue());
        assertEquals(3000, config.pollIntervalMillis());
        assertEquals(7, config.maxPolls());
    }

    @Test
    void rejectsIncrementalSourceWithoutCursorField() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ElasticsearchSourceConfigParser.parse(json("""
                        {
                          "hosts": ["http://127.0.0.1:9200"],
                          "index": "orders",
                          "readMode": "INCREMENTAL"
                        }
                        """), IllegalArgumentException::new));

        assertEquals(
                "Elasticsearch Source config cursorField is required for INCREMENTAL read mode.",
                exception.getMessage());
    }

    @Test
    void rejectsUnsafeIndexName() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ElasticsearchSourceConfigParser.parse(json("""
                        {
                          "hosts": ["http://127.0.0.1:9200"],
                          "index": "orders;delete",
                          "readMode": "FULL"
                        }
                        """), IllegalArgumentException::new));

        assertEquals("Elasticsearch Source config index is invalid: orders;delete", exception.getMessage());
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
