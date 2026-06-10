package com.streamcraft.shared.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class ElasticsearchSinkConfigParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesSinkWithCurrentStudioOptions() throws Exception {
        ElasticsearchSinkConfig config = ElasticsearchSinkConfigParser.parse(json("""
                {
                  "hosts": ["localhost:9200"],
                  "index": "orders-${region}",
                  "primaryKeys": ["id", "tenant_id"],
                  "keyDelimiter": "$",
                  "maxBatchSize": 200,
                  "maxRetryCount": 5,
                  "authType": "api_key_encoded",
                  "apiKeyEncoded": "encoded-key"
                }
                """), IllegalArgumentException::new);

        assertEquals(List.of("http://localhost:9200"), config.hosts());
        assertEquals("orders-${region}", config.index());
        assertEquals(List.of("id", "tenant_id"), config.primaryKeys());
        assertEquals("$", config.keyDelimiter());
        assertEquals(200, config.maxBatchSize());
        assertEquals(5, config.maxRetryCount());
        assertEquals(ElasticsearchSinkConfig.AuthType.API_KEY_ENCODED, config.authType());
        assertEquals("encoded-key", config.apiKeyEncoded());
    }

    @Test
    void rejectsSinkWithoutHosts() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ElasticsearchSinkConfigParser.parse(json("""
                        {
                          "index": "orders"
                        }
                        """), IllegalArgumentException::new));

        assertEquals("Elasticsearch Sink config hosts is required.", exception.getMessage());
    }

    @Test
    void allowsIndexDefinedEntirelyByFieldPlaceholder() throws Exception {
        ElasticsearchSinkConfig config = ElasticsearchSinkConfigParser.parse(json("""
                {
                  "hosts": ["localhost:9200"],
                  "index": "${table_name}"
                }
                """), IllegalArgumentException::new);

        assertEquals("${table_name}", config.index());
    }

    @Test
    void rejectsUnsafePrimaryKeyField() throws Exception {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ElasticsearchSinkConfigParser.parse(json("""
                        {
                          "hosts": ["http://127.0.0.1:9200"],
                          "index": "orders",
                          "primaryKeys": ["id;delete"]
                        }
                        """), IllegalArgumentException::new));

        assertEquals("Elasticsearch Sink config primary key field is invalid: id;delete", exception.getMessage());
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
