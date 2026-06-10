package com.streamcraft.core.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import java.util.List;
import org.junit.jupiter.api.Test;

class PipelineRuntimeValidatorElasticsearchSourceContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PipelineRuntimeValidator validator = new PipelineRuntimeValidator();

    @Test
    void runValidationAllowsElasticsearchSourceFullMode() {
        assertDoesNotThrow(() -> validator.validate(definitionWithElasticsearchSource("""
                {
                  "hosts": ["http://127.0.0.1:9200"],
                  "index": "orders-*",
                  "source": ["_id", "customerName", "amount"],
                  "query": {"match_all": {}},
                  "readMode": "FULL",
                  "scrollSize": 500
                }
                """)));
    }

    @Test
    void runValidationRejectsElasticsearchIncrementalSourceWithoutCursorField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithElasticsearchSource("""
                        {
                          "hosts": ["http://127.0.0.1:9200"],
                          "index": "orders",
                          "readMode": "INCREMENTAL"
                        }
                        """)));

        assertTrue(exception.getMessage().contains("cursorField"));
    }

    private PipelineDefinition definitionWithElasticsearchSource(String sourceConfigJson) {
        return new PipelineDefinition(
                "pipeline-elasticsearch-source",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Elasticsearch Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.ELASTICSEARCH_SOURCE,
                                jsonNode(sourceConfigJson)),
                        new PipelineNode(
                                "sink-1",
                                "Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.KAFKA_SINK,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topic": "orders-out",
                                          "deliveryGuarantee": "AT_LEAST_ONCE",
                                          "authType": "NONE",
                                          "format": "JSON"
                                        }
                                        """))),
                List.of(new PipelineEdge("edge-1", "source-1", "output-0", "sink-1", "input-0")));
    }

    private JsonNode jsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
