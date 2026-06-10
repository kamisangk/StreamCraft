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

class PipelineRuntimeValidatorJdbcSourceContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PipelineRuntimeValidator validator = new PipelineRuntimeValidator();

    @Test
    void runValidationAllowsJdbcSourceFullMode() {
        assertDoesNotThrow(() -> validator.validate(definitionWithJdbcSource("""
                {
                  "url": "jdbc:postgresql://127.0.0.1:5432/dw",
                  "driver": "org.postgresql.Driver",
                  "query": "select id, name from dim_customer",
                  "readMode": "FULL",
                  "fetchSize": 500
                }
                """)));
    }

    @Test
    void runValidationRejectsJdbcIncrementalSourceWithoutCursorField() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithJdbcSource("""
                        {
                          "url": "jdbc:postgresql://127.0.0.1:5432/dw",
                          "driver": "org.postgresql.Driver",
                          "query": "select id, updated_at from orders",
                          "readMode": "INCREMENTAL"
                        }
                        """)));

        assertTrue(exception.getMessage().contains("cursorField"));
    }

    private PipelineDefinition definitionWithJdbcSource(String sourceConfigJson) {
        return new PipelineDefinition(
                "pipeline-jdbc-source",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Jdbc Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.JDBC_SOURCE,
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
