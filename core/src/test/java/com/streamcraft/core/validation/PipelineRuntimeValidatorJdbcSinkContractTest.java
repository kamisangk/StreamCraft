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

class PipelineRuntimeValidatorJdbcSinkContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PipelineRuntimeValidator validator = new PipelineRuntimeValidator();

    @Test
    void runValidationAllowsJdbcSinkInsertMode() {
        assertDoesNotThrow(() -> validator.validate(definitionWithJdbcSink("""
                {
                  "url": "jdbc:postgresql://127.0.0.1:5432/dw",
                  "driver": "org.postgresql.Driver",
                  "tablePath": "dwd_orders",
                  "writeMode": "INSERT",
                  "fields": ["id", "customer_name", "amount"],
                  "batchSize": 500
                }
                """)));
    }

    @Test
    void runValidationRejectsJdbcUpsertSinkWithoutKeyFields() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validate(definitionWithJdbcSink("""
                        {
                          "url": "jdbc:postgresql://127.0.0.1:5432/dw",
                          "driver": "org.postgresql.Driver",
                          "tablePath": "dwd_orders",
                          "writeMode": "UPSERT",
                          "fields": ["id", "customer_name", "amount"]
                        }
                        """)));

        assertTrue(exception.getMessage().contains("keyFields"));
    }

    private PipelineDefinition definitionWithJdbcSink(String sinkConfigJson) {
        return new PipelineDefinition(
                "pipeline-jdbc-sink",
                List.of(
                        new PipelineNode(
                                "source-1",
                                "Source",
                                PipelineNodeType.SOURCE,
                                PipelineOperator.KAFKA_SOURCE,
                                jsonNode("""
                                        {
                                          "bootstrapServers": "127.0.0.1:9092",
                                          "topics": ["orders"],
                                          "groupId": "group-1",
                                          "consumeMode": "earliest",
                                          "authType": "NONE",
                                          "format": "JSON"
                                        }
                                        """)),
                        new PipelineNode(
                                "sink-1",
                                "Jdbc Sink",
                                PipelineNodeType.SINK,
                                PipelineOperator.JDBC_SINK,
                                jsonNode(sinkConfigJson))),
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
