package com.streamcraft.core.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import com.streamcraft.core.runtime.ExecutionMode;
import java.util.List;
import org.junit.jupiter.api.Test;

class PipelineRuntimeValidatorRoutePortTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PipelineRuntimeValidator validator = new PipelineRuntimeValidator();

    @Test
    void rejectsUnknownPortBeforeRunOrPreviewBuildsTopology() throws Exception {
        PipelineDefinition definition = definition("typo", "null");

        for (ExecutionMode mode : List.of(ExecutionMode.RUN, ExecutionMode.PREVIEW)) {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> validator.validate(definition, mode));
            assertTrue(error.getMessage().contains("route-edge uses unsupported source port typo for source operator ROUTE"));
            assertTrue(error.getMessage().contains("priority"));
            assertTrue(error.getMessage().contains("unmatched"));
        }
    }

    @Test
    void allowsDeclaredPortsAndOnlyEnablesUnmatchedWhenConfigured() throws Exception {
        PipelineDefinition declared = definition("priority", "false");
        PipelineDefinition defaultUnmatched = definition("unmatched", "null");
        PipelineDefinition disabledUnmatched = definition("unmatched", "false");

        assertDoesNotThrow(() -> validator.validate(declared));
        assertDoesNotThrow(() -> validator.validate(defaultUnmatched, ExecutionMode.PREVIEW));
        assertThrows(IllegalArgumentException.class, () -> validator.validate(disabledUnmatched));
    }

    private PipelineDefinition definition(String port, String includeUnmatched) throws Exception {
        return new PipelineDefinition("pipeline-1", List.of(
                new PipelineNode("source", "source", PipelineNodeType.SOURCE, PipelineOperator.KAFKA_SOURCE,
                        objectMapper.readTree("""
                                {"bootstrapServers":"127.0.0.1:9092","topics":["input"],"groupId":"group",
                                 "consumeMode":"earliest","authType":"NONE","format":"JSON","sampleData":["{}"]}
                                """)),
                new PipelineNode("route", "route", PipelineNodeType.TRANSFORM, PipelineOperator.ROUTE,
                        objectMapper.readTree("""
                                {"routes":[{"portId":"priority","condition":"value > 0"}],"includeUnmatched":%s}
                                """.formatted(includeUnmatched))),
                new PipelineNode("sink", "sink", PipelineNodeType.SINK, PipelineOperator.KAFKA_SINK,
                        objectMapper.readTree("""
                                {"bootstrapServers":"127.0.0.1:9092","topic":"output","authType":"NONE",
                                 "deliveryGuarantee":"AT_LEAST_ONCE","format":"JSON"}
                                """))),
                List.of(new PipelineEdge("input-edge", "source", "records", "route", "records"),
                        new PipelineEdge("route-edge", "route", port, "sink", "records")));
    }
}
