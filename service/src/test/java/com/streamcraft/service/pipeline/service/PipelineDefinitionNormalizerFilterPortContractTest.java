package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionNormalizerFilterPortContractTest {

    private final PipelineDefinitionNormalizer normalizer = new PipelineDefinitionNormalizer(new ObjectMapper());

    @Test
    void missingSourcePortRemainsUnset() {
        JsonNode normalized = normalizer.normalizeTree("""
                {
                  "nodes": [
                    {"id": "source-1", "type": "SOURCE", "operator": "KAFKA_SOURCE"},
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {"id": "edge-1", "sourceNodeId": "source-1", "targetNodeId": "sink-1"}
                  ]
                }
                """);

        JsonNode edge = normalized.path("edges").path(0);
        assertFalse(edge.has("sourcePortId"));
    }

    @Test
    void filterEdgeWithMissingSourcePortRemainsUnset() {
        JsonNode normalized = normalizer.normalizeTree("""
                {
                  "nodes": [
                    {"id": "filter-1", "type": "TRANSFORM", "operator": "FILTER"},
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {"id": "edge-1", "sourceNodeId": "filter-1", "targetNodeId": "sink-1"}
                  ]
                }
                """);

        JsonNode edge = normalized.path("edges").path(0);
        assertFalse(edge.has("sourcePortId"));
    }

    @Test
    void missingTargetPortRemainsUnset() {
        JsonNode normalized = normalizer.normalizeTree("""
                {
                  "nodes": [
                    {"id": "filter-1", "type": "TRANSFORM", "operator": "FILTER"},
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {"id": "edge-1", "sourceNodeId": "filter-1", "sourcePortId": "true", "targetNodeId": "sink-1"}
                  ]
                }
                """);

        JsonNode edge = normalized.path("edges").path(0);
        assertFalse(edge.has("targetPortId"));
    }

    @Test
    void explicitPortsRemainUnchanged() {
        JsonNode normalized = normalizer.normalizeTree("""
                {
                  "nodes": [
                    {"id": "source-1", "type": "SOURCE", "operator": "KAFKA_SOURCE"},
                    {"id": "sink-1", "type": "SINK", "operator": "KAFKA_SINK"}
                  ],
                  "edges": [
                    {
                      "id": "edge-1",
                      "sourceNodeId": "source-1",
                      "sourcePortId": "output-0",
                      "targetNodeId": "sink-1",
                      "targetPortId": "input-0"
                    }
                  ]
                }
                """);

        JsonNode edge = normalized.path("edges").path(0);
        assertEquals("output-0", edge.path("sourcePortId").asText());
        assertEquals("input-0", edge.path("targetPortId").asText());
    }
}
