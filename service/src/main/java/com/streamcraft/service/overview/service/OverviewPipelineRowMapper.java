package com.streamcraft.service.overview.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.overview.web.OverviewResponse;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.service.PipelineRuntimeView;
import java.util.ArrayList;
import java.util.List;

final class OverviewPipelineRowMapper {

    private static final String METRICS_UNAVAILABLE = "Metrics unavailable";

    private final ObjectMapper objectMapper;

    OverviewPipelineRowMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    OverviewResponse.PipelineRow toResponse(PipelineRuntimeView runtimeView) {
        Pipeline pipeline = runtimeView.pipeline();
        Labels labels = parseLabels(pipeline.getDefinitionJson());
        return new OverviewResponse.PipelineRow(
                pipeline.getId(),
                pipeline.getName(),
                labels.sourceLabels,
                labels.sinkLabels,
                pipeline.getLastRunStatus() == null ? null : pipeline.getLastRunStatus().name(),
                runtimeView.runtimeTargetLabel(),
                runtimeView.durationMillis(),
                runtimeView.metricsAvailable(),
                metricsUnavailableReason(runtimeView),
                pipeline.getUpdatedAt());
    }

    private String metricsUnavailableReason(PipelineRuntimeView runtimeView) {
        Pipeline pipeline = runtimeView.pipeline();
        if (runtimeView.running()
                && hasText(pipeline.getLastJobId())
                && runtimeView.runtimeTargetUnavailable()) {
            return METRICS_UNAVAILABLE;
        }
        if (runtimeView.metricsEligible() && !runtimeView.metricsAvailable()) {
            return METRICS_UNAVAILABLE;
        }
        return null;
    }

    private Labels parseLabels(String definitionJson) {
        if (definitionJson == null || definitionJson.isBlank()) {
            return new Labels(List.of(), List.of());
        }
        try {
            JsonNode root = objectMapper.readTree(definitionJson);
            JsonNode nodes = root.path("nodes");
            if (nodes == null || !nodes.isArray()) {
                return new Labels(List.of(), List.of());
            }

            List<String> sourceLabels = new ArrayList<>();
            List<String> sinkLabels = new ArrayList<>();
            for (JsonNode node : nodes) {
                String type = node.path("type").asText();
                String label = nodeLabel(node);
                if (label == null) {
                    continue;
                }
                if ("SOURCE".equalsIgnoreCase(type)) {
                    sourceLabels.add(label);
                } else if ("SINK".equalsIgnoreCase(type)) {
                    sinkLabels.add(label);
                }
            }
            return new Labels(sourceLabels, sinkLabels);
        } catch (Exception exception) {
            return new Labels(List.of(), List.of());
        }
    }

    private String nodeLabel(JsonNode node) {
        if (node == null || node.isMissingNode()) {
            return null;
        }
        String operator = node.path("operator").asText();
        JsonNode config = node.path("config");

        if ("KAFKA_SOURCE".equals(operator)) {
            JsonNode topics = config.path("topics");
            if (topics.isArray() && !topics.isEmpty()) {
                String topic = topics.get(0).asText();
                if (!topic.isBlank()) {
                    return "Kafka (" + topic + ")";
                }
            }
        }
        if ("KAFKA_SINK".equals(operator)) {
            String topic = config.path("topic").asText();
            if (!topic.isBlank()) {
                return "Kafka (" + topic + ")";
            }
        }
        if ("HDFS_FILE_SOURCE".equals(operator) || "HDFS_FILE_SINK".equals(operator)) {
            String path = config.path("path").asText();
            if (!path.isBlank()) {
                return "HDFS File (" + path + ")";
            }
        }
        if ("ELASTICSEARCH_SINK".equals(operator)) {
            String index = config.path("index").asText();
            if (!index.isBlank()) {
                return "Elasticsearch (" + index + ")";
            }
        }
        if ("INFLUXDB_SINK".equals(operator)) {
            String measurement = config.path("measurement").asText();
            if (!measurement.isBlank()) {
                return "InfluxDB (" + measurement + ")";
            }
        }
        String name = node.path("name").asText();
        return name.isBlank() ? null : name;
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private record Labels(List<String> sourceLabels, List<String> sinkLabels) {
    }
}
