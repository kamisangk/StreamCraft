package com.streamcraft.service.overview.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.runtime.model.RuntimeTargetStatus;
import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import com.streamcraft.service.runtime.service.FlinkRuntimeTargetService;
import com.streamcraft.service.overview.web.OverviewResponse;
import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.service.PipelineRuntimeView;
import com.streamcraft.service.pipeline.service.PipelineService;
import com.streamcraft.service.pipeline.service.PipelineRuntimeSnapshot;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class OverviewService {

    private static final String METRICS_UNAVAILABLE = "Metrics unavailable";

    private final FlinkRuntimeTargetService runtimeTargetService;
    private final PipelineService pipelineService;
    private final ObjectMapper objectMapper;

    public OverviewService(
            FlinkRuntimeTargetService runtimeTargetService,
            PipelineService pipelineService,
            ObjectMapper objectMapper) {
        this.runtimeTargetService = runtimeTargetService;
        this.pipelineService = pipelineService;
        this.objectMapper = objectMapper;
    }

    public OverviewResponse getOverview() {
        FlinkRuntimeTarget runtimeTarget = runtimeTargetService.findTarget().orElse(null);
        List<PipelineRuntimeSnapshot> runtimeSnapshots = pipelineService.listRuntimeSnapshots();
        List<Pipeline> pipelines = runtimeSnapshots.stream()
                .map(PipelineRuntimeSnapshot::pipeline)
                .toList();

        List<OverviewResponse.RuntimeTargetCapacity> runtimeTargetCapacities = runtimeTarget == null
                ? List.of()
                : List.of(toCapacity(runtimeTarget));

        SnapshotAccumulator snapshot = new SnapshotAccumulator();
        List<OverviewResponse.PipelineRow> pipelineRows = new ArrayList<>();
        for (PipelineRuntimeSnapshot runtimeSnapshot : runtimeSnapshots) {
            pipelineRows.add(buildPipelineRow(PipelineRuntimeView.of(runtimeSnapshot, runtimeTarget), snapshot));
        }
        pipelineRows.sort(Comparator.comparing(
                        OverviewResponse.PipelineRow::updatedAt,
                        Comparator.nullsLast(Comparator.reverseOrder()))
                .thenComparing(OverviewResponse.PipelineRow::pipelineId, Comparator.nullsLast(Comparator.naturalOrder())));

        OverviewResponse.RuntimeSnapshot runtimeSnapshot = new OverviewResponse.RuntimeSnapshot(
                snapshot.totalInputRecords,
                snapshot.totalOutputRecords,
                snapshot.includedPipelineCount,
                snapshot.missingPipelineCount);

        OverviewResponse.Summary summary = new OverviewResponse.Summary(
                runtimeTarget == null ? 0 : 1,
                runtimeTarget != null && runtimeTarget.getStatus() == RuntimeTargetStatus.CONNECTED ? 1 : 0,
                pipelines.size(),
                (int) pipelines.stream().filter(p -> p.getLastRunStatus() == PipelineRunStatus.RUNNING).count(),
                (int) pipelines.stream().filter(p -> p.getLastRunStatus() != PipelineRunStatus.RUNNING).count(),
                snapshot.unhealthyPipelineCount,
                pipelines.stream()
                        .map(Pipeline::getLastSubmittedAt)
                        .filter(i -> i != null)
                        .max(Comparator.naturalOrder())
                        .orElse(null));

        return new OverviewResponse(summary, runtimeSnapshot, runtimeTargetCapacities, pipelineRows);
    }

    private OverviewResponse.RuntimeTargetCapacity toCapacity(FlinkRuntimeTarget runtimeTarget) {
        int totalSlots = runtimeTarget.getTotalSlots() == null ? 0 : runtimeTarget.getTotalSlots();
        int availableSlots = runtimeTarget.getAvailableSlots() == null ? 0 : runtimeTarget.getAvailableSlots();
        int usedSlots = totalSlots - availableSlots;
        Integer usagePercent = null;
        if (runtimeTarget.getStatus() == RuntimeTargetStatus.CONNECTED) {
            usagePercent = totalSlots == 0 ? 0 : (int) Math.round((usedSlots * 100.0) / totalSlots);
        }

        return new OverviewResponse.RuntimeTargetCapacity(
                runtimeTarget.getId(),
                runtimeTarget.getType() == null ? "Flink runtime" : runtimeTarget.getType().name(),
                runtimeTarget.getStatus() == null ? null : runtimeTarget.getStatus().name(),
                totalSlots,
                availableSlots,
                usedSlots,
                usagePercent,
                runtimeTarget.getStatusMessage());
    }

    private OverviewResponse.PipelineRow buildPipelineRow(
            PipelineRuntimeView runtimeView,
            SnapshotAccumulator snapshot) {
        Pipeline pipeline = runtimeView.pipeline();
        Labels labels = parseLabels(pipeline.getDefinitionJson());
        boolean running = runtimeView.running();
        boolean metricsEligible = runtimeView.metricsEligible();
        boolean metricsAvailable = runtimeView.metricsAvailable();
        String metricsUnavailableReason = null;
        Long durationMillis = runtimeView.durationMillis();
        PipelineAggregate aggregate = new PipelineAggregate(
                runtimeView.totalInputRecords(),
                runtimeView.totalOutputRecords());

        if (running
                && pipeline.getLastJobId() != null
                && !pipeline.getLastJobId().isBlank()
                && runtimeView.runtimeTargetUnavailable()) {
            metricsUnavailableReason = METRICS_UNAVAILABLE;
        } else if (metricsEligible && !metricsAvailable) {
            metricsUnavailableReason = METRICS_UNAVAILABLE;
        }

        if (running) {
            if (metricsAvailable) {
                snapshot.totalInputRecords += aggregate.inputRecords;
                snapshot.totalOutputRecords += aggregate.outputRecords;
                snapshot.includedPipelineCount++;
            } else {
                snapshot.missingPipelineCount++;
            }
        }

        if (pipeline.getLastRunStatus() == PipelineRunStatus.FAILED
                || runtimeView.runtimeTargetUnavailable()
                || (running && !metricsAvailable)) {
            snapshot.unhealthyPipelineCount++;
        }

        return new OverviewResponse.PipelineRow(
                pipeline.getId(),
                pipeline.getName(),
                labels.sourceLabels,
                labels.sinkLabels,
                pipeline.getLastRunStatus() == null ? null : pipeline.getLastRunStatus().name(),
                runtimeView.runtimeTargetLabel(),
                durationMillis,
                metricsAvailable,
                metricsUnavailableReason,
                pipeline.getUpdatedAt());
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
        } catch (Exception ex) {
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

    private record Labels(List<String> sourceLabels, List<String> sinkLabels) {
    }

    private record PipelineAggregate(long inputRecords, long outputRecords) {
    }

    private static class SnapshotAccumulator {
        private long totalInputRecords;
        private long totalOutputRecords;
        private int includedPipelineCount;
        private int missingPipelineCount;
        private int unhealthyPipelineCount;
    }
}

