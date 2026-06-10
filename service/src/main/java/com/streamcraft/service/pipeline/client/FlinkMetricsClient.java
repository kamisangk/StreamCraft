package com.streamcraft.service.pipeline.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.pipeline.model.NodeMetrics;
import com.streamcraft.service.pipeline.model.PipelineMetrics;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

public class FlinkMetricsClient {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public FlinkMetricsClient(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
        this.objectMapper = new ObjectMapper();
    }

    public PipelineRunStatus getJobStatus(String flinkRestUrl, String jobId) {
        try {
            return mapFlinkStateToRunStatus(fetchJobNode(flinkRestUrl, jobId).path("state").asText());
        } catch (Exception exception) {
            throw new RuntimeException("Failed to fetch Flink job status: " + exception.getMessage(), exception);
        }
    }

    public PipelineMetrics getJobMetrics(String flinkRestUrl, String jobId, List<String> nodeIds, Map<String, String> nodeNames) {
        PipelineMetrics metrics = new PipelineMetrics();
        metrics.setJobId(jobId);

        try {
            JsonNode jobNode = fetchJobNode(flinkRestUrl, jobId);

            String state = jobNode.path("state").asText();
            metrics.setStatus(mapFlinkStateToRunStatus(state));

            long startTime = jobNode.path("start-time").asLong();
            if (startTime > 0) {
                metrics.setStartTime(Instant.ofEpochMilli(startTime));
                metrics.setDuration(System.currentTimeMillis() - startTime);
            }

            JsonNode vertices = jobNode.path("vertices");
            Map<String, String> vertexIdMap = new HashMap<>();

            for (JsonNode vertex : vertices) {
                String name = vertex.path("name").asText();
                String vertexId = vertex.path("id").asText();

                for (String nodeId : nodeIds) {
                    if (name.contains("metrics-" + nodeId)) {
                        vertexIdMap.put(nodeId, vertexId);
                    }
                }
            }

            List<NodeMetrics> nodeMetricsList = new ArrayList<>();
            for (String nodeId : nodeIds) {
                String vertexId = vertexIdMap.get(nodeId);
                if (vertexId != null) {
                    NodeMetrics nodeMetrics = getNodeMetrics(flinkRestUrl, jobId, vertexId, nodeId, nodeNames.get(nodeId));
                    nodeMetricsList.add(nodeMetrics);
                } else {
                    nodeMetricsList.add(new NodeMetrics(nodeId, nodeNames.get(nodeId), 0L, 0L));
                }
            }

            metrics.setNodeMetrics(nodeMetricsList);
        } catch (Exception exception) {
            throw new RuntimeException("Failed to fetch Flink metrics: " + exception.getMessage(), exception);
        }

        return metrics;
    }

    private JsonNode fetchJobNode(String flinkRestUrl, String jobId) throws Exception {
        String jobUrl = flinkRestUrl + "/jobs/" + jobId;
        String jobResponse = restTemplate.getForObject(jobUrl, String.class);
        return objectMapper.readTree(jobResponse);
    }

    private NodeMetrics getNodeMetrics(String flinkRestUrl, String jobId, String vertexId, String nodeId, String nodeName) {
        try {
            String metricsUrl = flinkRestUrl + "/jobs/" + jobId + "/vertices/" + vertexId + "/metrics";
            String metricsResponse = restTemplate.getForObject(metricsUrl, String.class);
            JsonNode metricsNode = objectMapper.readTree(metricsResponse);

            List<String> inputMetricIds = new ArrayList<>();
            List<String> outputMetricIds = new ArrayList<>();

            for (JsonNode metric : metricsNode) {
                String id = metric.path("id").asText();
                if (id.contains("streamcraft.node." + nodeId + ".input_records")) {
                    inputMetricIds.add(id);
                } else if (id.contains("streamcraft.node." + nodeId + ".output_records")) {
                    outputMetricIds.add(id);
                }
            }

            if (inputMetricIds.isEmpty() && outputMetricIds.isEmpty()) {
                return new NodeMetrics(nodeId, nodeName, 0L, 0L);
            }

            List<String> requestedMetricIds = new ArrayList<>(inputMetricIds);
            requestedMetricIds.addAll(outputMetricIds);
            String metricValuesUrl = UriComponentsBuilder.fromHttpUrl(metricsUrl)
                    .queryParam("get", String.join(",", requestedMetricIds))
                    .build()
                    .encode()
                    .toUriString();
            String metricValuesResponse = restTemplate.getForObject(metricValuesUrl, String.class);
            JsonNode metricValuesNode = objectMapper.readTree(metricValuesResponse);

            long inputRecords = sumMetricValues(metricValuesNode, inputMetricIds);
            long outputRecords = sumMetricValues(metricValuesNode, outputMetricIds);

            return new NodeMetrics(nodeId, nodeName, inputRecords, outputRecords);
        } catch (Exception exception) {
            return new NodeMetrics(nodeId, nodeName, 0L, 0L);
        }
    }

    private long sumMetricValues(JsonNode metricValuesNode, List<String> metricIds) {
        Set<String> targetIds = new HashSet<>(metricIds);
        long total = 0L;
        for (JsonNode metric : metricValuesNode) {
            String id = metric.path("id").asText();
            if (targetIds.contains(id)) {
                total += parseMetricValue(metric.path("value").asText("0"));
            }
        }
        return total;
    }

    private long parseMetricValue(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(rawValue);
        } catch (NumberFormatException exception) {
            return 0L;
        }
    }

    private PipelineRunStatus mapFlinkStateToRunStatus(String flinkState) {
        return switch (flinkState.toUpperCase()) {
            case "RUNNING" -> PipelineRunStatus.RUNNING;
            case "FAILED" -> PipelineRunStatus.FAILED;
            case "CANCELED", "FINISHED" -> PipelineRunStatus.STOPPED;
            default -> PipelineRunStatus.RUNNING;
        };
    }
}
