package com.streamcraft.service.pipeline.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.pipeline.model.NodeMetrics;
import com.streamcraft.service.pipeline.model.PipelineMetrics;
import com.streamcraft.service.pipeline.model.PipelineRunStatus;
import com.streamcraft.service.pipeline.model.RuntimeDataAvailability;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.HttpClientErrorException;
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
        } catch (HttpClientErrorException exception) {
            throw exception;
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
                    nodeMetricsList.add(NodeMetrics.noData(
                            nodeId,
                            nodeNames.get(nodeId),
                            "NODE_VERTEX_NOT_FOUND"));
                }
            }

            metrics.setNodeMetrics(nodeMetricsList);
            int unavailableNodeCount = (int) nodeMetricsList.stream()
                    .filter(nodeMetrics -> nodeMetrics.getCollectionStatus() != RuntimeDataAvailability.AVAILABLE)
                    .count();
            metrics.setUnavailableNodeCount(unavailableNodeCount);
            if (nodeMetricsList.isEmpty()) {
                metrics.setCollectionStatus(RuntimeDataAvailability.NO_DATA);
                metrics.setUnavailableReason("NODE_METRICS_NOT_REQUESTED");
            } else if (unavailableNodeCount == 0) {
                metrics.setCollectionStatus(RuntimeDataAvailability.AVAILABLE);
                metrics.setUnavailableReason(null);
            } else if (unavailableNodeCount == nodeMetricsList.size()) {
                boolean hasQueryFailure = nodeMetricsList.stream()
                        .anyMatch(nodeMetrics -> nodeMetrics.getCollectionStatus() == RuntimeDataAvailability.UNAVAILABLE);
                boolean hasPartialData = nodeMetricsList.stream()
                        .anyMatch(nodeMetrics -> nodeMetrics.getCollectionStatus() == RuntimeDataAvailability.PARTIAL);
                if (hasQueryFailure) {
                    metrics.setCollectionStatus(RuntimeDataAvailability.UNAVAILABLE);
                    metrics.setUnavailableReason("NODE_METRICS_UNAVAILABLE");
                } else if (hasPartialData) {
                    metrics.setCollectionStatus(RuntimeDataAvailability.PARTIAL);
                    metrics.setUnavailableReason("NODE_METRICS_NOT_COMPLETE");
                } else {
                    metrics.setCollectionStatus(RuntimeDataAvailability.NO_DATA);
                    metrics.setUnavailableReason("NODE_METRICS_NOT_FOUND");
                }
            } else if (unavailableNodeCount > 0) {
                metrics.setCollectionStatus(RuntimeDataAvailability.PARTIAL);
                metrics.setUnavailableReason("NODE_METRICS_PARTIALLY_UNAVAILABLE");
            }
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
                return NodeMetrics.noData(nodeId, nodeName, "NODE_METRICS_NOT_EXPOSED");
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

            Long inputRecords = inputMetricIds.isEmpty()
                    ? null
                    : sumMetricValues(metricValuesNode, inputMetricIds);
            Long outputRecords = outputMetricIds.isEmpty()
                    ? null
                    : sumMetricValues(metricValuesNode, outputMetricIds);

            if (inputRecords == null || outputRecords == null) {
                return NodeMetrics.partial(
                        nodeId,
                        nodeName,
                        inputRecords,
                        outputRecords,
                        "NODE_METRICS_PARTIALLY_EXPOSED");
            }

            return new NodeMetrics(nodeId, nodeName, inputRecords, outputRecords);
        } catch (Exception exception) {
            return NodeMetrics.unavailable(nodeId, nodeName, "NODE_METRICS_QUERY_FAILED");
        }
    }

    private long sumMetricValues(JsonNode metricValuesNode, List<String> metricIds) {
        Set<String> targetIds = new HashSet<>(metricIds);
        Set<String> foundIds = new HashSet<>();
        long total = 0L;
        for (JsonNode metric : metricValuesNode) {
            String id = metric.path("id").asText();
            if (targetIds.contains(id)) {
                total += parseMetricValue(metric.path("value"));
                foundIds.add(id);
            }
        }
        if (foundIds.size() != targetIds.size()) {
            throw new IllegalStateException("Metric value response did not contain every requested metric.");
        }
        return total;
    }

    private long parseMetricValue(JsonNode valueNode) {
        if (valueNode == null || valueNode.isMissingNode() || valueNode.isNull()) {
            throw new IllegalStateException("Metric value is missing.");
        }
        String rawValue = valueNode.asText();
        if (rawValue == null || rawValue.isBlank()) {
            throw new IllegalStateException("Metric value is blank.");
        }
        try {
            return Long.parseLong(rawValue);
        } catch (NumberFormatException exception) {
            throw new IllegalStateException("Metric value is not a whole number.", exception);
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
