package com.streamcraft.service.pipeline.model;

public class NodeMetrics {

    private String nodeId;
    private String nodeName;
    private Long inputRecords;
    private Long outputRecords;
    private Double inputRate;
    private Double outputRate;
    private RuntimeDataAvailability collectionStatus = RuntimeDataAvailability.NOT_REQUESTED;
    private String unavailableReason;

    public NodeMetrics() {
    }

    public NodeMetrics(String nodeId, String nodeName, Long inputRecords, Long outputRecords) {
        this.nodeId = nodeId;
        this.nodeName = nodeName;
        this.inputRecords = inputRecords;
        this.outputRecords = outputRecords;
        this.collectionStatus = inputRecords == null && outputRecords == null
                ? RuntimeDataAvailability.NO_DATA
                : RuntimeDataAvailability.AVAILABLE;
    }

    public static NodeMetrics unavailable(String nodeId, String nodeName, String reason) {
        NodeMetrics metrics = new NodeMetrics(nodeId, nodeName, null, null);
        metrics.collectionStatus = RuntimeDataAvailability.UNAVAILABLE;
        metrics.unavailableReason = reason;
        return metrics;
    }

    public static NodeMetrics noData(String nodeId, String nodeName, String reason) {
        NodeMetrics metrics = new NodeMetrics(nodeId, nodeName, null, null);
        metrics.collectionStatus = RuntimeDataAvailability.NO_DATA;
        metrics.unavailableReason = reason;
        return metrics;
    }

    public static NodeMetrics partial(
            String nodeId,
            String nodeName,
            Long inputRecords,
            Long outputRecords,
            String reason) {
        NodeMetrics metrics = new NodeMetrics(nodeId, nodeName, inputRecords, outputRecords);
        metrics.collectionStatus = RuntimeDataAvailability.PARTIAL;
        metrics.unavailableReason = reason;
        return metrics;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public Long getInputRecords() {
        return inputRecords;
    }

    public void setInputRecords(Long inputRecords) {
        this.inputRecords = inputRecords;
    }

    public Long getOutputRecords() {
        return outputRecords;
    }

    public void setOutputRecords(Long outputRecords) {
        this.outputRecords = outputRecords;
    }

    public Double getInputRate() {
        return inputRate;
    }

    public void setInputRate(Double inputRate) {
        this.inputRate = inputRate;
    }

    public Double getOutputRate() {
        return outputRate;
    }

    public void setOutputRate(Double outputRate) {
        this.outputRate = outputRate;
    }

    public RuntimeDataAvailability getCollectionStatus() {
        return collectionStatus;
    }

    public void setCollectionStatus(RuntimeDataAvailability collectionStatus) {
        this.collectionStatus = collectionStatus;
    }

    public String getUnavailableReason() {
        return unavailableReason;
    }

    public void setUnavailableReason(String unavailableReason) {
        this.unavailableReason = unavailableReason;
    }
}
