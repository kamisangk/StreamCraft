package com.streamcraft.service.pipeline.model;

public class NodeMetrics {

    private String nodeId;
    private String nodeName;
    private Long inputRecords;
    private Long outputRecords;
    private Double inputRate;
    private Double outputRate;

    public NodeMetrics() {
    }

    public NodeMetrics(String nodeId, String nodeName, Long inputRecords, Long outputRecords) {
        this.nodeId = nodeId;
        this.nodeName = nodeName;
        this.inputRecords = inputRecords;
        this.outputRecords = outputRecords;
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
}
