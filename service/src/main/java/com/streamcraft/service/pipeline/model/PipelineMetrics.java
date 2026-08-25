package com.streamcraft.service.pipeline.model;

import java.time.Instant;
import java.util.List;

public class PipelineMetrics {

    private String jobId;
    private PipelineRunStatus status;
    private Instant startTime;
    private Long duration;
    private List<NodeMetrics> nodeMetrics;
    private RuntimeDataAvailability collectionStatus = RuntimeDataAvailability.NOT_REQUESTED;
    private String unavailableReason;
    private int unavailableNodeCount;

    public PipelineMetrics() {
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public PipelineRunStatus getStatus() {
        return status;
    }

    public void setStatus(PipelineRunStatus status) {
        this.status = status;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public List<NodeMetrics> getNodeMetrics() {
        return nodeMetrics;
    }

    public void setNodeMetrics(List<NodeMetrics> nodeMetrics) {
        this.nodeMetrics = nodeMetrics;
        if (collectionStatus == RuntimeDataAvailability.NOT_REQUESTED || collectionStatus == null) {
            collectionStatus = nodeMetrics == null || nodeMetrics.isEmpty()
                    ? RuntimeDataAvailability.NO_DATA
                    : RuntimeDataAvailability.AVAILABLE;
        }
    }

    public static PipelineMetrics unavailable(String jobId, String reason) {
        PipelineMetrics metrics = new PipelineMetrics();
        metrics.jobId = jobId;
        metrics.collectionStatus = RuntimeDataAvailability.UNAVAILABLE;
        metrics.unavailableReason = reason;
        return metrics;
    }

    public static PipelineMetrics noData(String jobId, String reason) {
        PipelineMetrics metrics = new PipelineMetrics();
        metrics.jobId = jobId;
        metrics.collectionStatus = RuntimeDataAvailability.NO_DATA;
        metrics.unavailableReason = reason;
        return metrics;
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

    public int getUnavailableNodeCount() {
        return unavailableNodeCount;
    }

    public void setUnavailableNodeCount(int unavailableNodeCount) {
        this.unavailableNodeCount = unavailableNodeCount;
    }
}
