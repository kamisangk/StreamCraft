package com.streamcraft.service.pipeline.model;

import java.time.Instant;
import java.util.List;

public class PipelineMetrics {

    private String jobId;
    private PipelineRunStatus status;
    private Instant startTime;
    private Long duration;
    private List<NodeMetrics> nodeMetrics;

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
    }
}
