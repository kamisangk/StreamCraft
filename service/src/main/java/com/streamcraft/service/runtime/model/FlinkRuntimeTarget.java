package com.streamcraft.service.runtime.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "flink_runtime_target")
public class FlinkRuntimeTarget {

    @Id
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private RuntimeTargetType type;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private RuntimeTargetStatus status;

    private String jobManagerUrl;
    private String statusMessage;
    private String flinkVersion;
    private Integer taskManagerCount;
    private Integer totalSlots;
    private Integer availableSlots;
    private Instant lastValidatedAt;
    private Instant createdAt;
    private Instant updatedAt;

    @PrePersist
    void prePersist() {
        Instant now = Instant.now();
        createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public RuntimeTargetType getType() {
        return type;
    }

    public void setType(RuntimeTargetType type) {
        this.type = type;
    }

    public RuntimeTargetStatus getStatus() {
        return status;
    }

    public void setStatus(RuntimeTargetStatus status) {
        this.status = status;
    }

    public String getJobManagerUrl() {
        return jobManagerUrl;
    }

    public void setJobManagerUrl(String jobManagerUrl) {
        this.jobManagerUrl = jobManagerUrl;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    public String getFlinkVersion() {
        return flinkVersion;
    }

    public void setFlinkVersion(String flinkVersion) {
        this.flinkVersion = flinkVersion;
    }

    public Integer getTaskManagerCount() {
        return taskManagerCount;
    }

    public void setTaskManagerCount(Integer taskManagerCount) {
        this.taskManagerCount = taskManagerCount;
    }

    public Integer getTotalSlots() {
        return totalSlots;
    }

    public void setTotalSlots(Integer totalSlots) {
        this.totalSlots = totalSlots;
    }

    public Integer getAvailableSlots() {
        return availableSlots;
    }

    public void setAvailableSlots(Integer availableSlots) {
        this.availableSlots = availableSlots;
    }

    public Instant getLastValidatedAt() {
        return lastValidatedAt;
    }

    public void setLastValidatedAt(Instant lastValidatedAt) {
        this.lastValidatedAt = lastValidatedAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
