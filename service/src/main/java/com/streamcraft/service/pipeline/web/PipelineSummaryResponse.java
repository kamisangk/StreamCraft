package com.streamcraft.service.pipeline.web;

import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.service.PipelineRuntimeStatusSnapshot;
import java.time.Instant;

public record PipelineSummaryResponse(
        Long id,
        String name,
        String description,
        String lastJobId,
        String lastRunStatus,
        String lastRunMessage,
        Instant lastSubmittedAt,
        Instant updatedAt,
        String runtimeStatusAvailability,
        String runtimeStatusSource,
        String runtimeStatusUnavailableReason) {

    public PipelineSummaryResponse(
            Long id,
            String name,
            String description,
            String lastJobId,
            String lastRunStatus,
            String lastRunMessage,
            Instant lastSubmittedAt,
            Instant updatedAt) {
        this(
                id,
                name,
                description,
                lastJobId,
                lastRunStatus,
                lastRunMessage,
                lastSubmittedAt,
                updatedAt,
                "NOT_REQUESTED",
                "STORED",
                null);
    }

    public static PipelineSummaryResponse from(Pipeline entity) {
        return new PipelineSummaryResponse(
                entity.getId(),
                entity.getName(),
                entity.getDescription(),
                entity.getLastJobId(),
                entity.getLastRunStatus() == null ? null : entity.getLastRunStatus().name(),
                entity.getLastRunMessage(),
                entity.getLastSubmittedAt(),
                entity.getUpdatedAt());
    }

    public static PipelineSummaryResponse from(PipelineRuntimeStatusSnapshot snapshot) {
        Pipeline entity = snapshot.pipeline();
        return new PipelineSummaryResponse(
                entity.getId(),
                entity.getName(),
                entity.getDescription(),
                entity.getLastJobId(),
                entity.getLastRunStatus() == null ? null : entity.getLastRunStatus().name(),
                entity.getLastRunMessage(),
                entity.getLastSubmittedAt(),
                entity.getUpdatedAt(),
                snapshot.runtimeStatusAvailability().name(),
                snapshot.runtimeStatusSource(),
                snapshot.runtimeStatusUnavailableReason());
    }
}
