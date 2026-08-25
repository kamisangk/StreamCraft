package com.streamcraft.service.pipeline.web;

import com.streamcraft.service.pipeline.model.Pipeline;
import com.streamcraft.service.pipeline.service.PipelineRuntimeStatusSnapshot;
import java.time.Instant;

public record PipelineDetailResponse(
        Long id,
        String name,
        String description,
        String definitionJson,
        String lastJobId,
        String lastRunStatus,
        String lastRunMessage,
        Instant lastSubmittedAt,
        Instant createdAt,
        Instant updatedAt,
        String runtimeStatusAvailability,
        String runtimeStatusSource,
        String runtimeStatusUnavailableReason) {

    public PipelineDetailResponse(
            Long id,
            String name,
            String description,
            String definitionJson,
            String lastJobId,
            String lastRunStatus,
            String lastRunMessage,
            Instant lastSubmittedAt,
            Instant createdAt,
            Instant updatedAt) {
        this(
                id,
                name,
                description,
                definitionJson,
                lastJobId,
                lastRunStatus,
                lastRunMessage,
                lastSubmittedAt,
                createdAt,
                updatedAt,
                "NOT_REQUESTED",
                "STORED",
                null);
    }

    public static PipelineDetailResponse from(Pipeline entity) {
        return new PipelineDetailResponse(
                entity.getId(),
                entity.getName(),
                entity.getDescription(),
                entity.getDefinitionJson(),
                entity.getLastJobId(),
                entity.getLastRunStatus() == null ? null : entity.getLastRunStatus().name(),
                entity.getLastRunMessage(),
                entity.getLastSubmittedAt(),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }

    public static PipelineDetailResponse from(PipelineRuntimeStatusSnapshot snapshot) {
        Pipeline entity = snapshot.pipeline();
        return new PipelineDetailResponse(
                entity.getId(),
                entity.getName(),
                entity.getDescription(),
                entity.getDefinitionJson(),
                entity.getLastJobId(),
                entity.getLastRunStatus() == null ? null : entity.getLastRunStatus().name(),
                entity.getLastRunMessage(),
                entity.getLastSubmittedAt(),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                snapshot.runtimeStatusAvailability().name(),
                snapshot.runtimeStatusSource(),
                snapshot.runtimeStatusUnavailableReason());
    }
}
