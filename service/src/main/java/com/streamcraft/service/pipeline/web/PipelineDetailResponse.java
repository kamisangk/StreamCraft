package com.streamcraft.service.pipeline.web;

import com.streamcraft.service.pipeline.model.Pipeline;
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
        Instant updatedAt) {

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
}
