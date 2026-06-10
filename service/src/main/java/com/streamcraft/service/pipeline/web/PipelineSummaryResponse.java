package com.streamcraft.service.pipeline.web;

import com.streamcraft.service.pipeline.model.Pipeline;
import java.time.Instant;

public record PipelineSummaryResponse(
        Long id,
        String name,
        String description,
        String lastJobId,
        String lastRunStatus,
        String lastRunMessage,
        Instant lastSubmittedAt,
        Instant updatedAt) {

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
}
