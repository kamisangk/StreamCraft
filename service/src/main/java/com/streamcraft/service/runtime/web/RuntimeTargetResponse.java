package com.streamcraft.service.runtime.web;

import com.streamcraft.service.runtime.model.FlinkRuntimeTarget;
import java.time.Instant;

public record RuntimeTargetResponse(
        boolean configured,
        String type,
        String status,
        String statusMessage,
        String jobManagerUrl,
        String flinkVersion,
        Integer taskManagerCount,
        Integer totalSlots,
        Integer availableSlots,
        Instant lastValidatedAt) {

    public static RuntimeTargetResponse unconfigured() {
        return new RuntimeTargetResponse(
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    public static RuntimeTargetResponse from(FlinkRuntimeTarget entity) {
        return new RuntimeTargetResponse(
                true,
                entity.getType().name(),
                entity.getStatus().name(),
                entity.getStatusMessage(),
                entity.getJobManagerUrl(),
                entity.getFlinkVersion(),
                entity.getTaskManagerCount(),
                entity.getTotalSlots(),
                entity.getAvailableSlots(),
                entity.getLastValidatedAt());
    }
}
