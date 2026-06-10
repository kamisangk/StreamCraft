package com.streamcraft.service.runtime.client;

public record StandaloneValidationResponse(
        boolean reachable,
        String status,
        String statusMessage,
        String flinkVersion,
        Integer taskManagerCount,
        Integer totalSlots,
        Integer availableSlots) {
}
