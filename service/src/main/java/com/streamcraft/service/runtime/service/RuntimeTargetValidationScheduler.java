package com.streamcraft.service.runtime.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class RuntimeTargetValidationScheduler {

    private final FlinkRuntimeTargetService runtimeTargetService;

    public RuntimeTargetValidationScheduler(FlinkRuntimeTargetService runtimeTargetService) {
        this.runtimeTargetService = runtimeTargetService;
    }

    @Scheduled(fixedDelayString = "${streamcraft.runtime-target.validation-interval:5000}")
    public void revalidateTarget() {
        runtimeTargetService.findTarget().ifPresent(target -> runtimeTargetService.revalidate());
    }
}
