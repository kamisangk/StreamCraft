package com.streamcraft.service.runtime.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.scheduling.annotation.Scheduled;

@ExtendWith(MockitoExtension.class)
class RuntimeTargetValidationSchedulerTest {

    @Mock
    private FlinkRuntimeTargetService runtimeTargetService;

    @Test
    void schedulerRevalidatesConfiguredRuntimeTarget() {
        RuntimeTargetValidationScheduler scheduler = new RuntimeTargetValidationScheduler(runtimeTargetService);
        com.streamcraft.service.runtime.model.FlinkRuntimeTarget target =
                new com.streamcraft.service.runtime.model.FlinkRuntimeTarget();
        org.mockito.Mockito.when(runtimeTargetService.findTarget()).thenReturn(java.util.Optional.of(target));

        scheduler.revalidateTarget();

        verify(runtimeTargetService).revalidate();
    }

    @Test
    void revalidateTargetRunsEveryFiveSeconds() throws Exception {
        Method method = RuntimeTargetValidationScheduler.class.getDeclaredMethod("revalidateTarget");

        Scheduled scheduled = method.getAnnotation(Scheduled.class);

        assertThat(scheduled).isNotNull();
        assertThat(scheduled.fixedDelayString()).isEqualTo("${streamcraft.runtime-target.validation-interval:5000}");
    }
}
