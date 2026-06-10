package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamcraft.service.runtime.web.RuntimeTargetApiController;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.springframework.web.bind.annotation.PutMapping;

class RuntimeTargetApiControllerContractTest {

    @Test
    void runtimeTargetApiExposesStandaloneSaveOnly() {
        assertThat(Arrays.stream(RuntimeTargetApiController.class.getDeclaredMethods())
                .filter(method -> method.getAnnotation(PutMapping.class) != null)
                .map(method -> method.getAnnotation(PutMapping.class))
                .flatMap(mapping -> Arrays.stream(mapping.value()))
                .toList())
                .containsExactly("/standalone")
                .doesNotContain("/yarn-application");
    }

    @Test
    void runtimeTargetApiDoesNotExposeYarnSaveOperation() {
        assertThat(Arrays.stream(RuntimeTargetApiController.class.getDeclaredMethods())
                .map(java.lang.reflect.Method::getName)
                .toList())
                .doesNotContain("saveYarnApplication");
    }
}
