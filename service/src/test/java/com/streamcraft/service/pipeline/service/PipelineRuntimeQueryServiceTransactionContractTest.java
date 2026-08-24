package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.annotation.Transactional;

class PipelineRuntimeQueryServiceTransactionContractTest {

    @Test
    void runtimeQueryOperationsDoNotHoldTransactionOpen() throws Exception {
        assertNotTransactional("list");
        assertNotTransactional("listRunningPipelines");
        assertNotTransactional("listRuntimeSnapshots");
        assertNotTransactional("get", Long.class);
        assertNotTransactional("getMetrics", Long.class);
    }

    private void assertNotTransactional(String methodName, Class<?>... parameterTypes) throws Exception {
        Method method = PipelineRuntimeQueryService.class.getDeclaredMethod(methodName, parameterTypes);
        assertNull(method.getAnnotation(Transactional.class), methodName + " should not be transactional");
    }
}
