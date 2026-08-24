package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.annotation.Transactional;

class PipelineServiceTransactionContractTest {

    @Test
    void remoteCallingPipelineOperationsDoNotHoldTransactionOpen() throws Exception {
        assertNotTransactional("delete", Long.class);
    }

    private void assertNotTransactional(String methodName, Class<?>... parameterTypes) throws Exception {
        Method method = PipelineService.class.getDeclaredMethod(methodName, parameterTypes);
        assertNull(method.getAnnotation(Transactional.class), methodName + " should not be transactional");
    }
}
