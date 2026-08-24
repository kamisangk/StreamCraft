package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.annotation.Transactional;

class PipelineCrudServiceTransactionContractTest {

    @Test
    void saveKeepsWriteTransactionAndDeleteUsesNoRemoteTransaction() throws Exception {
        assertTransactional("save", com.streamcraft.service.pipeline.web.SavePipelineRequest.class);
        assertNotTransactional("delete", Long.class);
    }

    private void assertTransactional(String methodName, Class<?>... parameterTypes) throws Exception {
        Method method = PipelineCrudService.class.getDeclaredMethod(methodName, parameterTypes);
        org.junit.jupiter.api.Assertions.assertNotNull(
                method.getAnnotation(Transactional.class),
                methodName + " should be transactional");
    }

    private void assertNotTransactional(String methodName, Class<?>... parameterTypes) throws Exception {
        Method method = PipelineCrudService.class.getDeclaredMethod(methodName, parameterTypes);
        assertNull(method.getAnnotation(Transactional.class), methodName + " should not be transactional");
    }
}
