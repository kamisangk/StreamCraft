package com.streamcraft.service.pipeline.service;

import static org.junit.jupiter.api.Assertions.assertNull;

import com.streamcraft.service.pipeline.web.PipelinePreviewRequest;
import com.streamcraft.service.pipeline.web.RunPipelineRequest;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.annotation.Transactional;

class PipelineServiceTransactionContractTest {

    @Test
    void remoteCallingPipelineOperationsDoNotHoldTransactionOpen() throws Exception {
        assertNotTransactional("preview", PipelinePreviewRequest.class);
        assertNotTransactional("run", Long.class, RunPipelineRequest.class);
        assertNotTransactional("list");
        assertNotTransactional("listRunningPipelines");
        assertNotTransactional("listRuntimeSnapshots");
        assertNotTransactional("get", Long.class);
        assertNotTransactional("stop", Long.class);
        assertNotTransactional("delete", Long.class);
        assertNotTransactional("getMetrics", Long.class);
    }

    private void assertNotTransactional(String methodName, Class<?>... parameterTypes) throws Exception {
        Method method = PipelineService.class.getDeclaredMethod(methodName, parameterTypes);
        assertNull(method.getAnnotation(Transactional.class), methodName + " should not be transactional");
    }
}
