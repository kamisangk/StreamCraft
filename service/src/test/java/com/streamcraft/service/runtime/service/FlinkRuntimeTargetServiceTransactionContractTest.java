package com.streamcraft.service.runtime.service;

import static org.junit.jupiter.api.Assertions.assertNull;

import com.streamcraft.service.runtime.web.SaveStandaloneRuntimeTargetRequest;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.annotation.Transactional;

class FlinkRuntimeTargetServiceTransactionContractTest {

    @Test
    void remoteCallingRuntimeTargetOperationsDoNotHoldTransactionOpen() throws Exception {
        assertNotTransactional("findTarget");
        assertNotTransactional("requireTarget");
        assertNotTransactional("saveStandalone", SaveStandaloneRuntimeTargetRequest.class);
        assertNotTransactional("revalidate");
    }

    private void assertNotTransactional(String methodName, Class<?>... parameterTypes) throws Exception {
        Method method = FlinkRuntimeTargetService.class.getDeclaredMethod(methodName, parameterTypes);
        assertNull(method.getAnnotation(Transactional.class), methodName + " should not be transactional");
    }
}
