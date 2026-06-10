package com.streamcraft.service.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class FlinkGatewayPropertyContractTest {

    @Test
    void serviceDoesNotDependOnRemovedControllerGatewayProperties() throws Exception {
        String properties = Files.readString(Path.of("src/main/resources/application.properties"));

        assertFalse(properties.contains("streamcraft.controller.base-url=http://localhost:8082"));
        assertFalse(properties.contains("streamcraft.controller.connect-timeout=3s"));
        assertFalse(properties.contains("streamcraft.controller.read-timeout=5s"));
        assertFalse(properties.contains("streamcraft.controller.submit-read-timeout=90s"));
        assertTrue(properties.contains(
                "streamcraft.runtime-target.validation-interval=${STREAMCRAFT_RUNTIME_TARGET_VALIDATION_INTERVAL:5000}"));
    }

    @Test
    void serviceDoesNotExposeFlinkCliOrYarnConfigurationEntryPoints() throws Exception {
        String properties = Files.readString(Path.of("src/main/resources/application.properties"));

        assertFalse(properties.contains("streamcraft.flink.flink-home"));
        assertFalse(properties.contains("streamcraft.flink.flink-conf-dir"));
        assertFalse(properties.contains("streamcraft.flink.hadoop-conf-dir"));
        assertFalse(properties.contains("streamcraft.flink.yarn-submit-timeout"));
        assertFalse(properties.contains("streamcraft.flink.yarn-queue"));
        assertFalse(properties.contains("streamcraft.flink.kerberos-principal"));
        assertFalse(properties.contains("streamcraft.flink.kerberos-keytab"));
        assertFalse(properties.contains("streamcraft.flink.krb5-conf"));
        assertFalse(properties.contains("streamcraft.pipeline.runtime.yarn"));
    }
}
