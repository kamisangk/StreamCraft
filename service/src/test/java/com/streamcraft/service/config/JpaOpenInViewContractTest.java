package com.streamcraft.service.config;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class JpaOpenInViewContractTest {

    @Test
    void openInViewIsDisabledToAvoidHoldingSqliteConnectionsAcrossRequests() throws Exception {
        String properties = Files.readString(Path.of("src/main/resources/application.properties"));

        assertTrue(properties.contains("spring.jpa.open-in-view=false"));
    }
}
