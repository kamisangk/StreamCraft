package com.streamcraft.service.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class NoHaModeContractTest {

    @Test
    void serviceDoesNotExposeHaModeCodeOrConfiguration() throws Exception {
        Path haPackage = Path.of("src/main/java/com/streamcraft/service/ha");
        try (Stream<Path> haSources = Files.exists(haPackage) ? Files.list(haPackage) : Stream.empty()) {
            assertThat(haSources).noneMatch(path -> path.toString().endsWith(".java"));
        }
        assertThat(Files.exists(Path.of("src/main/java/com/streamcraft/service/runtime/service/RuntimeTargetValidationLease.java"))).isFalse();
        assertThat(Files.exists(Path.of("src/main/java/com/streamcraft/service/runtime/service/LocalRuntimeTargetValidationLease.java"))).isFalse();
        assertThat(Files.exists(Path.of("src/main/resources/application-ha-mysql.properties"))).isFalse();

        assertThat(Files.readString(Path.of("src/main/resources/application.properties"))).doesNotContain("streamcraft.ha");
        assertThat(Files.readString(Path.of("src/main/resources/messages.properties"))).doesNotContain("ha.error");
        assertThat(Files.readString(Path.of("src/main/resources/messages_en_US.properties"))).doesNotContain("ha.error");
        assertThat(Files.readString(Path.of("src/main/resources/messages_zh_CN.properties"))).doesNotContain("ha.error");
    }
}
