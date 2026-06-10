package com.streamcraft.service.config;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class SqliteDatasourceContractTest {

    @Test
    void sqliteStaysDefaultForSingleNodeMode() throws Exception {
        String properties = Files.readString(Path.of("src/main/resources/application.properties"));

        assertTrue(properties.contains(
                "streamcraft.datasource.type=${STREAMCRAFT_DATASOURCE_TYPE:sqlite}"));
        assertTrue(properties.contains(
                "spring.datasource.url=${STREAMCRAFT_DATASOURCE_URL:jdbc:sqlite:streamcraft-service.db}"));
        assertTrue(!properties.contains("spring.datasource.driver-class-name=org.sqlite.JDBC"));
        assertTrue(!properties.contains("spring.jpa.database-platform=org.hibernate.community.dialect.SQLiteDialect"));
        assertTrue(properties.contains(
                "spring.datasource.hikari.maximum-pool-size=${STREAMCRAFT_DATASOURCE_POOL_SIZE:1}"));
        assertTrue(properties.contains(
                "spring.datasource.hikari.data-source-properties.busy_timeout=${STREAMCRAFT_SQLITE_BUSY_TIMEOUT:10000}"));
        assertTrue(!properties.contains("streamcraft.ha"));
        assertTrue(!Files.exists(Path.of("src/main/resources/application-mysql.properties")));
        assertTrue(properties.contains(
                "logging.file.name=${STREAMCRAFT_LOG_FILE:${STREAMCRAFT_LOG_DIR:./logs}/streamcraft-service.log}"));
        assertTrue(properties.contains("logging.level.root=${STREAMCRAFT_LOG_LEVEL:INFO}"));
        assertTrue(properties.contains(
                "streamcraft.runtime-target.validation-interval=${STREAMCRAFT_RUNTIME_TARGET_VALIDATION_INTERVAL:5000}"));
    }
}
