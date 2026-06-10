package com.streamcraft.service.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.mock.env.MockEnvironment;

class StreamCraftDatasourceEnvironmentPostProcessorTest {

    private final StreamCraftDatasourceEnvironmentPostProcessor postProcessor =
            new StreamCraftDatasourceEnvironmentPostProcessor();

    @Test
    void defaultsToSqliteDriverAndDialect() {
        MockEnvironment environment = new MockEnvironment();

        postProcessor.postProcessEnvironment(environment, new SpringApplication());

        assertThat(environment.getProperty("spring.datasource.driver-class-name"))
                .isEqualTo("org.sqlite.JDBC");
        assertThat(environment.getProperty("spring.jpa.database-platform"))
                .isEqualTo("org.hibernate.community.dialect.SQLiteDialect");
    }

    @Test
    void mapsMysqlTypeToDriverAndDialect() {
        MockEnvironment environment = new MockEnvironment()
                .withProperty("streamcraft.datasource.type", "MySQL");

        postProcessor.postProcessEnvironment(environment, new SpringApplication());

        assertThat(environment.getProperty("spring.datasource.driver-class-name"))
                .isEqualTo("com.mysql.cj.jdbc.Driver");
        assertThat(environment.getProperty("spring.jpa.database-platform"))
                .isEqualTo("org.hibernate.dialect.MySQLDialect");
    }

    @Test
    void explicitSpringDatasourcePropertiesKeepPrecedence() {
        MockEnvironment environment = new MockEnvironment()
                .withProperty("streamcraft.datasource.type", "mysql")
                .withProperty("spring.datasource.driver-class-name", "example.CustomDriver")
                .withProperty("spring.jpa.database-platform", "example.CustomDialect");

        postProcessor.postProcessEnvironment(environment, new SpringApplication());

        assertThat(environment.getProperty("spring.datasource.driver-class-name"))
                .isEqualTo("example.CustomDriver");
        assertThat(environment.getProperty("spring.jpa.database-platform"))
                .isEqualTo("example.CustomDialect");
    }

    @Test
    void rejectsUnsupportedDatasourceType() {
        MockEnvironment environment = new MockEnvironment()
                .withProperty("streamcraft.datasource.type", "postgresql");

        assertThatThrownBy(() -> postProcessor.postProcessEnvironment(environment, new SpringApplication()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Supported values: sqlite, mysql");
    }
}
