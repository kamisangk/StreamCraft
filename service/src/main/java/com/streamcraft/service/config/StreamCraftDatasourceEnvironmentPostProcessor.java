package com.streamcraft.service.config;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

public final class StreamCraftDatasourceEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    private static final String PROPERTY_SOURCE_NAME = "streamcraftDatasourceDefaults";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        DatabaseType databaseType = DatabaseType.from(
                environment.getProperty("streamcraft.datasource.type", "sqlite"));

        Map<String, Object> defaults = new LinkedHashMap<>();
        defaults.put("spring.datasource.driver-class-name", databaseType.driverClassName);
        defaults.put("spring.jpa.database-platform", databaseType.hibernateDialect);

        environment.getPropertySources().addLast(new MapPropertySource(PROPERTY_SOURCE_NAME, defaults));
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    private enum DatabaseType {
        SQLITE("org.sqlite.JDBC", "org.hibernate.community.dialect.SQLiteDialect"),
        MYSQL("com.mysql.cj.jdbc.Driver", "org.hibernate.dialect.MySQLDialect");

        private final String driverClassName;
        private final String hibernateDialect;

        DatabaseType(String driverClassName, String hibernateDialect) {
            this.driverClassName = driverClassName;
            this.hibernateDialect = hibernateDialect;
        }

        private static DatabaseType from(String rawType) {
            String normalized = rawType == null ? "sqlite" : rawType.trim().toUpperCase(Locale.ROOT);
            return switch (normalized) {
                case "SQLITE" -> SQLITE;
                case "MYSQL" -> MYSQL;
                default -> throw new IllegalArgumentException(
                        "Unsupported streamcraft.datasource.type '" + rawType + "'. Supported values: sqlite, mysql.");
            };
        }
    }
}
