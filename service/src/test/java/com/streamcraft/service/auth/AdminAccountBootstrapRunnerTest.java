package com.streamcraft.service.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamcraft.service.auth.service.AdminPasswordGenerator;
import com.streamcraft.service.auth.service.AdminAccountBootstrapRunner;
import com.streamcraft.service.config.StreamCraftAuthProperties;
import com.streamcraft.service.config.UserDetailsConfig;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.JdbcUserDetailsManager;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(classes = {
        UserDetailsConfig.class,
        AdminAccountBootstrapRunner.class,
        AdminAccountBootstrapRunnerTest.TestConfig.class
})
@EnableConfigurationProperties(StreamCraftAuthProperties.class)
@TestPropertySource(properties = {
        "streamcraft.auth.remember-me-key=test-remember-me-key"
})
@ExtendWith(OutputCaptureExtension.class)
class AdminAccountBootstrapRunnerTest {

    @Autowired
    private UserDetailsService userDetailsService;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TrackingAdminPasswordGenerator adminPasswordGenerator;

    @Autowired
    private AdminAccountBootstrapRunner bootstrapRunner;

    @Test
    void createsInitialAdminAccountInJdbcStore(CapturedOutput output) {
        UserDetails user = userDetailsService.loadUserByUsername("admin");

        assertThat(user.getAuthorities())
                .extracting("authority")
                .containsExactly("ROLE_ADMIN");
        assertThat(passwordEncoder.matches("SeedPass-123", user.getPassword())).isTrue();
        assertThat(output).contains("Generated initial admin password");
        assertThat(jdbcTemplate.queryForObject("select count(*) from users", Integer.class)).isEqualTo(1);
    }

    @Test
    void preservesExistingAdminOnLaterBootstrap(CapturedOutput output) {
        UserDetails existingUser = userDetailsService.loadUserByUsername("admin");

        bootstrapRunner.run(null);

        UserDetails reloadedUser = userDetailsService.loadUserByUsername("admin");
        assertThat(reloadedUser.getPassword()).isEqualTo(existingUser.getPassword());
        assertThat(adminPasswordGenerator.invocations()).isEqualTo(1);
        assertThat(output).containsOnlyOnce("Generated initial admin password");
        assertThat(jdbcTemplate.queryForObject("select count(*) from users", Integer.class)).isEqualTo(1);
    }

    @Test
    void repairsMissingAdminRoleWithoutChangingExistingPassword() {
        UserDetails existingUser = userDetailsService.loadUserByUsername("admin");
        jdbcTemplate.update("delete from authorities where username = ? and authority = ?", "admin", "ROLE_ADMIN");

        bootstrapRunner.run(null);

        UserDetails repairedUser = userDetailsService.loadUserByUsername("admin");
        assertThat(repairedUser.getPassword()).isEqualTo(existingUser.getPassword());
        assertThat(repairedUser.getAuthorities())
                .extracting("authority")
                .containsExactly("ROLE_ADMIN");
        assertThat(adminPasswordGenerator.invocations()).isEqualTo(1);
    }

    @Test
    void schemaCreationWorksWithSqlite(@TempDir Path tempDir) {
        DataSource sqliteDataSource = sqliteDataSource(tempDir.resolve("admin-bootstrap.db"));
        JdbcTemplate sqliteJdbcTemplate = new JdbcTemplate(sqliteDataSource);
        PasswordEncoder sqlitePasswordEncoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
        AdminAccountBootstrapRunner sqliteRunner = new AdminAccountBootstrapRunner(
                sqliteJdbcTemplate,
                new JdbcUserDetailsManager(sqliteDataSource),
                sqlitePasswordEncoder,
                new StreamCraftAuthProperties("test-remember-me-key", 1209600),
                () -> "SqlitePass-123");

        sqliteRunner.run(null);

        assertThat(sqliteJdbcTemplate.queryForObject("select count(*) from users", Integer.class)).isEqualTo(1);
        assertThat(sqliteJdbcTemplate.queryForObject(
                "select count(*) from authorities where username = ? and authority = ?",
                Integer.class,
                "admin",
                "ROLE_ADMIN")).isEqualTo(1);
    }

    @Test
    void repairsAdminRoleWhenConcurrentCreateInsertsUserFirst(CapturedOutput output) {
        DataSource dataSource = h2DataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        PasswordEncoder passwordEncoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
        AdminAccountBootstrapRunner runner = new AdminAccountBootstrapRunner(
                jdbcTemplate,
                new RacingJdbcUserDetailsManager(dataSource, jdbcTemplate, passwordEncoder),
                passwordEncoder,
                new StreamCraftAuthProperties("test-remember-me-key", 1209600),
                () -> "RacePass-123");

        runner.run(null);

        assertThat(jdbcTemplate.queryForObject("select count(*) from users where username = ?", Integer.class, "admin"))
                .isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "select count(*) from authorities where username = ? and authority = ?",
                Integer.class,
                "admin",
                "ROLE_ADMIN")).isEqualTo(1);
        assertThat(output).doesNotContain("RacePass-123");
    }

    @Test
    void logsGeneratedPasswordWhenCreateUserStoredItBeforeAuthorityRace(CapturedOutput output) {
        DataSource dataSource = h2DataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        PasswordEncoder passwordEncoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
        AdminAccountBootstrapRunner runner = new AdminAccountBootstrapRunner(
                jdbcTemplate,
                new AuthorityRacingJdbcUserDetailsManager(dataSource, jdbcTemplate),
                passwordEncoder,
                new StreamCraftAuthProperties("test-remember-me-key", 1209600),
                () -> "RacePass-123");

        runner.run(null);

        UserDetails user = new JdbcUserDetailsManager(dataSource).loadUserByUsername("admin");
        assertThat(passwordEncoder.matches("RacePass-123", user.getPassword())).isTrue();
        assertThat(user.getAuthorities())
                .extracting("authority")
                .containsExactly("ROLE_ADMIN");
        assertThat(output)
                .contains("Generated initial admin password")
                .contains("RacePass-123");
    }

    private static DataSource h2DataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setUrl("jdbc:h2:mem:admin-race-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        dataSource.setUsername("sa");
        dataSource.setPassword("");
        return dataSource;
    }

    private static DataSource sqliteDataSource(Path databasePath) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite:" + databasePath);
        return dataSource;
    }

    @Configuration(proxyBeanMethods = false)
    static class TestConfig {

        @Bean
        DataSource dataSource() {
            DriverManagerDataSource dataSource = new DriverManagerDataSource();
            dataSource.setDriverClassName("org.h2.Driver");
            dataSource.setUrl("jdbc:h2:mem:admin-bootstrap-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
            dataSource.setUsername("sa");
            dataSource.setPassword("");
            return dataSource;
        }

        @Bean
        JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }

        @Bean
        TrackingAdminPasswordGenerator adminPasswordGenerator() {
            return new TrackingAdminPasswordGenerator();
        }
    }

    static class TrackingAdminPasswordGenerator implements AdminPasswordGenerator {

        private final AtomicInteger invocations = new AtomicInteger();

        @Override
        public String generate() {
            invocations.incrementAndGet();
            return "SeedPass-123";
        }

        int invocations() {
            return invocations.get();
        }
    }

    static class RacingJdbcUserDetailsManager extends JdbcUserDetailsManager {

        private final JdbcTemplate jdbcTemplate;
        private final PasswordEncoder passwordEncoder;

        RacingJdbcUserDetailsManager(DataSource dataSource,
                                     JdbcTemplate jdbcTemplate,
                                     PasswordEncoder passwordEncoder) {
            super(dataSource);
            this.jdbcTemplate = jdbcTemplate;
            this.passwordEncoder = passwordEncoder;
        }

        @Override
        public void createUser(UserDetails user) {
            jdbcTemplate.update(
                    "insert into users (username, password, enabled) values (?, ?, ?)",
                    user.getUsername(),
                    passwordEncoder.encode("ConcurrentPass-123"),
                    true);
            throw new DuplicateKeyException("Concurrent admin insert won the bootstrap race");
        }
    }

    static class AuthorityRacingJdbcUserDetailsManager extends JdbcUserDetailsManager {

        private final JdbcTemplate jdbcTemplate;

        AuthorityRacingJdbcUserDetailsManager(DataSource dataSource, JdbcTemplate jdbcTemplate) {
            super(dataSource);
            this.jdbcTemplate = jdbcTemplate;
        }

        @Override
        public void createUser(UserDetails user) {
            jdbcTemplate.update(
                    "insert into users (username, password, enabled) values (?, ?, ?)",
                    user.getUsername(),
                    user.getPassword(),
                    true);
            jdbcTemplate.update(
                    "insert into authorities (username, authority) values (?, ?)",
                    user.getUsername(),
                    "ROLE_ADMIN");
            throw new DuplicateKeyException("Concurrent authority insert won the bootstrap race");
        }
    }
}
