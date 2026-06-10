package com.streamcraft.service.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.streamcraft.service.auth.service.AdminPasswordService;
import com.streamcraft.service.auth.web.UpdateAdminPasswordRequest;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.JdbcUserDetailsManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;

class AdminPasswordServiceTest {

    private PasswordEncoder passwordEncoder;
    private JdbcUserDetailsManager userDetailsManager;
    private AdminPasswordService adminPasswordService;

    @BeforeEach
    void setUp() {
        DataSource dataSource = h2DataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("""
                create table users (
                    username varchar(50) not null primary key,
                    password varchar(500) not null,
                    enabled boolean not null
                )
                """);
        jdbcTemplate.execute("""
                create table authorities (
                    username varchar(50) not null,
                    authority varchar(50) not null,
                    constraint fk_authorities_users foreign key (username) references users(username),
                    constraint uk_authorities unique (username, authority)
                )
                """);

        passwordEncoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
        userDetailsManager = new JdbcUserDetailsManager(dataSource);
        adminPasswordService = new AdminPasswordService(userDetailsManager, passwordEncoder);

        userDetailsManager.createUser(User.withUsername("admin")
                .password(passwordEncoder.encode("CurrentPass-123"))
                .roles("ADMIN")
                .build());
        UserDetails authenticatedUser = userDetailsManager.loadUserByUsername("admin");
        SecurityContextHolder.getContext().setAuthentication(new UsernamePasswordAuthenticationToken(
                authenticatedUser,
                "CurrentPass-123",
                authenticatedUser.getAuthorities()));
    }

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void successfulChangeUpdatesStoredJdbcPasswordHash() {
        adminPasswordService.updatePassword(new UpdateAdminPasswordRequest(
                "CurrentPass-123",
                "NewPass-456",
                "NewPass-456"));

        UserDetails updatedUser = userDetailsManager.loadUserByUsername("admin");
        assertThat(passwordEncoder.matches("CurrentPass-123", updatedUser.getPassword())).isFalse();
        assertThat(passwordEncoder.matches("NewPass-456", updatedUser.getPassword())).isTrue();
    }

    @Test
    void rejectsWrongCurrentPassword() {
        assertThatThrownBy(() -> adminPasswordService.updatePassword(new UpdateAdminPasswordRequest(
                "WrongPass-123",
                "NewPass-456",
                "NewPass-456")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Current password is incorrect.");

        UserDetails unchangedUser = userDetailsManager.loadUserByUsername("admin");
        assertThat(passwordEncoder.matches("CurrentPass-123", unchangedUser.getPassword())).isTrue();
    }

    @Test
    void rejectsMismatchedConfirmation() {
        assertThatThrownBy(() -> adminPasswordService.updatePassword(new UpdateAdminPasswordRequest(
                "CurrentPass-123",
                "NewPass-456",
                "DifferentPass-456")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("New password confirmation does not match.");

        UserDetails unchangedUser = userDetailsManager.loadUserByUsername("admin");
        assertThat(passwordEncoder.matches("CurrentPass-123", unchangedUser.getPassword())).isTrue();
    }

    @Test
    void rollsBackPasswordAndAuthorityChangesWhenAuthorityRewriteFails() {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(TransactionalRollbackTestConfig.class)) {
            JdbcTemplate jdbcTemplate = context.getBean(JdbcTemplate.class);
            PasswordEncoder passwordEncoder = context.getBean(PasswordEncoder.class);
            JdbcUserDetailsManager userDetailsManager = context.getBean(JdbcUserDetailsManager.class);
            AdminPasswordService adminPasswordService = context.getBean(AdminPasswordService.class);

            new TransactionTemplate(context.getBean(org.springframework.transaction.PlatformTransactionManager.class))
                    .executeWithoutResult(status -> userDetailsManager.createUser(User.withUsername("admin")
                            .password(passwordEncoder.encode("CurrentPass-123"))
                            .roles("ADMIN")
                            .build()));
            UserDetails authenticatedUser = userDetailsManager.loadUserByUsername("admin");
            SecurityContextHolder.getContext().setAuthentication(new UsernamePasswordAuthenticationToken(
                    authenticatedUser,
                    "CurrentPass-123",
                    authenticatedUser.getAuthorities()));

            assertThatThrownBy(() -> adminPasswordService.updatePassword(new UpdateAdminPasswordRequest(
                    "CurrentPass-123",
                    "NewPass-456",
                    "NewPass-456")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Simulated authority rewrite failure");

            String storedPassword = jdbcTemplate.queryForObject(
                    "select password from users where username = ?",
                    String.class,
                    "admin");
            assertThat(passwordEncoder.matches("CurrentPass-123", storedPassword)).isTrue();
            assertThat(passwordEncoder.matches("NewPass-456", storedPassword)).isFalse();
            assertThat(jdbcTemplate.queryForObject(
                    "select count(*) from authorities where username = ? and authority = ?",
                    Integer.class,
                    "admin",
                    "ROLE_ADMIN")).isEqualTo(1);
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    private static DataSource h2DataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setUrl("jdbc:h2:mem:admin-password-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        dataSource.setUsername("sa");
        dataSource.setPassword("");
        return dataSource;
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement
    static class TransactionalRollbackTestConfig {

        @Bean
        DataSource dataSource() {
            return h2DataSource();
        }

        @Bean
        JdbcTemplate jdbcTemplate(DataSource dataSource) {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            jdbcTemplate.execute("""
                    create table users (
                        username varchar(50) not null primary key,
                        password varchar(500) not null,
                        enabled boolean not null
                    )
                    """);
            jdbcTemplate.execute("""
                    create table authorities (
                        username varchar(50) not null,
                        authority varchar(50) not null,
                        constraint fk_authorities_users foreign key (username) references users(username),
                        constraint uk_authorities unique (username, authority)
                    )
                    """);
            return jdbcTemplate;
        }

        @Bean
        org.springframework.transaction.PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new org.springframework.jdbc.datasource.DataSourceTransactionManager(dataSource);
        }

        @Bean
        PasswordEncoder passwordEncoder() {
            return PasswordEncoderFactories.createDelegatingPasswordEncoder();
        }

        @Bean
        JdbcUserDetailsManager userDetailsManager(DataSource dataSource, JdbcTemplate jdbcTemplate) {
            return new FailingAuthorityRewriteUserDetailsManager(dataSource, jdbcTemplate);
        }

        @Bean
        AdminPasswordService adminPasswordService(JdbcUserDetailsManager userDetailsManager,
                                                 PasswordEncoder passwordEncoder) {
            return new AdminPasswordService(userDetailsManager, passwordEncoder);
        }
    }

    static class FailingAuthorityRewriteUserDetailsManager extends JdbcUserDetailsManager {

        private final JdbcTemplate jdbcTemplate;

        FailingAuthorityRewriteUserDetailsManager(DataSource dataSource, JdbcTemplate jdbcTemplate) {
            super(dataSource);
            this.jdbcTemplate = jdbcTemplate;
        }

        @Override
        public void updateUser(UserDetails user) {
            jdbcTemplate.update(
                    "update users set password = ?, enabled = ? where username = ?",
                    user.getPassword(),
                    user.isEnabled(),
                    user.getUsername());
            jdbcTemplate.update("delete from authorities where username = ?", user.getUsername());
            throw new IllegalStateException("Simulated authority rewrite failure");
        }
    }
}
