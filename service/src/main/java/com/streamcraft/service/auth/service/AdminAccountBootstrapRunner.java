package com.streamcraft.service.auth.service;

import com.streamcraft.service.config.StreamCraftAuthProperties;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.JdbcUserDetailsManager;
import org.springframework.stereotype.Component;

@Component
public class AdminAccountBootstrapRunner implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdminAccountBootstrapRunner.class);

    private static final String CREATE_USERS_TABLE = """
            create table if not exists users (
                username varchar(50) not null primary key,
                password varchar(500) not null,
                enabled boolean not null
            )
            """;

    private static final String CREATE_AUTHORITIES_TABLE = """
            create table if not exists authorities (
                username varchar(50) not null,
                authority varchar(50) not null,
                constraint fk_authorities_users foreign key (username) references users(username),
                constraint uk_authorities unique (username, authority)
            )
            """;

    private static final String ADMIN_AUTHORITY = "ROLE_ADMIN";

    private final JdbcTemplate jdbcTemplate;
    private final JdbcUserDetailsManager userDetailsManager;
    private final PasswordEncoder passwordEncoder;
    private final StreamCraftAuthProperties properties;
    private final AdminPasswordGenerator passwordGenerator;

    public AdminAccountBootstrapRunner(JdbcTemplate jdbcTemplate,
                                       JdbcUserDetailsManager userDetailsManager,
                                       PasswordEncoder passwordEncoder,
                                       StreamCraftAuthProperties properties,
                                       AdminPasswordGenerator passwordGenerator) {
        this.jdbcTemplate = jdbcTemplate;
        this.userDetailsManager = userDetailsManager;
        this.passwordEncoder = passwordEncoder;
        this.properties = properties;
        this.passwordGenerator = passwordGenerator;
    }

    @Override
    public void run(ApplicationArguments args) {
        createSchemaIfMissing();

        String username = properties.username();
        if (userDetailsManager.userExists(username)) {
            ensureAdminAuthority(username);
            return;
        }

        String rawPassword = passwordGenerator.generate();
        try {
            userDetailsManager.createUser(User.withUsername(username)
                    .password(passwordEncoder.encode(rawPassword))
                    .roles("ADMIN")
                    .build());
            LOGGER.warn("Generated initial admin password for username '{}': {}", username, rawPassword);
        } catch (DataIntegrityViolationException ex) {
            if (!userDetailsManager.userExists(username)) {
                throw ex;
            }
            if (storedPasswordMatches(username, rawPassword)) {
                LOGGER.warn("Generated initial admin password for username '{}': {}", username, rawPassword);
            }
        }
        ensureAdminAuthority(username);
    }

    private void createSchemaIfMissing() {
        jdbcTemplate.execute(CREATE_USERS_TABLE);
        jdbcTemplate.execute(CREATE_AUTHORITIES_TABLE);
    }

    private void ensureAdminAuthority(String username) {
        if (hasAdminAuthority(username)) {
            return;
        }

        try {
            jdbcTemplate.update(
                    "insert into authorities (username, authority) values (?, ?)",
                    username,
                    ADMIN_AUTHORITY);
        } catch (DataIntegrityViolationException ex) {
            if (!hasAdminAuthority(username)) {
                throw ex;
            }
        }
    }

    private boolean hasAdminAuthority(String username) {
        Integer count = jdbcTemplate.queryForObject(
                "select count(*) from authorities where username = ? and authority = ?",
                Integer.class,
                username,
                ADMIN_AUTHORITY);
        return count != null && count > 0;
    }

    private boolean storedPasswordMatches(String username, String rawPassword) {
        List<String> passwords = jdbcTemplate.queryForList(
                "select password from users where username = ?",
                String.class,
                username);
        return !passwords.isEmpty() && passwordEncoder.matches(rawPassword, passwords.get(0));
    }
}
