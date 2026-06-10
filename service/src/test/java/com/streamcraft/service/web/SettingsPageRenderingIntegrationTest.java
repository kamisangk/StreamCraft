package com.streamcraft.service.web;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.streamcraft.service.auth.service.AdminPasswordGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "streamcraft.auth.remember-me-key=test-remember-me-key",
        "streamcraft.internal.token=test-internal-token",
        "spring.datasource.url=jdbc:h2:mem:settings-page-${random.uuid};DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password="
})
class SettingsPageRenderingIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void authenticatedSettingsPageRendersPasswordSettings() throws Exception {
        mockMvc.perform(get("/settings")
                        .param("lang", "en-US")
                        .with(user("admin").roles("ADMIN")))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Password settings")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("name=\"currentPassword\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("data-settings-password-form")));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestConfig {

        @Bean
        @Primary
        AdminPasswordGenerator adminPasswordGenerator() {
            return () -> "SeedPass-123";
        }
    }
}
