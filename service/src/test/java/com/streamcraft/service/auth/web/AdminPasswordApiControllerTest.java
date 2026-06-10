package com.streamcraft.service.auth.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrlPattern;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.streamcraft.service.auth.service.AdminPasswordService;
import com.streamcraft.service.config.SecurityConfig;
import com.streamcraft.service.config.StreamCraftAuthProperties;
import com.streamcraft.service.security.InternalAccessProperties;
import com.streamcraft.service.security.InternalTokenFilter;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(AdminPasswordApiController.class)
@Import({
        SecurityConfig.class,
        InternalTokenFilter.class,
        AdminPasswordExceptionHandler.class,
        AdminPasswordApiControllerTest.PropertiesConfig.class
})
@TestPropertySource(properties = {
        "streamcraft.auth.remember-me-key=test-remember-me-key",
        "streamcraft.internal.token=test-internal-token"
})
class AdminPasswordApiControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AdminPasswordService adminPasswordService;

    @MockBean
    private UserDetailsService userDetailsService;

    @Test
    void updatesPasswordForAuthenticatedUser() throws Exception {
        mockMvc.perform(post("/api/settings/password")
                        .with(user("admin").roles("ADMIN"))
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "currentPassword": "CurrentPass-123",
                                  "newPassword": "NewPass-456",
                                  "confirmPassword": "NewPass-456"
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.message").value("Password updated."));

        ArgumentCaptor<UpdateAdminPasswordRequest> requestCaptor =
                ArgumentCaptor.forClass(UpdateAdminPasswordRequest.class);
        verify(adminPasswordService).updatePassword(requestCaptor.capture());
        UpdateAdminPasswordRequest request = requestCaptor.getValue();
        assertThat(request.currentPassword()).isEqualTo("CurrentPass-123");
        assertThat(request.newPassword()).isEqualTo("NewPass-456");
        assertThat(request.confirmPassword()).isEqualTo("NewPass-456");
    }

    @Test
    void returnsBadRequestWhenCurrentPasswordIsWrong() throws Exception {
        doThrow(new IllegalArgumentException("Current password is incorrect."))
                .when(adminPasswordService)
                .updatePassword(org.mockito.ArgumentMatchers.any(UpdateAdminPasswordRequest.class));

        mockMvc.perform(post("/api/settings/password")
                        .with(user("admin").roles("ADMIN"))
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "currentPassword": "WrongPass-123",
                                  "newPassword": "NewPass-456",
                                  "confirmPassword": "NewPass-456"
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Current password is incorrect."));
    }

    @Test
    void returnsBadRequestWhenConfirmationDoesNotMatch() throws Exception {
        doThrow(new IllegalArgumentException("New password confirmation does not match."))
                .when(adminPasswordService)
                .updatePassword(org.mockito.ArgumentMatchers.any(UpdateAdminPasswordRequest.class));

        mockMvc.perform(post("/api/settings/password")
                        .with(user("admin").roles("ADMIN"))
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "currentPassword": "CurrentPass-123",
                                  "newPassword": "NewPass-456",
                                  "confirmPassword": "DifferentPass-456"
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("New password confirmation does not match."));
    }

    @Test
    void returnsBadRequestForBlankPasswordFields() throws Exception {
        mockMvc.perform(post("/api/settings/password")
                        .with(user("admin").roles("ADMIN"))
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "currentPassword": "",
                                  "newPassword": "NewPass-456",
                                  "confirmPassword": "NewPass-456"
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Password fields must not be blank."));
    }

    @Test
    void redirectsAnonymousPasswordUpdateRequestsToLogin() throws Exception {
        mockMvc.perform(post("/api/settings/password")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "currentPassword": "CurrentPass-123",
                                  "newPassword": "NewPass-456",
                                  "confirmPassword": "NewPass-456"
                                }
                                """))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrlPattern("**/login"));
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableConfigurationProperties({StreamCraftAuthProperties.class, InternalAccessProperties.class})
    static class PropertiesConfig {
    }
}
