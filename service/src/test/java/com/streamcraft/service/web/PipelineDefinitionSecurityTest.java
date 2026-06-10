package com.streamcraft.service.web;

import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrlPattern;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.service.config.SecurityConfig;
import com.streamcraft.service.config.StreamCraftAuthProperties;
import com.streamcraft.service.pipeline.service.PipelineService;
import com.streamcraft.service.pipeline.web.PipelineApiController;
import com.streamcraft.service.security.InternalAccessProperties;
import com.streamcraft.service.security.InternalTokenFilter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(PipelineApiController.class)
@Import({
        SecurityConfig.class,
        InternalTokenFilter.class,
        PipelineDefinitionSecurityTest.PropertiesConfig.class
})
@TestPropertySource(properties = {
        "streamcraft.auth.remember-me-key=test-remember-me-key",
        "streamcraft.internal.token=test-internal-token"
})
class PipelineDefinitionSecurityTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private PipelineService pipelineService;

    @MockBean
    private UserDetailsService userDetailsService;

    @Test
    void redirectsAnonymousDefinitionRequestsToLogin() throws Exception {
        when(pipelineService.getDefinition(1L)).thenReturn(objectMapper.readTree("""
                {
                  "nodes": [],
                  "edges": []
                }
                """));

        mockMvc.perform(get("/api/pipelines/1/definition"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrlPattern("**/login"));
    }

    @Test
    void allowsAuthenticatedUsersToReadDefinition() throws Exception {
        when(pipelineService.getDefinition(1L)).thenReturn(objectMapper.readTree("""
                {
                  "nodes": [],
                  "edges": []
                }
                """));

        mockMvc.perform(get("/api/pipelines/1/definition").with(user("admin").roles("ADMIN")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nodes").isArray())
                .andExpect(jsonPath("$.edges").isArray());
    }

    @Test
    void allowsInternalTokenToReadDefinition() throws Exception {
        when(pipelineService.getDefinition(1L)).thenReturn(objectMapper.readTree("""
                {
                  "nodes": [],
                  "edges": []
                }
                """));

        mockMvc.perform(get("/api/pipelines/1/definition")
                        .header("X-StreamCraft-Internal-Token", "test-internal-token"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nodes").isArray())
                .andExpect(jsonPath("$.edges").isArray());
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableConfigurationProperties({StreamCraftAuthProperties.class, InternalAccessProperties.class})
    static class PropertiesConfig {
    }
}
