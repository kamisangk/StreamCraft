package com.streamcraft.service.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.streamcraft.service.config.SecurityConfig;
import com.streamcraft.service.config.StreamCraftAuthProperties;
import com.streamcraft.service.pipeline.service.PipelineService;
import com.streamcraft.service.pipeline.web.PipelineApiController;
import com.streamcraft.service.pipeline.web.PipelinePreviewOutputResponse;
import com.streamcraft.service.pipeline.web.PipelinePreviewResponse;
import com.streamcraft.service.security.InternalAccessProperties;
import com.streamcraft.service.security.InternalTokenFilter;
import java.util.List;
import org.junit.jupiter.api.Test;
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

@WebMvcTest(PipelineApiController.class)
@Import({
        SecurityConfig.class,
        InternalTokenFilter.class,
        PipelinePreviewSecurityTest.PropertiesConfig.class
})
@TestPropertySource(properties = {
        "streamcraft.auth.remember-me-key=test-remember-me-key",
        "streamcraft.internal.token=test-internal-token"
})
class PipelinePreviewSecurityTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private PipelineService pipelineService;

    @MockBean
    private UserDetailsService userDetailsService;

    @Test
    void allowsAuthenticatedUsersToPreviewCurrentDefinition() throws Exception {
        when(pipelineService.preview(any())).thenReturn(new PipelinePreviewResponse(List.of(
                new PipelinePreviewOutputResponse("sink-1", "订单输出", List.of("{\"status\":\"ok\"}"))
        )));

        mockMvc.perform(post("/api/pipelines/preview")
                        .with(user("admin").roles("ADMIN"))
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "name": "Preview",
                                  "description": "unsaved graph",
                                  "definitionJson": "{\\"pipelineId\\":\\"draft\\",\\"nodes\\":[],\\"edges\\":[]}"
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outputs[0].nodeId").value("sink-1"))
                .andExpect(jsonPath("$.outputs[0].nodeName").value("订单输出"))
                .andExpect(jsonPath("$.outputs[0].records[0]").value("{\"status\":\"ok\"}"));
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableConfigurationProperties({StreamCraftAuthProperties.class, InternalAccessProperties.class})
    static class PropertiesConfig {
    }
}
