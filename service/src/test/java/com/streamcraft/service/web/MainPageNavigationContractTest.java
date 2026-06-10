package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.ui.ExtendedModelMap;
import org.springframework.ui.Model;

class MainPageNavigationContractTest {

    @Test
    void mainRouteUsesPipelineMonitorNavigation() {
        PageController controller = new PageController();
        Model model = new ExtendedModelMap();

        String viewName = controller.main(
                new UsernamePasswordAuthenticationToken("admin", "password"),
                model);

        assertThat(viewName).isEqualTo("main");
        assertThat(model.getAttribute("activeNav")).isEqualTo("pipeline-monitor");
    }

    @Test
    void sidebarUsesPipelineMonitorAsHomeEntry() throws Exception {
        String sidebar = loadTemplate("templates/fragments/sidebar.html");

        assertThat(sidebar)
                .doesNotContain("data-nav-key=\"dashboard\"")
                .doesNotContain("th:href=\"@{/pipelines/monitor}\"")
                .containsPattern("th:href=\"@\\{/main\\}\"[\\s\\S]*data-nav-key=\"pipeline-monitor\"");
    }

    @Test
    void sidebarContainsSettingsNavigationEntry() throws Exception {
        String sidebar = loadTemplate("templates/fragments/sidebar.html");

        assertThat(sidebar)
                .containsPattern("th:href=\"@\\{/settings\\}\"[\\s\\S]*data-nav-key=\"settings\"")
                .contains("nav.settings")
                .contains("activeNav == 'settings'");
    }

    @Test
    void shellFragmentsUseBalancedPlatformContainerClasses() throws Exception {
        String sidebar = loadTemplate("templates/fragments/sidebar.html");
        String topbar = loadTemplate("templates/fragments/topbar.html");

        assertThat(sidebar)
                .contains("data-shell-sidebar")
                .contains("sc-shell-sidebar")
                .contains("sc-nav-link")
                .contains("sc-nav-link-active")
                .contains("th:src=\"@{/brand-logo.png}\"")
                .contains("alt=\"StreamCraft\"");

        assertThat(topbar)
                .contains("data-shell-topbar")
                .contains("sc-shell-topbar")
                .contains("id=\"topbar-language-select\"")
                .contains("data-language-select")
                .contains("theme-toggle")
                .contains("data-page-title");

        assertThat(topbar.indexOf("id=\"topbar-language-select\""))
                .isLessThan(topbar.indexOf("id=\"theme-toggle\""));
    }

    @Test
    void topbarUsesCustomOutlinedSunIconForLightModeThemeToggle() throws Exception {
        String topbar = loadTemplate("templates/fragments/topbar.html");

        assertThat(topbar)
                .containsPattern("id=\"theme-toggle-light-icon\"[\\s\\S]*?fill=\"none\"")
                .containsPattern("id=\"theme-toggle-light-icon\"[\\s\\S]*?<circle cx=\"10\" cy=\"10\" r=\"3\\.25\"")
                .containsPattern("id=\"theme-toggle-light-icon\"[\\s\\S]*?stroke-linecap=\"round\"")
                .contains("M10 2.5V4.25")
                .contains("M15.303 4.697l-1.237 1.237");
    }

    private String loadTemplate(String path) throws IOException {
        ClassPathResource resource = new ClassPathResource(path);
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
