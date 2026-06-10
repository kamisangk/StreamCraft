package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class PipelineMonitorDetailTemplateContractTest {

    @Test
    void templateUsesReadonlyCanvasShellInsteadOfLegacyMetricCards() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("head th:replace=\"~{fragments/app-shell :: commonHead(#{monitorDetail.title})}\"")
                .contains("class=\"sc-shell-page")
                .contains("id=\"studio-bootstrap\"")
                .contains("data-page-mode=\"monitor\"")
                .contains("th:attr=\"data-pipeline-id=${pipelineId}\"")
                .contains("id=\"canvas-container\"")
                .contains("id=\"canvas-drop-zone\"")
                .contains("id=\"edges-layer\"")
                .contains("id=\"nodes-layer\"")
                .contains("id=\"refresh-button\"")
                .contains("id=\"monitor-last-refresh\"")
                .contains("data-monitor-shell=\"readonly\"")
                .contains("data-monitor-toolbar=\"actions\"")
                .contains("sc-btn-secondary")
                .contains("sc-card-strong")
                .contains("sc-surface")
                .contains(".node-port")
                .contains(".port-left")
                .contains(".port-right")
                .contains(".drag-handle")
                .contains(".drag-handle:active")
                .contains(".draggable-node")
                .contains(".draggable-node.node-active")
                .contains("/js/studio-editor.js")
                .doesNotContain("id=\"operator-palette\"")
                .doesNotContain("id=\"source-config-panel\"")
                .doesNotContain("id=\"transform-config-panel\"")
                .doesNotContain("id=\"sink-config-panel\"")
                .doesNotContain("id=\"node-metrics\"");
    }

    private String loadTemplate() throws IOException {
        ClassPathResource resource = new ClassPathResource("templates/pipeline-monitor-detail.html");
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
