package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class StudioRouteInspectorContractTest {

    @Test
    void scriptPersistsRouteConfigWithStructuredRows() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"ROUTE\"")
                .contains("defaultName: t(\"studio.operator.route\", \"Route\")")
                .contains("defaultConfig: { matchMode: \"FIRST_MATCH\", includeUnmatched: true, unmatchedPort: \"unmatched\", routes: [{ portId: \"matched\", condition: \"\" }], note: \"\" }")
                .contains("function normalizeRouteItems(")
                .contains("function renderRouteItems(")
                .contains("function collectRouteItems(")
                .contains("function appendRouteItem(")
                .contains("function removeRouteItem(")
                .contains("function moveRouteItem(")
                .contains("function updateRouteConfigUI(")
                .contains("selectedNode.config.matchMode = byId(\"route-match-mode\")?.value || \"FIRST_MATCH\";")
                .contains("selectedNode.config.includeUnmatched = Boolean(byId(\"route-include-unmatched\")?.checked);")
                .contains("selectedNode.config.unmatchedPort = byId(\"route-unmatched-port\")?.value?.trim?.() || \"unmatched\";")
                .contains("selectedNode.config.routes = collectRouteItems();")
                .contains("byId(\"route-config\")?.classList.remove(\"hidden\");")
                .contains("renderRouteItems(selectedNode.config.routes || []);")
                .contains("updateRouteConfigUI(selectedNode.config.includeUnmatched !== false);")
                .contains("byId(\"add-route-item-button\")?.addEventListener(\"click\", () => {")
                .contains("byId(\"route-rule-list\")?.addEventListener(\"input\", event => {")
                .contains("byId(\"route-rule-list\")?.addEventListener(\"click\", event => {")
                .contains("event.target.closest(\"[data-role='move-route-up']\")")
                .contains("event.target.closest(\"[data-role='move-route-down']\")")
                .contains("event.target.closest(\"[data-role='remove-route']\")")
                .contains("\"route-match-mode\"")
                .contains("\"route-include-unmatched\"")
                .contains("\"route-unmatched-port\"")
                .doesNotContain("\"route-routes\"");
    }

    @Test
    void scriptValidatesRoutePortShapeAndUniqueness() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("const routePortPattern = /^[A-Za-z0-9_-]+$/;")
                .contains("studio.validation.route.invalidPortId")
                .contains("studio.validation.route.duplicatePortId")
                .contains("seenPorts.has(portId)");
    }

    private String loadScript() throws IOException {
        ClassPathResource resource = new ClassPathResource("static/js/studio-editor.js");
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
