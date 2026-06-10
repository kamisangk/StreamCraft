package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class StudioTimeDeriveInspectorContractTest {

    @Test
    void scriptPersistsTimeDeriveConfigWithStructuredDerivations() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"TIME_DERIVE\"")
                .contains("defaultName: t(\"studio.operator.timeDerive\", \"Time derive\")")
                .contains("defaultConfig: { sourceField: \"\", sourceFormat: \"AUTO\", sourcePattern: \"\", sourceTimeZone: \"UTC\", outputTimeZone: \"UTC\", parseErrorStrategy: \"KEEP_ORIGINAL\", derivations: [{ outputField: \"dt\", type: \"DATE\", pattern: \"\" }], note: \"\" }")
                .contains("function normalizeTimeDeriveItems(")
                .contains("function renderTimeDeriveItems(")
                .contains("function collectTimeDeriveItems(")
                .contains("function appendTimeDeriveItem(")
                .contains("function removeTimeDeriveItem(")
                .contains("function moveTimeDeriveItem(")
                .contains("function updateTimeDeriveSourceFormatUI(")
                .contains("function updateTimeDeriveDerivationUI(")
                .contains("selectedNode.config.sourceField = byId(\"time-derive-source-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.sourceFormat = byId(\"time-derive-source-format\")?.value || \"AUTO\";")
                .contains("selectedNode.config.sourcePattern = byId(\"time-derive-source-pattern\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.sourceTimeZone = byId(\"time-derive-source-time-zone\")?.value?.trim?.() || \"UTC\";")
                .contains("selectedNode.config.outputTimeZone = byId(\"time-derive-output-time-zone\")?.value?.trim?.() || \"UTC\";")
                .contains("selectedNode.config.parseErrorStrategy = byId(\"time-derive-parse-error-strategy\")?.value || \"KEEP_ORIGINAL\";")
                .contains("selectedNode.config.derivations = collectTimeDeriveItems();")
                .contains("renderTimeDeriveItems(selectedNode.config.derivations || []);")
                .contains("updateTimeDeriveSourceFormatUI(selectedNode.config.sourceFormat || \"AUTO\");")
                .contains("byId(\"add-time-derive-item-button\")?.addEventListener(\"click\", () => {")
                .contains("byId(\"time-derive-derivation-list\")?.addEventListener(\"input\", event => {")
                .contains("byId(\"time-derive-derivation-list\")?.addEventListener(\"change\", event => {")
                .contains("byId(\"time-derive-derivation-list\")?.addEventListener(\"click\", event => {")
                .contains("event.target.closest(\"[data-role='move-time-derive-up']\")")
                .contains("event.target.closest(\"[data-role='move-time-derive-down']\")")
                .contains("event.target.closest(\"[data-role='remove-time-derive']\")")
                .doesNotContain("\"time-derive-derivations\"");
    }

    @Test
    void scriptValidatesTimeDeriveDerivationFields() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("studio.validation.timeDerive.outputFieldRequired")
                .contains("studio.validation.timeDerive.duplicateOutputField")
                .contains("studio.validation.timeDerive.patternRequired");
    }

    private String loadScript() throws IOException {
        ClassPathResource resource = new ClassPathResource("static/js/studio-editor.js");
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
