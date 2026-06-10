package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class StudioCaseWhenInspectorContractTest {

    @Test
    void scriptPersistsCaseWhenConfigWithStructuredRows() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"CASE_WHEN\"")
                .contains("defaultName: t(\"studio.operator.caseWhen\", \"Case when\")")
                .contains("defaultConfig: { targetField: \"\", cases: [{ condition: \"\", value: \"\" }], defaultMode: \"NONE\", defaultValue: \"\", defaultExpression: \"\", note: \"\" }")
                .contains("function normalizeCaseWhenItems(")
                .contains("function renderCaseWhenItems(")
                .contains("function collectCaseWhenItems(")
                .contains("function appendCaseWhenItem(")
                .contains("function removeCaseWhenItem(")
                .contains("function moveCaseWhenItem(")
                .contains("function updateCaseWhenValueModeUI(")
                .contains("function updateCaseWhenDefaultModeUI(")
                .contains("selectedNode.config.targetField = byId(\"case-when-target-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.cases = collectCaseWhenItems();")
                .contains("const caseWhenDefaultMode = byId(\"case-when-default-mode\")?.value || \"NONE\";")
                .contains("selectedNode.config.defaultMode = caseWhenDefaultMode;")
                .contains("selectedNode.config.defaultValue = caseWhenDefaultMode === \"VALUE\" ? byId(\"case-when-default-value\")?.value || \"\" : \"\";")
                .contains("selectedNode.config.defaultExpression = caseWhenDefaultMode === \"EXPRESSION\" ? byId(\"case-when-default-expression\")?.value?.trim?.() || \"\" : \"\";")
                .contains("renderCaseWhenItems(selectedNode.config.cases || []);")
                .contains("updateCaseWhenDefaultModeUI(selectedNode.config.defaultMode || \"NONE\");")
                .contains("byId(\"add-case-when-item-button\")?.addEventListener(\"click\", () => {")
                .contains("byId(\"case-when-rule-list\")?.addEventListener(\"input\", event => {")
                .contains("byId(\"case-when-rule-list\")?.addEventListener(\"change\", event => {")
                .contains("byId(\"case-when-rule-list\")?.addEventListener(\"click\", event => {")
                .contains("event.target.closest(\"[data-role='move-case-when-up']\")")
                .contains("event.target.closest(\"[data-role='move-case-when-down']\")")
                .contains("event.target.closest(\"[data-role='remove-case-when']\")")
                .contains("\"case-when-target-field\"")
                .contains("\"case-when-default-mode\"")
                .contains("\"case-when-default-value\"")
                .contains("\"case-when-default-expression\"")
                .doesNotContain("\"case-when-cases\"");
    }

    @Test
    void scriptValidatesCaseWhenValuesBySelectedMode() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("caseValueMode(item) === \"EXPRESSION\"")
                .contains("studio.validation.caseWhen.valueRequired")
                .contains("studio.validation.caseWhen.defaultValueRequired");
    }

    private String loadScript() throws IOException {
        ClassPathResource resource = new ClassPathResource("static/js/studio-editor.js");
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
