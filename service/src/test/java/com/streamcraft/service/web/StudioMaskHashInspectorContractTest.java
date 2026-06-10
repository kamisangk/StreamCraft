package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class StudioMaskHashInspectorContractTest {

    @Test
    void scriptPersistsMaskHashConfigWithStructuredRows() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"MASK_HASH\"")
                .contains("defaultName: t(\"studio.operator.maskHash\", \"Sensitive field handling\")")
                .contains("defaultConfig: { rules: [{ sourceField: \"\", targetField: \"\", action: \"MASK\", algorithm: \"SHA256\", salt: \"\", maskChar: \"*\", keepFirst: 3, keepLast: 4 }], note: \"\" }")
                .contains("function normalizeMaskHashRules(")
                .contains("function renderMaskHashRules(")
                .contains("function collectMaskHashRules(")
                .contains("function appendMaskHashRule(")
                .contains("function removeMaskHashRule(")
                .contains("function moveMaskHashRule(")
                .contains("function updateMaskHashActionUI(")
                .contains("selectedNode.config.rules = collectMaskHashRules();")
                .contains("renderMaskHashRules(selectedNode.config.rules || []);")
                .contains("byId(\"add-mask-hash-rule-button\")?.addEventListener(\"click\", () => {")
                .contains("byId(\"mask-hash-rule-list\")?.addEventListener(\"input\", event => {")
                .contains("byId(\"mask-hash-rule-list\")?.addEventListener(\"change\", event => {")
                .contains("byId(\"mask-hash-rule-list\")?.addEventListener(\"click\", event => {")
                .contains("event.target.closest(\"[data-role='move-mask-hash-up']\")")
                .contains("event.target.closest(\"[data-role='move-mask-hash-down']\")")
                .contains("event.target.closest(\"[data-role='remove-mask-hash']\")")
                .doesNotContain("\"mask-hash-rules\"");
    }

    @Test
    void scriptValidatesMaskHashActionSpecificFields() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("rule.action === \"HASH\"")
                .contains("studio.validation.maskHash.targetFieldRequired")
                .contains("studio.validation.maskHash.maskCharRequired")
                .contains("studio.validation.maskHash.keepFirstNonNegative")
                .contains("studio.validation.maskHash.keepLastNonNegative");
    }

    @Test
    void actionOptionsDescribeMaskAndHashRuntimeEffects() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("<option value=\"MASK\" th:text=\"#{studio.select.maskHash.mask}\">Mask: keep ends, replace middle</option>")
                .contains("<option value=\"HASH\" th:text=\"#{studio.select.maskHash.hash}\">Hash: create a non-reversible string</option>");
        assertMaskHashActionMessages(loadMessages("messages.properties"));
        assertMaskHashActionMessages(loadMessages("messages_en_US.properties"));
        Properties zhMessages = loadMessages("messages_zh_CN.properties");
        assertThat(zhMessages)
                .containsEntry(
                        "studio.select.maskHash.mask",
                        "\u8131\u654f\uff1a\u4fdd\u7559\u9996\u5c3e\uff0c\u4e2d\u95f4\u66ff\u6362")
                .containsEntry(
                        "studio.select.maskHash.hash",
                        "\u54c8\u5e0c\uff1a\u751f\u6210\u4e0d\u53ef\u8fd8\u539f\u5b57\u7b26\u4e32");
    }

    private void assertMaskHashActionMessages(Properties messages) {
        assertThat(messages)
                .containsEntry("studio.select.maskHash.mask", "Mask: keep ends, replace middle")
                .containsEntry("studio.select.maskHash.hash", "Hash: create a non-reversible string");
    }

    private String loadScript() throws IOException {
        ClassPathResource resource = new ClassPathResource("static/js/studio-editor.js");
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    private String loadTemplate() throws IOException {
        ClassPathResource resource = new ClassPathResource("templates/studio.html");
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    private Properties loadMessages(String path) throws IOException {
        ClassPathResource resource = new ClassPathResource(path);
        Properties properties = new Properties();
        properties.load(new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8));
        return properties;
    }
}
