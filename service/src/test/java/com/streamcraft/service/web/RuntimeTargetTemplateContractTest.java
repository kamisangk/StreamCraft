package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class RuntimeTargetTemplateContractTest {

    @Test
    void runtimeTargetPageConfiguresOneRuntimeTargetInline() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"runtime-target-form\"")
                .contains("id=\"standalone-target-panel\"")
                .contains("id=\"target-job-manager-url\"")
                .contains("id=\"save-runtime-target\"")
                .doesNotContain("data-runtime-mode=\"FLINK_STANDALONE\"")
                .doesNotContain("data-runtime-mode=\"FLINK_YARN_APPLICATION\"")
                .doesNotContain("id=\"yarn-target-panel\"")
                .doesNotContain("id=\"target-standalone-flink-home\"")
                .doesNotContain("id=\"target-yarn-flink-home\"")
                .doesNotContain("FLINK_CLI")
                .doesNotContain("Flink CLI")
                .doesNotContain("Flink on YARN")
                .doesNotContain("id=\"open-add-target-modal\"")
                .doesNotContain("id=\"add-target-modal\"")
                .doesNotContain("id=\"runtime-target-table\"")
                .doesNotContain("delete-runtime-target");
    }

    @Test
    void runtimeTargetPageUsesJobManagerRestConfigurationOnly() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .doesNotContain("id=\"standalone-rest-config\"")
                .doesNotContain("id=\"standalone-cli-config\"")
                .doesNotContain("standalone-submission-mode-label")
                .doesNotContain("submission-mode-segmented-control")
                .contains("id=\"target-job-manager-url\"")
                .contains("jobManagerUrl: fieldValue('target-job-manager-url')")
                .doesNotContain("isCliStandalone")
                .doesNotContain("standaloneSubmissionMode")
                .doesNotContain("flinkHome")
                .doesNotContain("flinkConfDir")
                .doesNotContain("hadoopConfDir");
    }

    @Test
    void runtimeTargetSummaryDoesNotContainCliConfiguredState() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .doesNotContain("CONFIGURED: t('runtimeTarget.status.configured', 'Configured')")
                .doesNotContain("function isCliBackedRuntimeTarget")
                .doesNotContain("target?.type === 'FLINK_YARN_APPLICATION'")
                .doesNotContain("standaloneSubmissionMode === 'FLINK_CLI'")
                .doesNotContain("displayStatus");
    }

    @Test
    void runtimeTargetSummaryShowsMissingSlotCapacityAsUnknown() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("function slotCountText(value)")
                .contains("Number.isFinite(parsed) ? String(parsed) : '--'")
                .contains("const totalSlotsText = slotCountText(target.totalSlots)")
                .contains("const availableSlotsText = slotCountText(target.availableSlots)")
                .contains("${totalSlotsText} <span")
                .contains("${t('runtimeTarget.summary.available', 'available')} / ${availableSlotsText}")
                .doesNotContain("const totalSlots = Number(target.totalSlots) || 0")
                .doesNotContain("const availableSlots = Number(target.availableSlots) || 0");
    }

    @Test
    void runtimeTargetSummaryRefreshesWhenEditingRuntimeMode() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .doesNotContain("function currentRuntimeTargetDraft()")
                .doesNotContain("function setRuntimeMode(mode)")
                .doesNotContain("setStandaloneSubmissionMode")
                .contains("mode: 'FLINK_STANDALONE'");
    }

    @Test
    void runtimeTargetPageUsesSingletonApiAndNoListPolling() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("async function loadRuntimeTarget()")
                .contains("async function saveRuntimeTarget()")
                .contains("fetch('/api/runtime-target')")
                .contains("fetch('/api/runtime-target/standalone'")
                .contains("method: 'PUT'")
                .doesNotContain("method: 'POST'")
                .doesNotContain("method: 'DELETE'")
                .doesNotContain("setInterval(loadRuntimeTargets")
                .doesNotContain("/api/runtime-target/yarn-application")
                .doesNotContain("runtimeTargetState.mode === 'FLINK_YARN_APPLICATION'");
    }

    @Test
    void runtimeTargetPageUsesSharedCardsFormsAndSingleSettingsSurface() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("sc-summary-card")
                .contains("sc-page-hero")
                .contains("sc-card-strong")
                .contains("sc-btn-primary")
                .contains("sc-input")
                .contains("sc-form-label")
                .doesNotContain("data-collection-panel=\"runtime-target-list\"");
    }

    @Test
    void runtimeTargetPageUsesCalStyleNeutralSegmentedControls() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .doesNotContain("runtime-mode-segmented-control")
                .doesNotContain("runtime-mode-segmented-option")
                .doesNotContain("submission-mode-segmented-control")
                .doesNotContain("submission-mode-segmented-option")
                .doesNotContain("hover:border-blue-500/70")
                .doesNotContain("border-blue-500")
                .doesNotContain("bg-blue-50")
                .doesNotContain("text-blue-700");
    }

    private String loadTemplate() throws IOException {
        ClassPathResource resource = new ClassPathResource("templates/runtime-target.html");
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
