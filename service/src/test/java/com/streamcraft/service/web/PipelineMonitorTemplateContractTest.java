package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class PipelineMonitorTemplateContractTest {

    @Test
    void templateRendersRealtimeTrendCardShell() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .containsOnlyOnce("class=\"mn-module mn-module-wide mn-rise\"")
                .containsOnlyOnce("id=\"pipeline-monitor-total-input\"")
                .containsOnlyOnce("id=\"pipeline-monitor-total-output\"")
                .containsOnlyOnce("id=\"pipeline-monitor-realtime-plot\"")
                .containsOnlyOnce("id=\"pipeline-monitor-realtime-y-axis\"")
                .containsOnlyOnce("id=\"pipeline-monitor-realtime-svg\"")
                .containsOnlyOnce("id=\"pipeline-monitor-realtime-empty\"")
                .containsOnlyOnce("id=\"pipeline-monitor-realtime-tooltip\"")
                .containsOnlyOnce("id=\"pipeline-monitor-realtime-status\"")
                .containsOnlyOnce("id=\"pipeline-monitor-health-score\"")
                .containsOnlyOnce("id=\"pipeline-monitor-health-ring\"")
                .containsOnlyOnce("id=\"pipeline-monitor-health-label\"")
                .containsOnlyOnce("id=\"pipeline-monitor-runtime-targets\"")
                .containsOnlyOnce("id=\"pipeline-monitor-status-ring\"")
                .containsOnlyOnce("id=\"pipeline-monitor-status-total\"")
                .containsOnlyOnce("id=\"pipeline-monitor-status-label\"")
                .containsOnlyOnce("id=\"pipeline-monitor-status-legend\"")
                .containsOnlyOnce("id=\"pipeline-monitor-slot-ring\"")
                .containsOnlyOnce("id=\"pipeline-monitor-slot-rate\"")
                .containsOnlyOnce("id=\"pipeline-monitor-slot-label\"")
                .containsOnlyOnce("id=\"pipeline-monitor-used-slots\"")
                .containsOnlyOnce("id=\"pipeline-monitor-total-slots\"")
                .containsOnlyOnce("id=\"pipeline-monitor-available-slots\"")
                .contains("id=\"pipeline-monitor-realtime-status\" class=\"mn-plot-status hidden\"")
                .contains("grid-template-columns: repeat(5, minmax(0, 1fr));")
                .doesNotContain("id=\"pipeline-monitor-slot-usage\"")
                .doesNotContain("id=\"pipeline-monitor-status-bar\"");
    }

    @Test
    void templateBootstrapsRealtimeTrendSamplingAndWarningFlow() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("const PIPELINE_MONITOR_CHART_WINDOW_MS = 5 * 60 * 1000;")
                .contains("const PIPELINE_MONITOR_POLL_INTERVAL_MS = 5000;")
                .contains("let monitorRealtimeSamples = [];")
                .contains("function appendMonitorRealtimeSample")
                .contains("function renderMonitorRealtimeChart")
                .contains("function buildMonitorRealtimePath")
                .contains("function bindMonitorRealtimeInteractions")
                .contains("id=\"pipeline-monitor-realtime-crosshair\"")
                .contains("id=\"pipeline-monitor-realtime-x-axis\"")
                .contains("safePayload.runtimeSnapshot")
                .contains("function buildMonitorStatusRingSegments")
                .contains("function resolveMonitorSlotTone")
                .contains("const tooltipWidthPx = tooltip.getBoundingClientRect().width || tooltip.offsetWidth || 0;")
                .contains("const tooltipMarginPx = 12;")
                .contains("const tooltipCenterX = Math.max(")
                .contains("tooltip.style.left = `${tooltipCenterX}px`;")
                .contains("tooltip.style.right = 'auto';")
                .contains("tooltip.style.transform = 'translateX(-50%)';")
                .contains("const effectiveMissingPipelineCount = Number.isFinite(missingPipelineCount)")
                .contains("setMonitorRealtimeStatus(")
                .contains("setInterval(loadPipelineMonitor, PIPELINE_MONITOR_POLL_INTERVAL_MS);");
    }

    @Test
    void templateKeepsRealtimeTooltipAnchoredToHoveredSampleInsteadOfEdges() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .doesNotContain("tooltip.style.left = '0.75rem';")
                .doesNotContain("tooltip.style.right = '0.75rem';")
                .doesNotContain("pointerNormalizedX >= 0.6");
    }

    @Test
    void templateKeepsRealtimeLegendAndTotalsInHeaderRow() throws Exception {
        String normalizedTemplate = normalizeWhitespace(loadTemplate());

        assertThat(normalizedTemplate)
                .containsOnlyOnce("id=\"pipeline-monitor-realtime-legend\"")
                .containsPattern("<header class=\"mn-module-head mn-module-head-wide\">(?:(?!</header>).)*th:text=\"#\\{main.realtime.title\\}\"(?:(?!</header>).)*id=\"pipeline-monitor-realtime-legend\"(?:(?!</header>).)*id=\"pipeline-monitor-total-input\"(?:(?!</header>).)*id=\"pipeline-monitor-total-output\"(?:(?!</header>).)*</header>")
                .doesNotContain("id=\"pipeline-monitor-realtime-legend\" class=\"ml-12 mr-2 mt-4 flex items-center justify-end gap-4 text-xs\"");
    }

    @Test
    void templateGivesRealtimeTrendTheWiderDesktopSpan() throws Exception {
        String normalizedTemplate = normalizeWhitespace(loadTemplate());

        assertThat(normalizedTemplate)
                .containsPattern("th:text=\"#\\{main.status.title\\}\"[\\s\\S]*?</article>[\\s\\S]*?id=\"pipeline-monitor-slot-ring\"")
                .containsPattern("<article class=\"mn-module mn-rise\"[^>]*>(?:(?!</article>).)*th:text=\"#\\{main.status.title\\}\"")
                .containsPattern("<article class=\"mn-module mn-module-wide mn-rise\"[^>]*>(?:(?!</article>).)*th:text=\"#\\{main.realtime.title\\}\"")
                .contains(".mn-module-wide { grid-column: span 2; }")
                .doesNotContain("<article class=\"sc-summary-card md:col-span-2 xl:col-span-2 flex items-center gap-6 overflow-hidden\">")
                .doesNotContain("id=\"pipeline-monitor-realtime-card\" class=\"sc-summary-card xl:col-span-1 flex flex-col justify-between\"");
    }

    @Test
    void templateSwitchesRealtimeStatusUsingSemanticToneAndVisibility() throws Exception {
        String normalizedTemplate = normalizeWhitespace(loadTemplate());

        assertThat(normalizedTemplate)
                .contains("status.dataset.tone = tone;")
                .contains("status.textContent = message || '';")
                .contains("status.classList.toggle('hidden', !hasMessage);")
                .doesNotContain("status.classList.remove('text-slate-500 dark:text-slate-400 dark:text-slate-500'")
                .doesNotContain("status.classList.add('text-slate-500 dark:text-slate-400 dark:text-slate-500')");
    }

    @Test
    void templateUsesReadableWarningAndUnavailableTonesAcrossThemes() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains(".mn-plot-status[data-tone=\"warning\"] { color: var(--sc-warning); }")
                .contains(".mn-plot-status[data-tone=\"error\"] { color: var(--sc-danger); }")
                .contains(".mn-plot-status[data-tone=\"success\"] { color: var(--sc-success); }")
                .contains("color: 'var(--sc-danger)'")
                .contains("color: 'var(--sc-warning)'")
                .doesNotContain("text-amber-300")
                .doesNotContain("status.classList.add('text-amber-200')")
                .doesNotContain("status.classList.add('text-red-200')");
    }

    @Test
    void templateUsesLocalizedOperationsHeaderAndAccessibleCollectionStates() throws Exception {
        String template = loadTemplate();
        String shellTemplate = loadTemplate("templates/fragments/app-shell.html");

        assertThat(template)
                .contains("data-page-hero=\"monitor-overview\"")
                .contains("th:text=\"#{main.hero.label}\"")
                .contains("th:text=\"${pageTitle}\"")
                .contains("th:text=\"${pageSubtitle}\"")
                .contains("data-collection-panel=\"monitor-cards\"")
                .contains("class=\"mn-empty\" role=\"status\"");

        assertThat(shellTemplate)
                .contains(".sc-topbar-subtitle:empty")
                .contains(".sc-page-lead:empty");
    }

    @Test
    void templatePlacesRefreshControlsInsideThePageHeader() throws Exception {
        String normalizedTemplate = normalizeWhitespace(loadTemplate());

        assertThat(normalizedTemplate)
                .containsOnlyOnce("id=\"pipeline-monitor-last-refresh\"")
                .containsOnlyOnce("id=\"pipeline-monitor-refresh\"")
                .doesNotContain("id=\"pipeline-monitor-hero-refresh\"")
                .doesNotContain(">Refresh<")
                .doesNotContain("Metrics and cards stay aligned with the latest polling cycle.")
                .doesNotContain("全局任务状态每 5 秒自动刷新")
                .containsPattern("data-page-hero=\"monitor-overview\"(?:(?!</section>).)*id=\"pipeline-monitor-last-refresh\"(?:(?!</section>).)*id=\"pipeline-monitor-refresh\"(?:(?!</section>).)*</section>");
    }

    @Test
    void templateUsesThemeTokensForMonitorCards() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains(".mn-card {")
                .contains(".mn-card:hover {")
                .contains("background: var(--sc-card-strong);")
                .contains(".mn-card[data-tone=\"failed\"] { --tone: var(--sc-danger); }")
                .doesNotContain("blur-2xl")
                .doesNotContain("dark:bg-neutral-950")
                .doesNotContain("dark:bg-neutral-950/60")
                .doesNotContain("dark:bg-slate-900")
                .doesNotContain("dark:bg-slate-950")
                .doesNotContain("dark:bg-[#0b1120]")
                .doesNotContain("dark:bg-[#0b1120]/50");
    }

    private String loadTemplate() throws IOException {
        return loadTemplate("templates/main.html");
    }

    private String loadTemplate(String path) throws IOException {
        ClassPathResource resource = new ClassPathResource(path);
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    private String normalizeWhitespace(String content) {
        return content.replaceAll("\\s+", " ").trim();
    }
}
