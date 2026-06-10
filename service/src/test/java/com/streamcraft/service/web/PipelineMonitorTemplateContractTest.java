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
                .containsOnlyOnce("id=\"pipeline-monitor-realtime-card\"")
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
                .contains("id=\"pipeline-monitor-realtime-status\" class=\"hidden mt-2 text-[10px] text-slate-500 dark:text-slate-400 line-clamp-1\"")
                .contains("grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-6")
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
                .containsPattern("id=\"pipeline-monitor-realtime-card\"[\\s\\S]*?<div class=\"flex flex-wrap items-center justify-between gap-2\">[\\s\\S]*?<h3 class=\"text-slate-500 dark:text-slate-400 dark:text-slate-500 text-sm font-medium mr-2\"[^>]*th:text=\"#\\{main.realtime.title\\}\"[^>]*>[\\s\\S]*?</h3>[\\s\\S]*?<div id=\"pipeline-monitor-realtime-legend\" class=\"flex flex-shrink-0 items-center justify-end gap-3 text-\\[10px\\] font-medium\">[\\s\\S]*?id=\"pipeline-monitor-total-input\"[\\s\\S]*?id=\"pipeline-monitor-total-output\"[\\s\\S]*?</div>[\\s\\S]*?</div>")
                .doesNotContain("id=\"pipeline-monitor-realtime-legend\" class=\"ml-12 mr-2 mt-4 flex items-center justify-end gap-4 text-xs\"");
    }

    @Test
    void templateGivesRealtimeTrendTheWiderDesktopSpan() throws Exception {
        String normalizedTemplate = normalizeWhitespace(loadTemplate());

        assertThat(normalizedTemplate)
                .containsPattern("th:text=\"#\\{main.status.title\\}\"[\\s\\S]*?</article>[\\s\\S]*?id=\"pipeline-monitor-slot-ring\"")
                .containsPattern("<article class=\"sc-summary-card md:col-span-2 xl:col-span-1 flex items-center gap-6 overflow-hidden\">[\\s\\S]*?th:text=\"#\\{main.status.title\\}\"")
                .containsPattern("id=\"pipeline-monitor-realtime-card\" class=\"sc-summary-card xl:col-span-2 flex flex-col justify-between\"")
                .doesNotContain("<article class=\"sc-summary-card md:col-span-2 xl:col-span-2 flex items-center gap-6 overflow-hidden\">")
                .doesNotContain("id=\"pipeline-monitor-realtime-card\" class=\"sc-summary-card xl:col-span-1 flex flex-col justify-between\"");
    }

    @Test
    void templateUsesSingleClassTokensForRealtimeStatusToneSwitching() throws Exception {
        String normalizedTemplate = normalizeWhitespace(loadTemplate());

        assertThat(normalizedTemplate)
                .contains("status.classList.remove( 'text-slate-500', 'dark:text-slate-400', 'text-amber-700', 'dark:text-amber-200', 'text-red-700', 'dark:text-red-200', 'text-emerald-600', 'dark:text-emerald-200' );")
                .contains("status.classList.add('text-slate-500', 'dark:text-slate-400');")
                .doesNotContain("status.classList.remove('text-slate-500 dark:text-slate-400 dark:text-slate-500'")
                .doesNotContain("status.classList.add('text-slate-500 dark:text-slate-400 dark:text-slate-500')");
    }

    @Test
    void templateUsesReadableWarningAndUnavailableTonesAcrossThemes() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("text-amber-700")
                .contains("dark:text-amber-200")
                .contains("text-red-700")
                .contains("dark:text-red-200")
                .contains("metricsAvailable ? 'text-emerald-600 dark:text-emerald-200' : 'text-amber-700 dark:text-amber-200'")
                .doesNotContain("text-amber-300")
                .doesNotContain("status.classList.add('text-amber-200')")
                .doesNotContain("status.classList.add('text-red-200')");
    }

    @Test
    void templateUsesOperationsHeroAndStructuredCollectionSurfaces() throws Exception {
        String template = loadTemplate();
        String shellTemplate = loadTemplate("templates/fragments/app-shell.html");

        assertThat(template)
                .contains("data-page-hero=\"monitor-overview\"")
                .contains("sc-page-hero")
                .contains("sc-page-heading")
                .contains("sc-page-lead")
                .contains("sc-surface")
                .contains("data-collection-panel=\"monitor-cards\"")
                .contains("sc-empty-state");

        assertThat(shellTemplate)
                .contains(".sc-topbar-subtitle:empty")
                .contains(".sc-page-lead:empty");
    }

    @Test
    void templatePlacesRefreshControlsInsideHeroUtilityCard() throws Exception {
        String normalizedTemplate = normalizeWhitespace(loadTemplate());

        assertThat(normalizedTemplate)
                .containsOnlyOnce("id=\"pipeline-monitor-last-refresh\"")
                .containsOnlyOnce("id=\"pipeline-monitor-refresh\"")
                .doesNotContain("id=\"pipeline-monitor-hero-refresh\"")
                .doesNotContain(">Refresh<")
                .doesNotContain("Metrics and cards stay aligned with the latest polling cycle.")
                .doesNotContain("全局任务状态每 5 秒自动刷新")
                .containsPattern("data-page-hero=\"monitor-overview\"[\\s\\S]*?<div class=\"sc-surface px-4 py-4 min-w-\\[280px\\] max-w-xl flex-1 lg:flex-none lg:min-w-\\[360px\\]\">[\\s\\S]*?id=\"pipeline-monitor-last-refresh\"[\\s\\S]*?id=\"pipeline-monitor-refresh\"[\\s\\S]*?</div>[\\s\\S]*?</section>");
    }

    @Test
    void templateUsesSharedMonitorSurfaceGrammar() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("sc-panel-muted")
                .contains("sc-card-interactive")
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
