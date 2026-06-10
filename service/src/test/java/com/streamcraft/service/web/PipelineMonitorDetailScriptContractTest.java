package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class PipelineMonitorDetailScriptContractTest {

    @Test
    void studioEditorSupportsSeparateEditorAndMonitorModes() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("const PAGE_MODE_EDITOR = \"editor\";")
                .contains("const PAGE_MODE_MONITOR = \"monitor\";")
                .contains("pageMode: STUDIO_BOOTSTRAP?.dataset.pageMode || PAGE_MODE_EDITOR")
                .contains("function isMonitorMode()")
                .contains("function isEditorMode()")
                .contains("if (isEditorMode())")
                .contains("if (isMonitorMode())")
                .contains("function bindNodeInteractions()")
                .contains("if (!isEditorMode())")
                .contains("function bindEdgeInteractions()")
                .contains("layer.classList.toggle(\"pointer-events-none\", !isEditorMode())");
    }

    @Test
    void studioEditorMonitorModeRendersMetricTilesAndClientSideRateHelpers() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("function calculateRate(currentValue, previousValue, intervalSeconds)")
                .contains("function formatMetricCount(value)")
                .contains("function formatMetricRate(value)")
                .contains("function escapeHtml(value)")
                .contains("metrics?.statusTone === \"error\"")
                .contains("escapeHtml(monitorStatusText)")
                .contains("t(\"studio.monitor.inputTotal\"")
                .contains("t(\"studio.monitor.outputTotal\"")
                .contains("t(\"studio.monitor.inputRate\"")
                .contains("t(\"studio.monitor.outputRate\"")
                .contains("t(\"studio.monitor.metrics.empty\"")
                .contains("t(\"studio.monitor.metrics.error\"");
    }

    @Test
    void studioEditorMonitorModeUsesInputMetricsForSinkOutputTiles() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("const displayedOutputRecords = node.type === \"SINK\" ? metrics?.inputRecords : metrics?.outputRecords;")
                .contains("const displayedOutputRate = node.type === \"SINK\" ? metrics?.inputRate : metrics?.outputRate;")
                .contains("const outputCount = formatMetricCount(displayedOutputRecords);")
                .contains("const outputRate = formatMetricRate(displayedOutputRate);");
    }

    @Test
    void studioEditorEditorModeSupportsNodeContextMenuDeletion() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("contextMenu: { visible: false, nodeId: null, x: 0, y: 0 }")
                .contains("function showNodeContextMenu(nodeId, event)")
                .contains("function hideNodeContextMenu()")
                .contains("function renderNodeContextMenu()")
                .contains("function duplicateNode(nodeId)")
                .contains("nodeElement.addEventListener(\"contextmenu\", event => {")
                .contains("showNodeContextMenu(nodeElement.dataset.nodeId, event);")
                .contains("data-node-context-action=\"copy\"")
                .contains("data-node-context-action=\"delete\"")
                .contains("duplicateNode(state.contextMenu.nodeId);")
                .contains("deleteNode(state.contextMenu.nodeId);")
                .contains("state.edges = [...state.edges];")
                .contains("if (event.key === \"Escape\")")
                .contains("hideNodeContextMenu();");
    }

    @Test
    void studioEditorMonitorModePollsMetricsAndCleansUpOnUnload() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("const MONITOR_REFRESH_INTERVAL_MS = 5000;")
                .contains("async function refreshMonitorMetrics()")
                .contains("function applyMonitorMetrics(metrics)")
                .contains("function startMonitorRefresh()")
                .contains("function stopMonitorRefresh()")
                .contains("window.addEventListener(\"beforeunload\", stopMonitorRefresh);")
                .contains("state.monitorRefreshTimer = window.setInterval(refreshMonitorMetrics, MONITOR_REFRESH_INTERVAL_MS);")
                .contains("t(\"studio.monitor.metrics.errorPreserved\"")
                .contains("statusTone: \"error\"")
                .contains("monitorLastSampleAt: null")
                .contains("monitorRefreshInFlight: false")
                .contains("if (state.monitorRefreshInFlight)")
                .contains("sampledAt - state.monitorLastSampleAt")
                .contains("state.monitorLastSampleAt = null;")
                .contains("AbortController")
                .contains("controller.abort()")
                .contains("signal: controller.signal")
                .contains("loadExistingPipeline().then(loaded => {")
                .contains("if (!loaded)");
    }

    private String loadScript() throws IOException {
        ClassPathResource resource = new ClassPathResource("static/js/studio-editor.js");
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
