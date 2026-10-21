package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class StudioEditorFilterPortsContractTest {

    @Test
    void templateDefinesFilterStackedPortHooks() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains(".port-stack")
                .contains(".port-right-stack")
                .contains(".port-right-stack { right: -6px; align-items: flex-end; }")
                .contains(".port-label")
                .doesNotContain(".port-right-stack { right: -36px;")
                .doesNotContain(".port-right-stack { right: -50px;");
    }

    @Test
    void scriptDefinesExplicitFilterOutputPortsAndPortAwareAnchors() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("const PORT_CONTRACT = window.StreamCraftPipelinePortContract;")
                .contains("function outputPortsForNode(node)")
                .contains("return PORT_CONTRACT.outputPortsForNode(node).map(portId => ({ id: portId, label: \"\" }));")
                .contains("function inputPortsForNode(node)")
                .contains("return PORT_CONTRACT.inputPortsForNode(node).map(portId => ({ id: portId, label: \"\" }));")
                .contains("function resolvePortAnchor(nodeId, direction, portId)")
                .contains("resolvePortAnchor(edge.sourceNodeId, \"output\", edge.sourcePortId)")
                .contains("resolvePortAnchor(edge.targetNodeId, \"input\", edge.targetPortId)")
                .doesNotContain("port-right-stack { right: -36px;");
    }

    @Test
    void scriptOmitsEmptyInputPortLabelsSoStackedPortsStayOnNodeEdge() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("const inputPortLabel = port.label ? `<span class=\"port-label port-label-left\">${escapeHtml(port.label)}</span>` : \"\";")
                .contains("${inputPortLabel}")
                .doesNotContain("<span class=\"port-label port-label-left\">${escapeHtml(port.label || \"\")}</span>");
    }

    @Test
    void scriptTreatsFilterPortIdsAsPartOfConnectionIdentity() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("edge.sourceNodeId === source?.id")
                .contains("edge.sourcePortId === state.connectState.sourcePortId")
                .contains("sourcePortId: state.connectState.sourcePortId");
    }

    @Test
    void scriptDoesNotSilentlyDefaultLegacyFilterSourcePortsToOutputZero() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("function normalizeEdge(edge)")
                .contains("sourcePortId: edge.sourcePortId")
                .contains("targetPortId: edge.targetPortId")
                .contains("state.edges = Array.isArray(definition?.edges) ? definition.edges.map(normalizeEdge) : [];")
                .doesNotContain("sourcePortId: edge.sourcePortId || DEFAULT_SOURCE_PORT_ID")
                .doesNotContain("targetPortId: edge.targetPortId || DEFAULT_TARGET_PORT_ID")
                .doesNotContain("const resolvedPortId = portId || (direction === \"input\" ? DEFAULT_TARGET_PORT_ID : DEFAULT_SOURCE_PORT_ID);");
    }

    @Test
    void scriptDefinesStackedDataQualityCleanAndDirtyOutputPorts() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("const PORT_CONTRACT = window.StreamCraftPipelinePortContract;")
                .contains("return PORT_CONTRACT.outputPortsForNode(node).map(portId => ({ id: portId, label: \"\" }));")
                .doesNotContain("DEFAULT_SOURCE_PORT_ID")
                .doesNotContain("DATA_QUALITY_OUTPUT_PORT_IDS");
    }

    @Test
    void scriptClampsCanvasPlacementForFilterPortStackReachability() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("const FILTER_PORT_STACK_OUTSET_X = 6;")
                .contains("function canvasWidthFootprintForNode(node)")
                .contains("function clampCanvasNodePosition(node, x, y)")
                .contains("const maxX = Math.max(CANVAS_NODE_PADDING, rect.width - canvasWidthFootprintForNode(node) - CANVAS_NODE_PADDING);")
                .contains("return clampCanvasNodePosition(node, x, y);")
                .contains("const clampedPoint = clampCanvasNodePosition(nodeTemplate, point.x, point.y);")
                .contains("const clampedPosition = clampCanvasNodePosition(");
    }

    @Test
    void scriptRecomputesEdgesAfterInitialLayoutStabilizes() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("let edgeRefreshFrameId = null;")
                .contains("function scheduleEdgeRefresh()")
                .contains("window.cancelAnimationFrame(edgeRefreshFrameId);")
                .contains("edgeRefreshFrameId = window.requestAnimationFrame(() => {")
                .contains("renderEdges();")
                .contains("scheduleEdgeRefresh();")
                .contains("document.fonts?.ready?.then(() => {")
                .contains("window.addEventListener(\"resize\", scheduleEdgeRefresh);");
    }

    @Test
    void scriptClampsLoadedNodePositionsWithoutDirtyingOnOpen() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("const normalizedNode = {")
                .contains("const clampedPosition = clampCanvasNodePosition(")
                .contains("normalizedNode.ui = clampedPosition;")
                .contains("definition.nodes.map(normalizeNode)")
                .contains("clearDirty();")
                .doesNotContain("markDirty();\n    const nodes = Array.isArray(definition?.nodes) ? definition.nodes.map(node => normalizeNode(node)) : [];");
    }

    @Test
    void scriptUsesReadableWorkbenchStatusAndFeedbackTones() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("text-emerald-700")
                .contains("dark:text-emerald-200")
                .contains("text-blue-700")
                .contains("dark:text-blue-200")
                .contains("text-red-700")
                .contains("dark:text-red-200")
                .contains("bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border border-slate-300 dark:border-slate-600")
                .doesNotContain("text-emerald-100")
                .doesNotContain("text-blue-100")
                .doesNotContain("text-red-100")
                .doesNotContain("bg-slate-700/60 text-slate-300 border border-slate-600");
    }

    @Test
    void scriptUsesThemeAwareNodeComponentsAndReadableMetrics() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("draggable-node studio-node-card")
                .contains("drag-handle studio-node-header")
                .contains("studio-node-title flex-1 truncate ml-2 min-w-0")
                .contains("studio-node-type mt-2")
                .contains("bg-slate-50 dark:bg-neutral-950/70 p-2")
                .contains("text-xs font-semibold text-slate-900 dark:text-neutral-100 mt-1")
                .doesNotContain("bg-white/96 dark:bg-slate-800/95 border border-slate-300 dark:border-slate-600 rounded-lg shadow-xl")
                .doesNotContain("bg-white dark:bg-slate-800/95 border border-slate-300 dark:border-slate-600 rounded-lg shadow-xl")
                .doesNotContain("text-white flex-1 truncate");

        assertThat(loadTemplate())
                .containsPattern("(?s)\\.studio-node-card \\{[^}]*background: var\\(--sc-card-strong\\);")
                .containsPattern("(?s)\\.studio-node-header \\{[^}]*background: var\\(--sc-surface\\);")
                .containsPattern("(?s)\\.studio-node-type \\{[^}]*color: var\\(--sc-muted\\);");
    }

    private String loadTemplate() throws IOException {
        ClassPathResource resource = new ClassPathResource("templates/studio.html");
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    private String loadScript() throws IOException {
        ClassPathResource resource = new ClassPathResource("static/js/studio-editor.js");
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
