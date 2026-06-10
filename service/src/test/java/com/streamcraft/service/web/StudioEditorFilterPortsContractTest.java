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
                .contains("const FILTER_OUTPUT_PORT_IDS = [\"true\", \"false\"];")
                .contains("function outputPortsForNode(node)")
                .contains("return FILTER_OUTPUT_PORT_IDS.map(portId => ({ id: portId }));")
                .contains("function inputPortsForNode(node)")
                .contains("function sourcePortIdForEdge(edge, sourceNode)")
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
                .contains("edge.sourcePortId === (state.connectState.sourcePortId || DEFAULT_SOURCE_PORT_ID)")
                .contains("sourcePortId: state.connectState.sourcePortId || DEFAULT_SOURCE_PORT_ID");
    }

    @Test
    void scriptDoesNotSilentlyDefaultLegacyFilterSourcePortsToOutputZero() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("function sourcePortIdForEdge(edge, sourceNode)")
                .contains("if (sourceNode?.operator === \"FILTER\")")
                .contains("return edge.sourcePortId;")
                .contains("sourcePortId: sourcePortIdForEdge(edge, findNodeById(edge.sourceNodeId))")
                .contains("targetPortId: edge.targetPortId")
                .contains("state.edges = Array.isArray(definition?.edges) ? definition.edges.map(edge => normalizeEdge(edge, nodes)) : [];")
                .doesNotContain("sourcePortId: edge.sourcePortId || DEFAULT_SOURCE_PORT_ID")
                .doesNotContain("targetPortId: edge.targetPortId || DEFAULT_TARGET_PORT_ID")
                .doesNotContain("const resolvedPortId = portId || (direction === \"input\" ? DEFAULT_TARGET_PORT_ID : DEFAULT_SOURCE_PORT_ID);");
    }

    @Test
    void scriptDefinesStackedDataQualityCleanAndDirtyOutputPorts() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("const DATA_QUALITY_OUTPUT_PORT_IDS = [DEFAULT_SOURCE_PORT_ID, \"dirty\"];")
                .contains("return DATA_QUALITY_OUTPUT_PORT_IDS.map(portId => ({ id: portId }));")
                .contains("node.operator === \"FILTER\" || node.operator === \"ROUTE\" || node.operator === \"DATA_QUALITY\"");
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
    void scriptUsesLightNodeCardsInLightModeAndDarkNodeCardsInDarkMode() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("bg-white dark:bg-neutral-900/95 border border-slate-300 dark:border-neutral-700 rounded-lg shadow-xl")
                .contains("bg-slate-50 dark:bg-neutral-900 border-b border-slate-200 dark:border-neutral-700 rounded-t-lg")
                .contains("text-slate-900 dark:text-neutral-100 flex-1 truncate")
                .contains("text-slate-500 dark:text-neutral-400 mt-2")
                .contains("bg-slate-50 dark:bg-neutral-950/70 p-2")
                .contains("text-xs font-semibold text-slate-900 dark:text-neutral-100 mt-1")
                .doesNotContain("bg-white/96 dark:bg-slate-800/95 border border-slate-300 dark:border-slate-600 rounded-lg shadow-xl")
                .doesNotContain("bg-white dark:bg-slate-800/95 border border-slate-300 dark:border-slate-600 rounded-lg shadow-xl")
                .doesNotContain("text-white flex-1 truncate");
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
