package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.ui.ExtendedModelMap;
import org.springframework.ui.Model;

class PipelineListTemplateContractTest {

    @Test
    void pipelineListUsesSaveThenRunCopyWithoutVersionColumn() throws Exception {
        String template = loadTemplate("templates/pipeline-list.html");

        assertThat(template)
                .contains("item.id")
                .doesNotContain("item.version")
                .doesNotContain("latestVersion")
                .doesNotContain("publishVersion");
    }

    @Test
    void pipelineListDoesNotRenderPerPipelineClusterBinding() throws Exception {
        String template = loadTemplate("templates/pipeline-list.html");

        assertThat(template)
                .doesNotContain("clusterConnectionId")
                .doesNotContain("pipelines.table.cluster")
                .doesNotContain("pipelines.cluster.bound")
                .doesNotContain("pipelines.cluster.unbound");
    }

    @Test
    void studioTemplateDoesNotMentionPublishingOrLatestVersion() throws Exception {
        String template = loadTemplate("templates/studio.html");

        assertThat(template)
                .contains("id=\"save-pipeline-button\"")
                .contains("id=\"run-pipeline-button\"")
                .doesNotContain("latestVersion")
                .doesNotContain("publishVersion")
                .doesNotContain("publish-pipeline-button");
    }

    @Test
    void pipelinesPageSubtitleUsesSaveThenRunLanguage() {
        PageController controller = new PageController();
        Model model = new ExtendedModelMap();

        String viewName = controller.pipelines(
                new UsernamePasswordAuthenticationToken("admin", "password"),
                model);

        assertThat(viewName).isEqualTo("pipeline-list");
        assertThat(model.getAttribute("pageSubtitle"))
                .isEqualTo("创建、保存并运行实时任务。");
    }

    @Test
    void pipelineListBindsStatusTonesToSharedThemeTokens() throws Exception {
        String template = loadTemplate("templates/pipeline-list.html");

        assertThat(template)
                .contains(".pl-chip[data-tone=\"running\"] { color: var(--sc-success);")
                .contains(".pl-chip[data-tone=\"stopped\"] { color: var(--sc-warning);")
                .contains(".pl-chip[data-tone=\"failed\"] { color: var(--sc-danger);")
                .contains(".pl-chip[data-tone=\"unknown\"] { color: var(--sc-muted-soft);")
                .contains("class=\"pl-chip\" data-tone=\"${tone}\"")
                .doesNotContain("text-emerald-100")
                .doesNotContain("text-blue-100")
                .doesNotContain("text-blue-700")
                .doesNotContain("text-red-100")
                .doesNotContain("font-semibold text-slate-100 text-center");
    }

    @Test
    void pipelineListKeepsCountersFiltersAndTableControls() throws Exception {
        String template = loadTemplate("templates/pipeline-list.html");

        assertThat(template)
                .contains("id=\"summary-total\"")
                .contains("id=\"summary-running\"")
                .contains("id=\"summary-stopped\"")
                .contains("id=\"pipeline-search\"")
                .contains("id=\"pipeline-status-filter\"")
                .contains("<table class=\"pl-table\" aria-labelledby=\"pipeline-table-title\">")
                .doesNotContain("dark:bg-slate-900")
                .doesNotContain("dark:bg-slate-950");
    }

    @Test
    void pipelineListUsesLocalizedHeaderAndAccessibleCollectionPanel() throws Exception {
        String template = loadTemplate("templates/pipeline-list.html");

        assertThat(template)
                .contains("data-page-hero=\"pipelines\"")
                .contains("th:text=\"#{pipelines.hero.title}\"")
                .contains("data-create-pipeline-link th:href=\"@{/studio}\"")
                .contains("data-collection-panel=\"pipeline-list\"")
                .contains("class=\"pl-empty\" role=\"status\"")
                .contains("aria-labelledby=\"pipeline-table-title\"");
    }

    @Test
    void pipelineListOmitsLegacyOperationalHelperCopy() throws Exception {
        String template = loadTemplate("templates/pipeline-list.html");

        assertThat(template)
                .doesNotContain("Create, filter, run, and clean up saved pipelines without leaving the operational workspace.")
                .doesNotContain("鎵€鏈夊凡淇濆瓨鐨勪换鍔￠兘浼氭樉绀哄湪杩欓噷銆?")
                .doesNotContain("鐐瑰嚮杩愯浼氱洿鎺ユ彁浜ゅ綋鍓嶄繚瀛樺唴瀹广€?")
                .doesNotContain("鍋滄鍚庢墠鍏佽鍒犻櫎浠诲姟銆?")
                .doesNotContain("浠诲姟鏁版嵁鏉ヨ嚜鍚庣 `/api/pipelines`锛岃繍琛屼腑鐨勪换鍔￠渶瑕佸厛鍋滄鍚庢墠鑳藉垹闄ゃ€?");
    }

    @Test
    void pipelineListOmitsSplitActionHintCopy() throws Exception {
        String template = loadTemplate("templates/pipeline-list.html");

        assertThat(template)
                .doesNotContain("操作已拆分为：运行 / 停止 / 删除");
    }

    @Test
    void pipelineListSurfacesAndRowsUseSharedThemeTokens() throws Exception {
        String template = loadTemplate("templates/pipeline-list.html");

        assertThat(template)
                .contains("background: color-mix(in srgb, var(--sc-card) 92%, transparent);")
                .contains(".pl-table tbody tr:hover {")
                .contains("color: var(--sc-text-body);")
                .contains("border-bottom: 1px solid var(--sc-border-soft);")
                .doesNotContain("dark:bg-[#0b1120]")
                .doesNotContain("dark:bg-slate-900/80")
                .doesNotContain("dark:hover:bg-slate-700 dark:bg-slate-800/30")
                .doesNotContain("divide-y divide-slate-700/50 text-sm");
    }

    @Test
    void pipelineListLocalizesHeadingsAndUsesThemeTextTokens() throws Exception {
        String template = loadTemplate("templates/pipeline-list.html");

        assertThat(template)
                .contains("th:text=\"#{pipelines.hero.label}\"")
                .contains("th:text=\"#{pipelines.hero.title}\"")
                .contains("th:text=\"#{pipelines.list.title}\"")
                .contains("color: var(--sc-text);")
                .contains("color: var(--sc-muted);")
                .doesNotContain("tracking-[0.25em]")
                .doesNotContain("tracking-[0.2em]")
                .doesNotContain("rounded-2xl")
                .doesNotContain("bg-blue-600")
                .doesNotContain("bg-blue-50")
                .doesNotContain("text-cyan-600")
                .doesNotContain("text-indigo-600");
    }

    private String loadTemplate(String path) throws IOException {
        ClassPathResource resource = new ClassPathResource(path);
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
