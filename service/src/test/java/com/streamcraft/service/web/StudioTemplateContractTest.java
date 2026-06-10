package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class StudioTemplateContractTest {

    @Test
    void templateUsesI18nHeadAndWorkbenchShell() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("commonHead(#{studio.documentTitle})")
                .contains("data-studio-shell=\"editor\"")
                .contains("data-studio-panel=\"palette\"")
                .contains("data-studio-panel=\"inspector\"")
                .contains("data-page-mode=\"editor\"")
                .contains("id=\"operator-palette\"")
                .contains("id=\"source-config-panel\"")
                .doesNotContain("definition-preview")
                .doesNotContain("DSL preview");
    }

    @Test
    void templateUsesSharedWorkbenchSurfacesAndReadableInputs() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("class=\"sc-shell-page")
                .contains("sc-card-strong")
                .contains("sc-btn-primary")
                .contains("sc-btn-secondary")
                .contains("sc-input")
                .contains("sc-form-label")
                .contains("class=\"sc-input\"")
                .contains("class=\"sc-input resize-none\"")
                .doesNotContain("class=\"w-full bg-slate-950 border border-slate-300 dark:border-slate-700 rounded-xl px-4 py-3 text-sm text-slate-900 dark:text-slate-100")
                .doesNotContain("bg-white dark:bg-slate-950 border border-slate-300 dark:border-slate-700 rounded-xl px-4 py-3 text-sm text-slate-900 dark:text-slate-100")
                .doesNotContain("rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-xs text-emerald-200")
                .doesNotContain("rounded-xl px-4 py-3 text-sm");
    }

    @Test
    void templateUsesStructuredWorkbenchRailsAndCanvasFrame() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-studio-header=\"workbench\"")
                .contains("data-studio-rail=\"palette\"")
                .contains("data-studio-canvas-frame")
                .contains("data-studio-stage")
                .contains("data-studio-rail=\"inspector\"")
                .contains("studio-shell-grid")
                .contains("studio-operator-group")
                .contains("studio-inspector-section")
                .doesNotContain("dark:bg-[#0b1120]");
    }

    @Test
    void templateUsesBalancedStudioSurfaceGrammar() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("studio-workbench-header")
                .contains("studio-rail-header")
                .contains("studio-canvas-frame")
                .contains("studio-stage")
                .contains("studio-inspector-note")
                .contains("background: var(--sc-surface);")
                .contains("border-radius: 12px;")
                .contains("box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);")
                .doesNotContain("backdrop-filter")
                .doesNotContain("background: rgba(19, 19, 19, 0.92);")
                .doesNotContain("background: rgba(21, 21, 21, 0.92);")
                .doesNotContain("background: rgba(29, 29, 29, 0.94);")
                .doesNotContain("background: #020617;")
                .doesNotContain("background: rgba(15, 23, 42, 0.88);")
                .doesNotContain("background: rgba(15, 23, 42, 0.92);")
                .doesNotContain("rounded-none border-x-0 border-t-0 h-16")
                .doesNotContain("sc-status-note flex items-center gap-2 p-3 rounded-xl");
    }

    @Test
    void templateKeepsCalInspiredStudioTreatmentsRestrained() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains(".studio-canvas-frame")
                .contains("border-radius: 16px;")
                .contains("background: var(--sc-card-strong);")
                .contains("background: var(--sc-bg);")
                .contains("color: var(--sc-text);")
                .doesNotContain("border-radius: 20px;")
                .doesNotContain("transform: translateY(-1px)")
                .doesNotContain("0 18px 40px")
                .doesNotContain("0 20px 40px")
                .doesNotContain("0 22px 44px")
                .doesNotContain("text-slate-900 dark:text-neutral-100")
                .doesNotContain("text-slate-500 dark:text-slate-400");
    }

    @Test
    void templateUsesLightCanvasInLightModeAndDarkCanvasInDarkMode() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .containsPattern("\\.canvas-bg \\{\\s*background: var\\(--sc-surface\\);")
                .containsPattern("\\.dark \\.canvas-bg \\{\\s*background: var\\(--sc-surface\\);")
                .doesNotContain("background: #0b1120;")
                .doesNotContain("background: linear-gradient(180deg, #020617 0%, #0f172a 100%);");
    }

    @Test
    void templateUsesNeutralBootstrapServerExamplesForSourceAndSink() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("placeholder=\"10.0.0.1:9092,10.0.0.2:9092,10.0.0.3:9092\"")
                .doesNotContain("placeholder=\"192.168.217.132:9092,192.168.217.134:9092\"");
    }

    @Test
    void templateAddsPreviewActionAndPreviewResultsSection() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"preview-pipeline-button\"")
                .contains("th:text=\"#{studio.action.preview}\"")
                .contains("id=\"pipeline-preview-results-panel\"")
                .contains("id=\"pipeline-preview-results-list\"")
                .contains("id=\"preview-output-item-template\"")
                .contains("th:aria-label=\"#{studio.preview.output.message}\"")
                .containsPattern("(?s)<template id=\"preview-output-item-template\">.*?data-role=\"preview-output-record\".*?</template>")
                .doesNotContain("data-role=\"remove-preview-output\"");
    }

    @Test
    void templateUsesI18nKeysForHeaderPaletteAndActions() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("th:text=\"#{studio.title.new}\"")
                .contains("th:text=\"#{studio.status.new}\"")
                .contains("th:text=\"#{studio.subtitle}\"")
                .contains("th:text=\"#{studio.action.save}\"")
                .contains("th:text=\"#{studio.action.run}\"")
                .contains("th:text=\"#{studio.action.stop}\"")
                .contains("th:text=\"#{studio.palette.title}\"")
                .contains("th:text=\"#{studio.palette.input}\"")
                .contains("th:text=\"#{studio.palette.transform}\"")
                .contains("th:text=\"#{studio.palette.output}\"")
                .contains("data-operator-key=\"put\" th:text=\"#{studio.operator.put}\"")
                .contains("data-operator-key=\"prune\" th:text=\"#{studio.operator.prune}\"")
                .contains("data-operator-key=\"rename\" th:text=\"#{studio.operator.rename}\"")
                .contains("data-operator-key=\"deserialize\" th:text=\"#{studio.operator.deserialize}\"")
                .contains("data-operator-key=\"serialize\" th:text=\"#{studio.operator.serialize}\"")
                .contains("data-operator-key=\"filter\" th:text=\"#{studio.operator.filter}\"")
                .contains("data-operator-key=\"grok\" th:text=\"#{studio.operator.grok}\"")
                .contains("data-operator-key=\"cast\" th:text=\"#{studio.operator.cast}\"")
                .contains("data-operator-key=\"eval\" th:text=\"#{studio.operator.eval}\"")
                .contains("data-operator-key=\"deduplicate\" th:text=\"#{studio.operator.deduplicate}\"")
                .contains("data-operator-key=\"dataQuality\" th:text=\"#{studio.operator.dataQuality}\"")
                .contains("data-operator-key=\"customCode\" th:text=\"#{studio.operator.customCode}\"");
    }

    @Test
    void templateProvidesRuntimeTargetDrivenRunResourceControls() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"runtime-target-summary\"")
                .contains("id=\"pipeline-runtime-resources\"")
                .contains("data-role=\"runtime-resources\"")
                .contains("id=\"run-parallelism\"")
                .contains("th:text=\"#{studio.runtime.resources.title}\"")
                .contains("th:text=\"#{studio.runtime.parallelism}\"")
                .doesNotContain("id=\"pipeline-yarn-runtime-resources\"")
                .doesNotContain("id=\"run-yarn-jobmanager-memory\"")
                .doesNotContain("id=\"run-yarn-taskmanager-memory\"")
                .doesNotContain("id=\"run-yarn-taskmanager-slots\"")
                .doesNotContain("studio.runtime.yarn")
                .doesNotContain("id=\"cluster-connection-id\"")
                .doesNotContain("#{studio.pipeline.cluster}");

        assertThat(template.indexOf("id=\"pipeline-runtime-resources\""))
                .isGreaterThan(template.indexOf("id=\"pipeline-description\""));
    }

    @Test
    void templateProvidesAggregatePaletteItem() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-operator-key=\"aggregate\"")
                .contains("#{studio.operator.aggregate}");
    }

    @Test
    void templateUsesSegmentedControlForJdbcSourceReadMode() throws Exception {
        String template = loadTemplate();

        assertSegmentedControl(template, "jdbc-read-mode");
    }

    @Test
    void templateUsesSegmentedControlForElasticsearchSourceReadMode() throws Exception {
        String template = loadTemplate();

        assertSegmentedControl(template, "elasticsearch-read-mode");
    }

    @Test
    void templateUsesSegmentedControlForInfluxDbSourceReadMode() throws Exception {
        String template = loadTemplate();

        assertSegmentedControl(template, "influxdb-read-mode");
    }

    @Test
    void templateUsesCalControlsForHdfsFileSourceModesAndCheckboxes() throws Exception {
        String template = loadTemplate();

        assertSegmentedControl(template, "hdfs-file-read-mode");
        assertThat(template)
                .contains("id=\"hdfs-file-parse-partition-from-path\" type=\"checkbox\" class=\"w-4 h-4 rounded border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900 text-[var(--sc-accent)] focus:ring-blue-500/50 cursor-pointer\"")
                .contains("id=\"hdfs-file-csv-use-header-line\" type=\"checkbox\" class=\"w-4 h-4 rounded border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900 text-[var(--sc-accent)] focus:ring-blue-500/50 cursor-pointer\"")
                .doesNotContain("id=\"hdfs-file-parse-partition-from-path\" type=\"checkbox\" class=\"h-4 w-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500\"")
                .doesNotContain("id=\"hdfs-file-csv-use-header-line\" type=\"checkbox\" class=\"h-4 w-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500\"");
    }

    @Test
    void templateUsesCalIconButtonsForSourceSampleControls() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .containsPattern("(?s)<button id=\"add-source-sample-button\".*?class=\"sc-btn-icon\"")
                .containsPattern("(?s)<button type=\"button\"\\s+data-role=\"remove-source-sample\".*?class=\"absolute right-2\\.5 top-2\\.5 z-10 sc-btn-icon\"")
                .doesNotContain("id=\"add-source-sample-button\"\n                                            type=\"button\"\n                                            th:aria-label=\"#{studio.action.addSample}\"\n                                            th:title=\"#{studio.action.addSample}\"\n                                            aria-label=\"Add sample\"\n                                            title=\"Add sample\"\n                                            class=\"inline-flex h-3 w-3 items-center justify-center rounded-full")
                .doesNotContain("data-role=\"remove-source-sample\"\n                                                th:aria-label=\"#{studio.action.removeSample}\"\n                                                th:title=\"#{studio.action.removeSample}\"\n                                                aria-label=\"Remove sample\"\n                                                title=\"Remove sample\"\n                                                class=\"absolute right-2.5 top-2.5 z-10 h-3 w-3 rounded-full");
    }

    @Test
    void templateUsesCalInspiredAggregateConfigBlocks() throws Exception {
        String template = loadTemplate();
        String aggregateSection = template.substring(
                template.indexOf("id=\"aggregate-config\""),
                template.indexOf("id=\"custom-code-config\""));

        assertThat(aggregateSection)
                .contains("id=\"aggregate-config\"")
                .contains("id=\"aggregate-event-time-config\"")
                .contains("id=\"aggregate-event-time-field\"")
                .contains("id=\"aggregate-event-time-unit\"")
                .contains("id=\"aggregate-output-mode\"")
                .contains("id=\"aggregate-flat-output-config\"")
                .contains("id=\"aggregate-window-start-field\"")
                .contains("id=\"aggregate-window-end-field\"")
                .contains("studio-repeatable-card")
                .contains("studio-repeatable-fields")
                .contains("data-role=\"aggregate-field-wrapper\"")
                .contains("data-role=\"aggregate-sort-field-wrapper\"")
                .contains("data-role=\"aggregate-sort-direction-wrapper\"")
                .contains("aggregate-limit-wrapper")
                .contains("th:text=\"#{studio.select.aggregateFunction.count}\"")
                .contains("th:text=\"#{studio.select.aggregateFunction.countDistinct}\"")
                .contains("th:text=\"#{studio.select.aggregateFunction.collectSet}\"")
                .contains("th:text=\"#{studio.field.timeUnit}\"")
                .contains("<option value=\"MILLISECONDS\" th:text=\"#{studio.select.timeUnit.milliseconds}\">Milliseconds</option>")
                .contains("<option value=\"SECONDS\" th:text=\"#{studio.select.timeUnit.seconds}\">Seconds</option>")
                .contains("<option value=\"MINUTES\" th:text=\"#{studio.select.timeUnit.minutes}\">Minutes</option>")
                .contains("<option value=\"HOURS\" th:text=\"#{studio.select.timeUnit.hours}\">Hours</option>")
                .contains("th:text=\"#{studio.field.eventTimeField}\"")
                .contains("th:text=\"#{studio.field.eventTimeUnit}\"")
                .contains("th:text=\"#{studio.field.aggregateSortField}\"")
                .contains("th:text=\"#{studio.field.aggregateSortDirection}\"")
                .contains("th:text=\"#{studio.field.outputMode}\"")
                .contains("th:text=\"#{studio.field.windowStartField}\"")
                .contains("th:text=\"#{studio.field.windowEndField}\"")
                .contains("class=\"sc-btn-icon aggregate-item-action\"")
                .doesNotContain("aggregate-item-card")
                .doesNotContain("aggregate-item-fields")
                .doesNotContain("grid-cols-[minmax(0,0.9fr)_minmax(0,1fr)_minmax(0,1fr)_minmax(0,0.7fr)_auto]")
                .doesNotContain("inline-flex h-3 w-3 items-center justify-center rounded-full")
                .doesNotContain("class=\"h-3 w-3 rounded-full");
    }

    @Test
    void templateUsesSharedCalRepeatableCardsForOperatorConfigItems() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains(".studio-repeatable-card")
                .contains(".studio-repeatable-fields")
                .contains("data-role=\"lookup-join-entry-item\" class=\"studio-repeatable-card\"")
                .contains("data-role=\"data-quality-rule-item\" class=\"studio-repeatable-card\"")
                .contains("data-role=\"time-derive-derivation-item\" class=\"studio-repeatable-card\"")
                .contains("data-role=\"mask-hash-rule-item\" class=\"studio-repeatable-card\"")
                .contains("data-role=\"case-when-rule-item\" class=\"studio-repeatable-card\"")
                .contains("data-role=\"route-rule-item\" class=\"studio-repeatable-card\"")
                .contains("data-role=\"aggregate-item\" class=\"studio-repeatable-card\"")
                .contains("data-role=\"mask-hash-mask-fields\" class=\"studio-repeatable-fields\"")
                .contains("data-role=\"mask-hash-hash-fields\" class=\"studio-repeatable-fields hidden\"")
                .doesNotContain("rounded-lg border border-slate-200 bg-white/70")
                .doesNotContain("rounded-lg border border-slate-200 dark:border-slate-800 p-3")
                .doesNotContain("dark:bg-slate-950/40")
                .doesNotContain("aggregate-item-card")
                .doesNotContain("aggregate-item-fields");
    }

    @Test
    void templateProvidesDeduplicatePaletteItemAndInspectorControls() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-operator-key=\"deduplicate\"")
                .contains("#{studio.operator.deduplicate}")
                .contains("id=\"deduplicate-config\"")
                .contains("id=\"deduplicate-key-fields\"")
                .contains("id=\"deduplicate-time-mode\"")
                .contains("data-target-select=\"deduplicate-time-mode\"")
                .contains("id=\"deduplicate-processing-time-section\"")
                .contains("id=\"deduplicate-event-time-section\"")
                .contains("id=\"deduplicate-event-time-section\" class=\"space-y-3 hidden\">\n                                    <div>\n                                        <label for=\"deduplicate-event-time-field\"")
                .contains("<div class=\"grid grid-cols-1 xl:grid-cols-2 gap-3\">\n                                        <div>\n                                            <label for=\"deduplicate-window-seconds\"")
                .contains("id=\"deduplicate-ttl-seconds\"")
                .contains("id=\"deduplicate-event-time-field\"")
                .contains("id=\"deduplicate-window-seconds\"")
                .contains("id=\"deduplicate-watermark-delay-seconds\"")
                .contains("id=\"deduplicate-keep-strategy\"")
                .contains("id=\"deduplicate-late-data-strategy\"")
                .contains("id=\"deduplicate-duplicate-strategy\"")
                .contains("th:text=\"#{studio.field.timeMode}\"")
                .contains("th:text=\"#{studio.select.time.processing}\"")
                .contains("th:text=\"#{studio.select.time.event}\"")
                .contains("th:text=\"#{studio.field.eventTimeField}\"")
                .contains("th:text=\"#{studio.field.windowSeconds}\"")
                .contains("th:text=\"#{studio.field.watermarkDelaySeconds}\"")
                .contains("th:text=\"#{studio.field.lateDataStrategy}\"")
                .contains("th:text=\"#{studio.select.deduplicate.first}\"")
                .contains("th:text=\"#{studio.select.deduplicate.last}\"")
                .contains("th:text=\"#{studio.select.deduplicate.eventTimeLatest}\"")
                .contains("th:text=\"#{studio.select.deduplicate.discard}\"")
                .contains("th:text=\"#{studio.select.lateData.drop}\"")
                .contains("data-operator-key=\"lookupEnrich\"")
                .contains("#{studio.operator.lookupEnrich}")
                .contains("id=\"lookup-enrich-config\"")
                .contains("id=\"lookup-enrich-source-field\"")
                .contains("id=\"lookup-enrich-target-field\"")
                .contains("id=\"lookup-enrich-missing-strategy\"")
                .contains("id=\"lookup-enrich-overwrite-target-field\"")
                .contains("id=\"lookup-enrich-entry-list\"");
    }

    @Test
    void templateUsesCalControlsForLookupEnrichConfig() throws Exception {
        String template = loadTemplate();

        assertSegmentedControl(template, "lookup-enrich-missing-strategy");
        assertThat(template)
                .contains("data-segmented-value=\"DISCARD\"")
                .contains("data-segmented-value=\"FAIL\"")
                .contains("th:text=\"#{studio.select.lookupEnrich.discard}\"")
                .contains("th:text=\"#{studio.select.lookupEnrich.fail}\"")
                .contains("data-role=\"lookup-enrich-entry-value-type\"")
                .contains("th:text=\"#{studio.select.valueType.string}\"")
                .contains("th:text=\"#{studio.select.valueType.number}\"")
                .contains("th:text=\"#{studio.select.valueType.boolean}\"")
                .contains("th:text=\"#{studio.select.valueType.json}\"")
                .contains("data-role=\"lookup-enrich-entry-item\" class=\"grid grid-cols-1 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1fr)_8rem_auto]")
                .containsPattern("(?s)<button id=\"add-lookup-enrich-entry-button\".*?class=\"sc-btn-icon\"")
                .containsPattern("(?s)<button type=\"button\"\\s+data-role=\"remove-lookup-enrich-entry\".*?class=\"sc-btn-icon\"")
                .doesNotContain("id=\"add-lookup-enrich-entry-button\"\n                                                type=\"button\"\n                                                th:aria-label=\"#{studio.action.addMapping}\"\n                                                th:title=\"#{studio.action.addMapping}\"\n                                                aria-label=\"Add mapping\"\n                                                title=\"Add mapping\"\n                                                class=\"inline-flex h-3 w-3 items-center justify-center rounded-full")
                .doesNotContain("data-role=\"remove-lookup-enrich-entry\"\n                                                    th:aria-label=\"#{studio.action.removeMapping}\"\n                                                    th:title=\"#{studio.action.removeMapping}\"\n                                                    aria-label=\"Remove mapping\"\n                                                    title=\"Remove mapping\"\n                                                    class=\"h-3 w-3 rounded-full");
    }

    @Test
    void templateProvidesFlattenAndExplodePaletteItemsAndInspectorControls() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-operator-key=\"flatten\"")
                .contains("#{studio.operator.flatten}")
                .contains("id=\"flatten-config\"")
                .contains("id=\"flatten-source-field\"")
                .contains("id=\"flatten-target-prefix\"")
                .contains("id=\"flatten-delimiter\"")
                .contains("id=\"flatten-remove-source-field\"")
                .contains("data-operator-key=\"explode\"")
                .contains("#{studio.operator.explode}")
                .contains("id=\"explode-config\"")
                .contains("id=\"explode-source-field\"")
                .contains("id=\"explode-target-field\"")
                .contains("id=\"explode-keep-empty\"");
    }

    @Test
    void templateProvidesDataQualityPaletteItemAndInspectorControls() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-operator-key=\"dataQuality\"")
                .contains("#{studio.operator.dataQuality}")
                .contains("id=\"data-quality-config\"")
                .contains("id=\"data-quality-mode\"")
                .contains("id=\"data-quality-error-field\"")
                .contains("id=\"data-quality-rule-list\"")
                .contains("id=\"data-quality-rule-template\"")
                .contains("id=\"add-data-quality-rule-button\"")
                .contains("data-role=\"data-quality-rule-field\"")
                .contains("data-role=\"data-quality-rule-kind\"")
                .contains("data-role=\"data-quality-value-type-wrapper\"")
                .contains("data-role=\"data-quality-rule-value-type\"")
                .contains("data-role=\"data-quality-range-wrapper\"")
                .contains("data-role=\"data-quality-rule-min\"")
                .contains("data-role=\"data-quality-rule-max\"")
                .contains("data-role=\"data-quality-length-wrapper\"")
                .contains("data-role=\"data-quality-rule-min-length\"")
                .contains("data-role=\"data-quality-rule-max-length\"")
                .contains("data-role=\"data-quality-enum-wrapper\"")
                .contains("data-role=\"data-quality-rule-enum-values\"")
                .contains("data-role=\"data-quality-regex-wrapper\"")
                .contains("data-role=\"data-quality-rule-pattern\"")
                .contains("data-role=\"data-quality-rule-message\"")
                .contains("data-role=\"remove-data-quality-rule\"");
    }

    @Test
    void templateUsesCalControlsForDataQualityConfig() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("<select id=\"data-quality-mode\" class=\"sc-input\">")
                .contains("<option value=\"FAIL\" th:text=\"#{studio.select.dataQuality.fail}\">Fail and stop</option>")
                .contains("class=\"grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_minmax(0,0.9fr)_auto] items-start gap-3\"")
                .contains("data-role=\"data-quality-range-wrapper\" class=\"hidden grid grid-cols-1 xl:grid-cols-2 gap-3\"")
                .contains("data-role=\"data-quality-length-wrapper\" class=\"hidden grid grid-cols-1 xl:grid-cols-2 gap-3\"")
                .containsPattern("(?s)<button id=\"add-data-quality-rule-button\".*?class=\"sc-btn-icon\"")
                .containsPattern("(?s)<button type=\"button\"\\s+data-role=\"remove-data-quality-rule\".*?class=\"sc-btn-icon\"")
                .doesNotContain(".studio-segmented-control[data-segment-count=\"4\"]")
                .doesNotContain("data-segmented-control data-target-select=\"data-quality-mode\" data-segment-count=\"4\"")
                .doesNotContain("id=\"add-data-quality-rule-button\"\n                                                type=\"button\"\n                                                th:aria-label=\"#{studio.action.addRule}\"\n                                                th:title=\"#{studio.action.addRule}\"\n                                                aria-label=\"Add rule\"\n                                                title=\"Add rule\"\n                                                class=\"inline-flex h-3 w-3 items-center justify-center rounded-full")
                .doesNotContain("data-role=\"remove-data-quality-rule\"\n                                                        th:aria-label=\"#{studio.action.removeRule}\"\n                                                        th:title=\"#{studio.action.removeRule}\"\n                                                        aria-label=\"Remove rule\"\n                                                        title=\"Remove rule\"\n                                                        class=\"h-3 w-3 rounded-full");
    }

    @Test
    void templateUsesCalLabelsForTimeDeriveConfig() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("<label for=\"time-derive-source-field\" class=\"sc-form-label\" th:text=\"#{studio.field.timeDeriveSourceField}\">Source time field</label>")
                .contains("<label for=\"time-derive-source-format\" class=\"sc-form-label\" th:text=\"#{studio.field.timeDeriveSourceFormat}\">Source time format</label>")
                .contains("id=\"time-derive-source-pattern-wrapper\"")
                .contains("id=\"time-derive-source-pattern\"")
                .contains("id=\"time-derive-source-time-zone\"")
                .contains("id=\"time-derive-output-time-zone\"")
                .contains("id=\"time-derive-parse-error-strategy\"")
                .contains("id=\"time-derive-derivation-list\"")
                .contains("id=\"time-derive-derivation-template\"")
                .contains("data-role=\"time-derive-output-field\"")
                .contains("data-role=\"time-derive-type\"")
                .contains("data-role=\"time-derive-pattern-wrapper\"")
                .contains("data-role=\"time-derive-pattern\"")
                .contains("data-role=\"move-time-derive-up\"")
                .contains("data-role=\"move-time-derive-up\" class=\"sc-btn-icon\" th:aria-label=\"#{studio.action.moveTimeDeriveUp}\" th:title=\"#{studio.action.moveTimeDeriveUp}\"")
                .contains("data-role=\"move-time-derive-down\"")
                .contains("data-role=\"move-time-derive-down\" class=\"sc-btn-icon\" th:aria-label=\"#{studio.action.moveTimeDeriveDown}\" th:title=\"#{studio.action.moveTimeDeriveDown}\"")
                .contains("data-role=\"remove-time-derive\"")
                .contains("data-role=\"remove-time-derive\" class=\"sc-btn-icon\" th:aria-label=\"#{studio.action.removeTimeDerive}\" th:title=\"#{studio.action.removeTimeDerive}\"")
                .contains("th:text=\"#{studio.field.timeDeriveSourcePattern}\"")
                .contains("th:text=\"#{studio.field.timeDeriveSourceTimeZone}\"")
                .contains("th:text=\"#{studio.field.timeDeriveOutputTimeZone}\"")
                .contains("th:text=\"#{studio.field.timeDeriveParseErrorStrategy}\"")
                .contains("th:text=\"#{studio.field.timeDeriveOutputField}\"")
                .contains("th:text=\"#{studio.field.timeDeriveType}\"")
                .contains("th:text=\"#{studio.action.addTimeDerive}\"")
                .contains("th:text=\"#{studio.select.timeDerive.year}\"")
                .contains("th:text=\"#{studio.select.parseError.fail}\"")
                .doesNotContain("id=\"time-derive-derivations\"");
    }

    @Test
    void templateUsesCalLabelForMaskHashConfig() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"mask-hash-rule-list\"")
                .contains("id=\"mask-hash-rule-template\"")
                .contains("data-role=\"mask-hash-source-field\"")
                .contains("data-role=\"mask-hash-target-field\"")
                .contains("data-role=\"mask-hash-action\"")
                .contains("class=\"mask-hash-action-field\"")
                .contains(".mask-hash-action-field { grid-column: 1 / -1; }")
                .contains("data-role=\"mask-hash-mask-fields\"")
                .contains("data-role=\"mask-hash-hash-fields\"")
                .contains("data-role=\"mask-hash-mask-char\"")
                .contains("data-role=\"mask-hash-keep-first\"")
                .contains("data-role=\"mask-hash-keep-last\"")
                .contains("data-role=\"mask-hash-algorithm\"")
                .contains("data-role=\"mask-hash-salt\"")
                .contains("data-role=\"move-mask-hash-up\"")
                .contains("data-role=\"move-mask-hash-up\" class=\"sc-btn-icon\" th:aria-label=\"#{studio.action.moveMaskHashRuleUp}\" th:title=\"#{studio.action.moveMaskHashRuleUp}\"")
                .contains("data-role=\"move-mask-hash-down\"")
                .contains("data-role=\"move-mask-hash-down\" class=\"sc-btn-icon\" th:aria-label=\"#{studio.action.moveMaskHashRuleDown}\" th:title=\"#{studio.action.moveMaskHashRuleDown}\"")
                .contains("data-role=\"remove-mask-hash\"")
                .contains("data-role=\"remove-mask-hash\" class=\"sc-btn-icon\" th:aria-label=\"#{studio.action.removeMaskHashRule}\" th:title=\"#{studio.action.removeMaskHashRule}\"")
                .contains("th:text=\"#{studio.field.maskHashRules}\"")
                .contains("th:text=\"#{studio.field.maskHashSourceField}\"")
                .contains("th:text=\"#{studio.field.maskHashTargetField}\"")
                .contains("th:text=\"#{studio.field.maskHashAction}\"")
                .contains("th:text=\"#{studio.field.maskHashMaskChar}\"")
                .contains("th:text=\"#{studio.field.maskHashKeepFirst}\"")
                .contains("th:text=\"#{studio.field.maskHashKeepLast}\"")
                .contains("th:text=\"#{studio.field.maskHashAlgorithm}\"")
                .contains("th:text=\"#{studio.field.maskHashSalt}\"")
                .contains("th:text=\"#{studio.select.maskHash.mask}\"")
                .contains("th:text=\"#{studio.select.maskHash.hash}\"")
                .contains("th:text=\"#{studio.action.addMaskHashRule}\"")
                .doesNotContain("id=\"mask-hash-rules\"");
    }

    @Test
    void templateUsesCalLabelsForCaseWhenConfig() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("<label for=\"case-when-target-field\" class=\"sc-form-label\" th:text=\"#{studio.field.caseWhenTargetField}\">Target field</label>")
                .contains("id=\"case-when-rule-list\"")
                .contains("id=\"case-when-rule-template\"")
                .contains("data-role=\"case-when-condition\"")
                .contains("data-role=\"case-when-value-mode\"")
                .contains("data-role=\"case-when-value\"")
                .contains("data-role=\"case-when-expression\"")
                .contains("data-role=\"move-case-when-up\"")
                .contains("data-role=\"move-case-when-up\" class=\"sc-btn-icon\" th:aria-label=\"#{studio.action.moveCaseUp}\" th:title=\"#{studio.action.moveCaseUp}\"")
                .contains("data-role=\"move-case-when-down\"")
                .contains("data-role=\"move-case-when-down\" class=\"sc-btn-icon\" th:aria-label=\"#{studio.action.moveCaseDown}\" th:title=\"#{studio.action.moveCaseDown}\"")
                .contains("data-role=\"remove-case-when\"")
                .contains("data-role=\"remove-case-when\" class=\"sc-btn-icon\" th:aria-label=\"#{studio.action.removeCase}\" th:title=\"#{studio.action.removeCase}\"")
                .contains("id=\"case-when-default-mode\"")
                .contains("id=\"case-when-default-value-wrapper\"")
                .contains("id=\"case-when-default-expression-wrapper\"")
                .contains("id=\"case-when-default-expression\"")
                .contains("th:text=\"#{studio.field.caseWhenCondition}\"")
                .contains("th:text=\"#{studio.field.caseWhenValueMode}\"")
                .contains("th:text=\"#{studio.field.caseWhenValue}\"")
                .contains("th:text=\"#{studio.field.caseWhenExpression}\"")
                .contains("th:text=\"#{studio.field.caseWhenDefaultMode}\"")
                .contains("th:text=\"#{studio.select.caseWhen.value}\"")
                .contains("th:text=\"#{studio.select.caseWhen.expression}\"")
                .contains("th:text=\"#{studio.select.caseWhen.none}\"")
                .contains("th:text=\"#{studio.action.addCase}\"")
                .doesNotContain("id=\"case-when-cases\"");
    }

    @Test
    void templateUsesCalControlsForRouteConfig() throws Exception {
        String template = loadTemplate();

        assertSegmentedControl(template, "route-match-mode");
        assertThat(template)
                .contains("<label id=\"route-match-mode-label\" for=\"route-match-mode\" class=\"sc-form-label\" th:text=\"#{studio.field.routeMatchMode}\">Match mode</label>")
                .contains("th:text=\"#{studio.select.route.firstMatch}\"")
                .contains("th:text=\"#{studio.select.route.allMatches}\"")
                .contains("<span th:text=\"#{studio.field.includeUnmatched}\">Include unmatched</span>")
                .contains("id=\"route-unmatched-port-wrapper\"")
                .contains("<label for=\"route-unmatched-port\" class=\"sc-form-label\" th:text=\"#{studio.field.unmatchedPort}\">Unmatched port</label>")
                .contains("id=\"route-rule-list\"")
                .contains("id=\"route-rule-template\"")
                .contains("data-role=\"route-port-id\"")
                .contains("data-role=\"route-condition\"")
                .contains("data-role=\"move-route-up\"")
                .contains("data-role=\"move-route-up\" class=\"sc-btn-icon\" th:aria-label=\"#{studio.action.moveRouteUp}\" th:title=\"#{studio.action.moveRouteUp}\"")
                .contains("data-role=\"move-route-down\"")
                .contains("data-role=\"move-route-down\" class=\"sc-btn-icon\" th:aria-label=\"#{studio.action.moveRouteDown}\" th:title=\"#{studio.action.moveRouteDown}\"")
                .contains("data-role=\"remove-route\"")
                .contains("data-role=\"remove-route\" class=\"sc-btn-icon\" th:aria-label=\"#{studio.action.removeRoute}\" th:title=\"#{studio.action.removeRoute}\"")
                .contains("th:text=\"#{studio.field.routePortId}\"")
                .contains("th:text=\"#{studio.field.routeCondition}\"")
                .contains("th:text=\"#{studio.action.addRoute}\"")
                .doesNotContain("id=\"route-routes\"")
                .doesNotContain("<span>Include unmatched</span>");
    }

    @Test
    void templateUsesSegmentedControlsForAggregateModes() throws Exception {
        String template = loadTemplate();

        assertSegmentedControl(template, "aggregate-mode");
        assertSegmentedControl(template, "aggregate-window-type");
        assertSegmentedControl(template, "aggregate-time-mode");
        assertThat(template)
                .contains("data-segmented-control data-target-select=\"aggregate-window-type\" data-segment-count=\"3\"")
                .contains("<select id=\"aggregate-time-unit\" class=\"sc-input\">");
    }

    @Test
    void templateUsesSegmentedControlForJdbcSinkWriteMode() throws Exception {
        String template = loadTemplate();

        assertSegmentedControl(template, "jdbc-sink-write-mode");
    }

    @Test
    void templateUsesCalCheckboxesForHdfsFileSinkConfig() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"hdfs-file-sink-partition-field-write-in-file\" type=\"checkbox\" checked class=\"w-4 h-4 rounded border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900 text-[var(--sc-accent)] focus:ring-blue-500/50 cursor-pointer\"")
                .contains("id=\"hdfs-file-sink-custom-filename\" type=\"checkbox\" class=\"w-4 h-4 rounded border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900 text-[var(--sc-accent)] focus:ring-blue-500/50 cursor-pointer\"")
                .contains("id=\"hdfs-file-sink-csv-use-header-line\" type=\"checkbox\" class=\"w-4 h-4 rounded border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900 text-[var(--sc-accent)] focus:ring-blue-500/50 cursor-pointer\"")
                .doesNotContain("id=\"hdfs-file-sink-partition-field-write-in-file\" type=\"checkbox\" checked class=\"h-4 w-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500\"")
                .doesNotContain("id=\"hdfs-file-sink-custom-filename\" type=\"checkbox\" class=\"h-4 w-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500\"")
                .doesNotContain("id=\"hdfs-file-sink-csv-use-header-line\" type=\"checkbox\" class=\"h-4 w-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500\"");
    }

    @Test
    void templateUsesCalIconButtonsForRenameMappings() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .containsPattern("(?s)<button id=\"add-rename-mapping-button\".*?class=\"sc-btn-icon\"")
                .containsPattern("(?s)<button type=\"button\"\\s+data-role=\"remove-rename-mapping\".*?class=\"sc-btn-icon\"")
                .doesNotContain("id=\"add-rename-mapping-button\"\n                                            type=\"button\"\n                                            th:aria-label=\"#{studio.action.addMapping}\"\n                                            th:title=\"#{studio.action.addMapping}\"\n                                            aria-label=\"Add mapping\"\n                                            title=\"Add mapping\"\n                                            class=\"inline-flex h-3 w-3 items-center justify-center rounded-full")
                .doesNotContain("data-role=\"remove-rename-mapping\"\n                                                th:aria-label=\"#{studio.action.removeMapping}\"\n                                                th:title=\"#{studio.action.removeMapping}\"\n                                                aria-label=\"Remove mapping\"\n                                                title=\"Remove mapping\"\n                                                class=\"h-3 w-3 rounded-full");
    }

    @Test
    void templateProvidesLookupJoinPaletteItemAndInspectorControls() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-operator-key=\"lookupJoin\"")
                .contains("#{studio.operator.lookupJoin}")
                .contains("id=\"lookup-join-config\"")
                .contains("id=\"lookup-join-source-field\"")
                .contains("id=\"lookup-join-target-field\"")
                .contains("id=\"lookup-join-type\"")
                .contains("id=\"lookup-join-missing-strategy\"")
                .contains("id=\"lookup-join-overwrite-target-field\"")
                .contains("id=\"lookup-join-missing-strategy-wrapper\"")
                .contains("id=\"lookup-join-entry-list\"")
                .contains("id=\"lookup-join-entry-template\"")
                .contains("data-role=\"lookup-join-entry-item\"")
                .contains("data-role=\"lookup-join-entry-key\"")
                .contains("data-role=\"lookup-join-field-list\"")
                .contains("data-role=\"add-lookup-join-field\"")
                .contains("data-role=\"lookup-join-field-item\"")
                .contains("data-role=\"lookup-join-field-name\"")
                .contains("data-role=\"lookup-join-field-value\"")
                .contains("data-role=\"lookup-join-field-type\"")
                .contains("data-role=\"remove-lookup-join-field\"")
                .contains("data-role=\"remove-lookup-join-entry\"")
                .contains("class=\"grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_auto] items-center gap-3\"")
                .contains("data-role=\"lookup-join-field-item\" class=\"grid grid-cols-1 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1fr)_8rem_auto] items-center gap-2\"")
                .doesNotContain("id=\"lookup-join-entries\"");
    }

    @Test
    void templateUsesSegmentedControlsForLookupJoinModes() throws Exception {
        String template = loadTemplate();

        assertSegmentedControl(template, "lookup-join-type");
        assertSegmentedControl(template, "lookup-join-missing-strategy");
    }

    @Test
    void templateProvidesStreamJoinPaletteItemInspectorControlsAndLeftPortStackStyles() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-operator-key=\"streamJoin\"")
                .contains("#{studio.operator.streamJoin}")
                .contains("id=\"stream-join-config\"")
                .contains("id=\"stream-join-input-section\"")
                .contains("id=\"stream-join-output-section\"")
                .contains("id=\"stream-join-strategy-section\"")
                .contains("id=\"stream-join-time-window-section\"")
                .contains("id=\"stream-join-left-key-field\"")
                .contains("id=\"stream-join-right-key-field\"")
                .contains("id=\"stream-join-target-field\"")
                .contains("id=\"stream-join-type\"")
                .contains("id=\"stream-join-missing-strategy-wrapper\"")
                .contains("id=\"stream-join-missing-strategy\"")
                .contains("id=\"stream-join-time-mode\"")
                .contains("id=\"stream-join-time-unit\"")
                .contains("id=\"stream-join-window-before\"")
                .contains("id=\"stream-join-window-after\"")
                .contains("id=\"stream-join-watermark-delay\"")
                .contains("id=\"stream-join-late-data-strategy\"")
                .contains("id=\"stream-join-late-data-note\"")
                .contains("id=\"stream-join-overwrite-target-field\"")
                .contains("id=\"stream-join-input-section\" class=\"space-y-3\">\n                                    <div class=\"grid grid-cols-1 xl:grid-cols-2 gap-4\">")
                .contains("<div class=\"grid grid-cols-1 xl:grid-cols-3 gap-4\">")
                .contains("#{studio.field.streamJoinWindowBefore}")
                .contains("#{studio.field.streamJoinWindowAfter}")
                .contains("#{studio.field.lateDataStrategy}")
                .contains("#{studio.help.streamJoinLateDataDrop}")
                .contains(".port-left-stack");
    }

    @Test
    void templateLocalizesStreamJoinConfigSelectOptions() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("<option value=\"LEFT\" th:text=\"#{studio.select.join.left}\">Left join</option>")
                .contains("<option value=\"INNER\" th:text=\"#{studio.select.join.inner}\">Inner join</option>")
                .contains("<option value=\"KEEP_ORIGINAL\" th:text=\"#{studio.select.missing.keepOriginal}\">Keep original</option>")
                .contains("<option value=\"PUT_NULL\" th:text=\"#{studio.select.missing.putNull}\">Write null</option>")
                .contains("<option value=\"PROCESSING_TIME\" th:text=\"#{studio.select.time.processing}\">Processing time</option>")
                .contains("<option value=\"EVENT_TIME\" th:text=\"#{studio.select.time.event}\">Event time</option>")
                .contains("<option value=\"MILLISECONDS\" th:text=\"#{studio.select.timeUnit.milliseconds}\">Milliseconds</option>")
                .contains("<option value=\"SECONDS\" th:text=\"#{studio.select.timeUnit.seconds}\">Seconds</option>")
                .contains("<option value=\"MINUTES\" th:text=\"#{studio.select.timeUnit.minutes}\">Minutes</option>")
                .contains("<option value=\"HOURS\" th:text=\"#{studio.select.timeUnit.hours}\">Hours</option>")
                .contains("<option value=\"DROP\" th:text=\"#{studio.select.lateData.drop}\">Discard after window closes</option>");
    }

    @Test
    void templateUsesSegmentedControlsForStreamJoinModes() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains(".studio-segmented-control")
                .contains(".studio-segmented-option")
                .contains("data-segmented-control data-target-select=\"stream-join-type\"")
                .contains("data-segmented-value=\"LEFT\" th:text=\"#{studio.select.join.left}\"")
                .contains("data-segmented-value=\"INNER\" th:text=\"#{studio.select.join.inner}\"")
                .contains("select id=\"stream-join-type\" class=\"studio-segmented-native\"")
                .contains("data-segmented-control data-target-select=\"stream-join-time-mode\"")
                .contains("data-segmented-value=\"PROCESSING_TIME\" th:text=\"#{studio.select.time.processing}\"")
                .contains("data-segmented-value=\"EVENT_TIME\" th:text=\"#{studio.select.time.event}\"")
                .contains("select id=\"stream-join-time-mode\" class=\"studio-segmented-native\"")
                .doesNotContain("<select id=\"stream-join-type\" class=\"sc-input\">")
                .doesNotContain("<select id=\"stream-join-time-mode\" class=\"sc-input\">");
    }

    @Test
    void sourceInspectorShowsDisplayNameAndKeepsSampleDataAtBottom() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"node-display-name\"")
                .contains("th:text=\"#{studio.node.displayName}\"")
                .contains("th:placeholder=\"#{studio.node.displayName.placeholder}\"")
                .contains("id=\"source-sample-list\"")
                .doesNotContain("id=\"source-use-mock\"");

        assertThat(template.indexOf("id=\"source-sample-list\""))
                .isGreaterThan(template.indexOf("id=\"kafka-config-section\""));
    }

    @Test
    void templateProvidesJdbcSourcePaletteItemAndInspectorControls() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-operator-key=\"jdbcSource\"")
                .contains("#{studio.operator.jdbcSource}")
                .contains("id=\"jdbc-config-section\"")
                .contains("id=\"jdbc-url\"")
                .contains("id=\"jdbc-driver\"")
                .contains("id=\"jdbc-username\"")
                .contains("id=\"jdbc-password\"")
                .contains("id=\"jdbc-query\"")
                .contains("id=\"jdbc-read-mode\"")
                .contains("id=\"jdbc-cursor-field\"")
                .contains("id=\"jdbc-cursor-type\"")
                .contains("id=\"jdbc-initial-cursor-value\"")
                .contains("id=\"jdbc-poll-interval-millis\"")
                .contains("id=\"jdbc-fetch-size\"");
    }

    @Test
    void kafkaSourceSampleDataUsesRepeatableInputsInsteadOfJsonArrayTextarea() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"source-sample-list\"")
                .contains("id=\"add-source-sample-button\"")
                .contains("th:aria-label=\"#{studio.action.addSample}\"")
                .contains("th:title=\"#{studio.action.addSample}\"")
                .contains("th:text=\"#{studio.action.addSample}\"")
                .contains("id=\"source-sample-item-template\"")
                .contains("data-role=\"source-sample-item\"")
                .contains("data-role=\"source-sample-input\"")
                .contains("data-role=\"remove-source-sample\"")
                .contains("th:aria-label=\"#{studio.action.removeSample}\"")
                .contains("th:title=\"#{studio.action.removeSample}\"")
                .contains("th:placeholder=\"#{studio.field.sampleMessage.placeholder}\"")
                .contains("th:text=\"#{studio.field.sampleData}\"")
                .doesNotContain("id=\"source-sample-data\"");
    }

    @Test
    void kafkaSourceAndSinkExposeTextFormatsAndSinkMessageFieldInput() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .containsPattern("(?s)<select id=\"source-format\"[^>]*>\\s*<option value=\"JSON\">JSON</option>\\s*<option value=\"TEXT\">TEXT</option>\\s*</select>")
                .containsPattern("(?s)id=\"sink-format\".*?<option value=\"TEXT\">TEXT</option>")
                .containsPattern("(?s)<div[^>]*id=\"sink-message-field-wrapper\"[^>]*class=\"[^\"]*\\bhidden\\b[^\"]*\"[^>]*>")
                .contains("id=\"sink-message-field\"")
                .contains("th:text=\"#{studio.field.outputFieldName}\"")
                .contains("th:placeholder=\"#{studio.field.sinkMessageField.placeholder}\"");
    }

    @Test
    void kafkaSourceAndSinkExposeAuthSelectorsAndScramMechanisms() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"source-auth-type\"")
                .contains("id=\"source-auth-username\"")
                .contains("id=\"source-auth-password\"")
                .contains("id=\"source-scram-mechanism\"")
                .contains("id=\"sink-auth-type\"")
                .contains("id=\"sink-auth-username\"")
                .contains("id=\"sink-auth-password\"")
                .contains("id=\"sink-scram-mechanism\"")
                .contains("th:text=\"#{studio.field.authType}\"")
                .contains("th:text=\"#{studio.field.username}\"")
                .contains("th:text=\"#{studio.field.password}\"")
                .contains("th:text=\"#{studio.field.scramMechanism}\"")
                .contains("th:placeholder=\"#{studio.field.kafkaPassword.placeholder}\"")
                .containsPattern("(?s)<select id=\"source-auth-type\"[^>]*>\\s*<option value=\"NONE\">None</option>\\s*<option value=\"SASL_PLAIN\">SASL/PLAIN</option>\\s*<option value=\"SASL_SCRAM\">SASL/SCRAM</option>\\s*</select>")
                .containsPattern("(?s)<select id=\"sink-auth-type\"[^>]*>\\s*<option value=\"NONE\">None</option>\\s*<option value=\"SASL_PLAIN\">SASL/PLAIN</option>\\s*<option value=\"SASL_SCRAM\">SASL/SCRAM</option>\\s*</select>")
                .containsPattern("(?s)<select id=\"source-scram-mechanism\"[^>]*>\\s*<option value=\"SCRAM-SHA-256\">SCRAM-SHA-256</option>\\s*<option value=\"SCRAM-SHA-512\">SCRAM-SHA-512</option>\\s*</select>")
                .containsPattern("(?s)<select id=\"sink-scram-mechanism\"[^>]*>\\s*<option value=\"SCRAM-SHA-256\">SCRAM-SHA-256</option>\\s*<option value=\"SCRAM-SHA-512\">SCRAM-SHA-512</option>\\s*</select>");
    }

    @Test
    void kafkaSourceInspectorPutsGroupIdAndConsumeModeOnSeparateRows() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"source-group-id\"")
                .contains("id=\"source-consume-mode\"")
                .contains("th:text=\"#{studio.field.consumeMode}\"")
                .doesNotContainPattern("(?s)<div class=\"grid grid-cols-2 gap-4\">\\s*<div>\\s*<label for=\"source-group-id\".*?<label for=\"source-consume-mode\"");
    }

    @Test
    void kafkaSourceTopicsPlaceholderUsesSingleTopicExample() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"source-topics\"")
                .contains("placeholder=\"topic_a\"")
                .doesNotContain("placeholder=\"topic_a,topic_b\"");
    }

    @Test
    void kafkaSinkInspectorPutsDeliveryGuaranteeAndFormatOnSeparateRows() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"sink-delivery-guarantee\"")
                .contains("id=\"sink-format\"")
                .contains("th:text=\"#{studio.field.deliveryGuarantee}\"")
                .contains("th:text=\"#{studio.field.format}\"")
                .containsPattern("(?s)<select id=\"sink-delivery-guarantee\"[^>]*>\\s*<option value=\"NONE\">NONE</option>\\s*<option value=\"AT_LEAST_ONCE\">AT_LEAST_ONCE</option>\\s*<option value=\"EXACTLY_ONCE\">EXACTLY_ONCE</option>\\s*</select>")
                .doesNotContainPattern("(?s)<div class=\"grid grid-cols-2 gap-4\">\\s*<div>\\s*<label for=\"sink-delivery-guarantee\".*?<label for=\"sink-format\"");
    }

    @Test
    void renameInspectorUsesRepeatableMappingRowsInsteadOfJsonTextarea() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-operator-key=\"rename\" th:text=\"#{studio.operator.rename}\"")
                .contains("id=\"rename-mapping-list\"")
                .contains("id=\"add-rename-mapping-button\"")
                .contains("th:aria-label=\"#{studio.action.addMapping}\"")
                .contains("th:title=\"#{studio.action.addMapping}\"")
                .contains("th:text=\"#{studio.action.addMapping}\"")
                .contains("id=\"rename-mapping-item-template\"")
                .contains("data-role=\"rename-mapping-item\" class=\"grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] items-center gap-3\"")
                .contains("data-role=\"rename-source-field\"")
                .contains("data-role=\"rename-target-field\"")
                .contains("data-role=\"remove-rename-mapping\"")
                .contains("th:aria-label=\"#{studio.action.removeMapping}\"")
                .contains("th:title=\"#{studio.action.removeMapping}\"")
                .contains("th:placeholder=\"#{studio.field.inputField.placeholder}\"")
                .contains("th:placeholder=\"#{studio.field.outputField.placeholder}\"")
                .doesNotContain("id=\"rename-mapping\"");
    }

    @Test
    void putAndPruneInspectorsUseI18nInputFieldCopy() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-operator-key=\"put\" th:text=\"#{studio.operator.put}\"")
                .contains("data-operator-key=\"prune\" th:text=\"#{studio.operator.prune}\"")
                .contains("for=\"put-field\" class=\"sc-form-label\" th:text=\"#{studio.field.inputField}\"")
                .contains("id=\"put-value-mode\"")
                .contains("data-segmented-control data-target-select=\"put-value-mode\" data-segment-count=\"3\"")
                .contains("data-segmented-value=\"LITERAL\"")
                .contains("data-segmented-value=\"FIELD_REFERENCE\"")
                .contains("data-segmented-value=\"TEMPLATE\"")
                .contains("id=\"put-literal-value-wrapper\"")
                .contains("id=\"put-reference-field-wrapper\"")
                .contains("id=\"put-template-wrapper\"")
                .contains("for=\"put-literal-value\" class=\"sc-form-label\" th:text=\"#{studio.field.fieldValue}\"")
                .contains("for=\"put-reference-field\" class=\"sc-form-label\" th:text=\"#{studio.field.putReferenceField}\"")
                .contains("for=\"put-template-value\" class=\"sc-form-label\" th:text=\"#{studio.field.putTemplate}\"")
                .contains("id=\"prune-field-list\"")
                .contains("id=\"add-prune-field-button\"")
                .contains("th:aria-label=\"#{studio.action.addField}\"")
                .contains("th:title=\"#{studio.action.addField}\"")
                .contains("th:text=\"#{studio.action.addField}\"")
                .contains("id=\"prune-field-item-template\"")
                .contains("data-role=\"prune-field-item\" class=\"grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_auto] items-center gap-3\"")
                .contains("data-role=\"prune-field-name\"")
                .contains("data-role=\"remove-prune-field\"")
                .contains("th:aria-label=\"#{studio.action.removeField}\"")
                .contains("th:title=\"#{studio.action.removeField}\"")
                .contains("th:placeholder=\"#{studio.field.inputField.placeholder}\"")
                .contains("placeholder=\"test\"")
                .contains("placeholder=\"status\"")
                .contains("placeholder=\"${status}-${amount}\"")
                .doesNotContain("id=\"put-value\"")
                .doesNotContain("id=\"prune-fields\"");
    }

    @Test
    void deserializeInspectorIncludesTargetFieldInput() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("for=\"deserialize-field\" class=\"sc-form-label\" th:text=\"#{studio.field.inputField}\"")
                .contains("id=\"deserialize-target-field\"")
                .contains("for=\"deserialize-target-field\" class=\"sc-form-label\" th:text=\"#{studio.field.outputField}\"")
                .contains("placeholder=\"payload\"")
                .contains("placeholder=\"parsed\"");
    }

    @Test
    void transformSerdeInspectorsExposeMultiFormatControls() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("for=\"serialize-source-fields\" class=\"sc-form-label\" th:text=\"#{studio.field.inputField}\"")
                .contains("for=\"serialize-target-field\" class=\"sc-form-label\" th:text=\"#{studio.field.outputField}\"")
                .contains("placeholder=\"payload\"")
                .containsPattern("(?s)<input id=\"serialize-target-field\"[^>]*placeholder=\"parsed\"")
                .contains("id=\"deserialize-format\"")
                .contains("id=\"deserialize-field-names\"")
                .contains("id=\"deserialize-field-names-wrapper\"")
                .contains("id=\"deserialize-delimiter-wrapper\"")
                .contains("id=\"deserialize-delimiter\"")
                .contains("id=\"serialize-format\"")
                .contains("id=\"serialize-delimiter-wrapper\"")
                .contains("id=\"serialize-delimiter\"")
                .contains("th:text=\"#{studio.field.fieldNames}\"")
                .contains("th:placeholder=\"#{studio.field.fieldNames.placeholder}\"")
                .contains("th:text=\"#{studio.field.delimiter}\"")
                .contains("th:placeholder=\"#{studio.field.delimiter.placeholder}\"")
                .doesNotContain("TEXT passthrough")
                .containsPattern("(?s)<select id=\"deserialize-format\"[^>]*>.*?<option value=\"JSON\">JSON</option>.*?<option value=\"KV\">KV</option>.*?<option value=\"CSV\">CSV</option>.*?<option value=\"XML\">XML</option>.*?</select>")
                .containsPattern("(?s)<select id=\"serialize-format\"[^>]*>.*?<option value=\"JSON\">JSON</option>.*?<option value=\"KV\">KV</option>.*?<option value=\"CSV\">CSV</option>.*?<option value=\"XML\">XML</option>.*?</select>");
    }

    @Test
    void filterInspectorUsesI18nConditionPlaceholder() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"filter-condition\"")
                .contains("for=\"filter-condition\" class=\"sc-form-label\" th:text=\"#{studio.field.filterCondition}\"")
                .contains("th:placeholder=\"#{studio.field.filterCondition.placeholder}\"")
                .contains("age > 18 && status == 'active'");
    }

    @Test
    void transformInspectorsExposeGrokCastAndEvalControls() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-operator-key=\"eval\" th:text=\"#{studio.operator.eval}\"")
                .contains("data-operator-key=\"grok\" th:text=\"#{studio.operator.grok}\"")
                .contains("id=\"grok-config\"")
                .contains("id=\"grok-input-field\"")
                .contains("id=\"grok-output-field\"")
                .contains("id=\"grok-pattern\"")
                .contains("th:placeholder=\"#{studio.field.grokPattern.placeholder}\"")
                .containsPattern("(?s)<input id=\"grok-input-field\"[^>]*placeholder=\"payload\"")
                .containsPattern("(?s)<input id=\"grok-output-field\"[^>]*placeholder=\"parsed\"")
                .contains("id=\"cast-config\"")
                .contains("id=\"cast-input-field\"")
                .contains("id=\"cast-output-mode\"")
                .contains("id=\"cast-output-field-wrapper\"")
                .contains("id=\"cast-output-field\"")
                .contains("id=\"cast-target-type\"")
                .contains("data-segmented-control data-target-select=\"cast-output-mode\"")
                .contains("data-segmented-value=\"OVERWRITE\"")
                .contains("data-segmented-value=\"NEW_FIELD\"")
                .contains("th:text=\"#{studio.select.cast.overwrite}\"")
                .contains("th:text=\"#{studio.select.cast.newField}\"")
                .contains("id=\"eval-config\"")
                .contains("id=\"eval-target-field\"")
                .contains("id=\"eval-expression\"")
                .contains("id=\"eval-output-mode\"")
                .contains("id=\"eval-error-strategy\"")
                .contains("for=\"eval-target-field\" class=\"sc-form-label\" th:text=\"#{studio.field.outputField}\"")
                .contains("for=\"eval-output-mode\" class=\"sc-form-label\" th:text=\"#{studio.field.outputMode}\"")
                .contains("for=\"eval-error-strategy\" class=\"sc-form-label\" th:text=\"#{studio.field.errorStrategy}\"")
                .contains("for=\"eval-expression\" class=\"sc-form-label\" th:text=\"#{studio.field.expression}\"")
                .contains("th:placeholder=\"#{studio.field.expression.placeholder}\"")
                .contains("th:text=\"#{studio.select.outputMode.overwrite}\"")
                .contains("th:text=\"#{studio.select.outputMode.writeIfAbsent}\"")
                .contains("th:text=\"#{studio.select.eval.keepOriginal}\"")
                .contains("th:text=\"#{studio.select.eval.putNull}\"")
                .contains("th:text=\"#{studio.select.eval.discard}\"")
                .contains("th:text=\"#{studio.select.eval.fail}\"")
                .containsPattern("(?s)<input id=\"eval-target-field\"[^>]*placeholder=\"calc.total\"")
                .containsPattern("(?s)<select id=\"cast-target-type\"[^>]*>.*?<option value=\"STRING\">STRING</option>.*?<option value=\"INT\">INT</option>.*?<option value=\"LONG\">LONG</option>.*?<option value=\"DOUBLE\">DOUBLE</option>.*?<option value=\"FLOAT\">FLOAT</option>.*?<option value=\"BOOLEAN\">BOOLEAN</option>.*?</select>");
    }

    @Test
    void customCodeInspectorUsesI18nKeys() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("data-operator-key=\"customCode\" th:text=\"#{studio.operator.customCode}\"")
                .contains("id=\"custom-code-config\"")
                .contains("id=\"custom-code-class-name\"")
                .contains("id=\"custom-code-source\"")
                .contains("id=\"custom-code-error-strategy\"")
                .contains("th:text=\"#{studio.field.className}\"")
                .contains("th:text=\"#{studio.field.errorStrategy}\"")
                .contains("th:text=\"#{studio.field.javaCode}\"");
    }

    @Test
    void studioStillBootstrapsClassBasedThemeAfterSharedHeadRefresh() throws Exception {
        String template = loadTemplate("templates/fragments/app-shell.html");

        assertThat(template)
                .contains("tailwind.config = {")
                .contains("darkMode: 'class'")
                .contains("localStorage.getItem('theme') === 'dark'")
                .contains("document.documentElement.classList.add('dark');");
    }

    @Test
    void templateIncludesStudioThemeToggleHookIds() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"theme-toggle\"")
                .contains("id=\"theme-toggle-light-icon\"")
                .contains("id=\"theme-toggle-dark-icon\"")
                .contains("th:aria-label=\"#{topbar.themeToggle}\"");
    }

    @Test
    void templateKeepsReadableStructureAndValidClosingTags() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("<h1 id=\"studio-title\"")
                .contains("id=\"studio-status\"")
                .contains("id=\"operator-palette\"")
                .contains("id=\"pipeline-name\"")
                .contains("id=\"selected-node-title\"")
                .doesNotContain("source-use-mock")
                .doesNotContain("?/span>")
                .doesNotContain("?/p>")
                .doesNotContain("?/div>")
                .doesNotContain("?/button>")
                .doesNotContain("?/label>")
                .doesNotContain("?/h3>");
    }

    private String loadTemplate() throws IOException {
        return loadTemplate("templates/studio.html");
    }

    private String loadTemplate(String path) throws IOException {
        ClassPathResource resource = new ClassPathResource(path);
        return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
                .replace("\r\n", "\n")
                .replace("\r", "\n");
    }

    private void assertSegmentedControl(String template, String selectId) {
        assertThat(template)
                .contains("data-segmented-control data-target-select=\"" + selectId + "\"")
                .contains("select id=\"" + selectId + "\" class=\"studio-segmented-native\"")
                .doesNotContain("<select id=\"" + selectId + "\" class=\"sc-input\">");
    }
}
