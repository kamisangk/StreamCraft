package com.streamcraft.service.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class StudioInspectorContractTest {

    @Test
    void templateSeparatesPipelineMetaAndNodeInspectorPanels() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"pipeline-meta-panel\"")
                .contains("id=\"node-detail-panel\"");
    }

    @Test
    void scriptDefaultsLoadedDefinitionsToPipelineMetaInspector() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("const pipelineMetaPanel = byId(\"pipeline-meta-panel\");")
                .contains("const nodeDetailPanel = byId(\"node-detail-panel\");")
                .contains("pipelineMetaPanel.classList.toggle(\"hidden\", Boolean(selectedNode));")
                .contains("nodeDetailPanel.classList.toggle(\"hidden\", !selectedNode);")
                .contains("state.selectedNodeId = null;")
                .doesNotContain("pipelineMetaPanel.hidden = Boolean(selectedNode);")
                .doesNotContain("nodeDetailPanel.hidden = !selectedNode;")
                .doesNotContain("state.selectedNodeId = state.nodes[0]?.id ?? null;");
    }

    @Test
    void scriptSubmitsRuntimeTargetDrivenRunResources() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("runtimeTarget: null")
                .contains("function updateRuntimeResourceControls()")
                .contains("function runResourcePayload()")
                .contains("async function loadRuntimeTarget()")
                .contains("const response = await fetch(\"/api/runtime-target\");")
                .contains("state.runtimeTarget = target.configured ? target : null;")
                .contains("payload.parallelism = positiveIntegerValue(\"run-parallelism\", DEFAULT_RUN_PARALLELISM);")
                .contains("...runResourcePayload()")
                .doesNotContain("function selectedRuntimeTargetType()")
                .doesNotContain("DEFAULT_YARN_JOBMANAGER_MEMORY")
                .doesNotContain("DEFAULT_YARN_TASKMANAGER_MEMORY")
                .doesNotContain("DEFAULT_YARN_TASKMANAGER_SLOTS")
                .doesNotContain("FLINK_YARN_APPLICATION")
                .doesNotContain("payload.jobManagerMemory")
                .doesNotContain("payload.taskManagerMemory")
                .doesNotContain("payload.taskManagerSlots")
                .doesNotContain("run-yarn-jobmanager-memory")
                .doesNotContain("run-yarn-taskmanager-memory")
                .doesNotContain("run-yarn-taskmanager-slots")
                .doesNotContain("function selectedCluster()")
                .doesNotContain("state.clusters = clusters;")
                .doesNotContain("clusterConnectionId");
    }

    @Test
    void templateProvidesCloseControlForNodeInspector() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"close-node-inspector-button\"");
    }

    @Test
    void scriptBindsCloseControlToReturnToPipelineMetaInspector() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("function closeNodeInspector()")
                .contains("byId(\"close-node-inspector-button\")?.addEventListener(\"click\", closeNodeInspector);")
                .contains("syncSelectedNodeConfigFromInspector();")
                .contains("state.selectedNodeId = null;")
                .contains("state.selectedEdgeId = null;")
                .contains("renderStudio();");
    }

    @Test
    void scriptTreatsCanvasBlankAreaClicksAsInspectorCloseGesture() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("if (event.target.closest(\".draggable-node\"))")
                .contains("closeNodeInspector();")
                .doesNotContain("if (event.target === dropZone)");
    }

    @Test
    void scriptStoresDisplayNameAndNoLongerHidesSampleDataSection() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("selectedNode.displayName")
                .contains("byId(\"node-display-name\")")
                .contains("function nodeDisplayTitle(node)")
                .doesNotContain("byId(\"mock-data-section\")?.classList.toggle(\"hidden\", !useMock);");
    }

    @Test
    void scriptStoresKafkaSourceSamplesAsStringArrayAndRendersRepeatableInputs() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("sampleData: [\"\"]")
                .contains("function normalizeSourceSampleData(")
                .contains("function renderSourceSampleInputs(")
                .contains("function collectSourceSampleInputs(")
                .contains("function removeSourceSampleInput(")
                .contains("const showRemoveButton = normalizedSamples.length > 1;")
                .contains("removeButton.classList.toggle(\"hidden\", !showRemoveButton);")
                .contains("byId(\"add-source-sample-button\")?.addEventListener(\"click\", () => {")
                .contains("byId(\"source-sample-list\")?.addEventListener(\"input\", event => {")
                .contains("byId(\"source-sample-list\")?.addEventListener(\"click\", event => {")
                .contains("event.target.closest(\"[data-role='remove-source-sample']\")")
                .contains("removeButton.closest(\"[data-role='source-sample-item']\")")
                .contains("event.target.matches(\"[data-role='source-sample-input']\")")
                .doesNotContain("removeButton.closest(\".rounded-xl\")")
                .doesNotContain("JSON.stringify(selectedNode.config.sampleData || [], null, 2)");
    }

    @Test
    void scriptPersistsSinkMessageFieldAndTogglesItFromFormatSelection() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("messageField: \"_streamcraft_message\"")
                .contains("selectedNode.config.messageField = byId(\"sink-message-field\")?.value?.trim?.() || \"_streamcraft_message\";")
                .contains("function updateSinkFormatUI(")
                .contains("byId(\"sink-format\")?.addEventListener(\"change\", () => {")
                .contains("byId(\"sink-message-field-wrapper\")?.classList.toggle(\"hidden\", format !== \"TEXT\");");
    }

    @Test
    void scriptClearsIrrelevantKafkaAuthFieldsWhenAuthModeChanges() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("function readKafkaAuthConfig(")
                .contains("if (authType === \"NONE\")")
                .contains("return { authType, username: \"\", password: \"\", scramMechanism: \"\" };")
                .contains("if (authType === \"SASL_PLAIN\")")
                .contains("return { authType, username, password, scramMechanism: \"\" };")
                .contains("return { authType, username, password, scramMechanism };")
                .contains("const authConfig = readKafkaAuthConfig(")
                .contains("Object.assign(selectedNode.config, authConfig);");
    }

    @Test
    void scriptPersistsJdbcSourceConfigAndTogglesSourceSections() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"JDBC_SOURCE\"")
                .contains("defaultName: t(\"studio.operator.jdbcSource\", \"JDBC Source\")")
                .contains("function updateSourceConfigSections(")
                .contains("function updateJdbcReadModeUI(")
                .contains("selectedNode.config.url = byId(\"jdbc-url\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.driver = byId(\"jdbc-driver\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.query = byId(\"jdbc-query\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.readMode = byId(\"jdbc-read-mode\")?.value || \"FULL\";")
                .contains("selectedNode.config.cursorField = byId(\"jdbc-cursor-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.cursorType = byId(\"jdbc-cursor-type\")?.value || \"STRING\";")
                .contains("selectedNode.config.initialCursorValue = byId(\"jdbc-initial-cursor-value\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.pollIntervalMillis = Number(byId(\"jdbc-poll-interval-millis\")?.value || 5000);")
                .contains("selectedNode.config.fetchSize = Number(byId(\"jdbc-fetch-size\")?.value || 1000);")
                .contains("\"jdbc-url\"")
                .contains("\"jdbc-driver\"")
                .contains("\"jdbc-query\"")
                .contains("\"jdbc-read-mode\"");
    }

    @Test
    void scriptPersistsElasticsearchSourceConfigAndTogglesSourceSections() throws Exception {
        String script = loadScript();
        String template = loadTemplate();

        assertThat(script)
                .contains("operator: \"ELASTICSEARCH_SOURCE\"")
                .contains("defaultName: t(\"studio.operator.elasticsearchSource\", \"Elasticsearch Source\")")
                .contains("byId(\"elasticsearch-config-section\")?.classList.toggle(\"hidden\", operator !== \"ELASTICSEARCH_SOURCE\");")
                .contains("function updateElasticsearchReadModeUI(")
                .contains("selectedNode.config.hosts = splitTopics(byId(\"elasticsearch-hosts\")?.value || \"\");")
                .contains("selectedNode.config.index = byId(\"elasticsearch-index\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.source = splitTopics(byId(\"elasticsearch-source-fields\")?.value || \"\");")
                .contains("selectedNode.config.query = parseJsonValue(byId(\"elasticsearch-query\")?.value || \"{}\", {});")
                .contains("selectedNode.config.readMode = byId(\"elasticsearch-read-mode\")?.value || \"FULL\";")
                .contains("selectedNode.config.cursorField = byId(\"elasticsearch-cursor-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.cursorType = byId(\"elasticsearch-cursor-type\")?.value || \"STRING\";")
                .contains("selectedNode.config.initialCursorValue = byId(\"elasticsearch-initial-cursor-value\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.scrollSize = Number(byId(\"elasticsearch-scroll-size\")?.value || 100);")
                .contains("selectedNode.config.scrollTime = byId(\"elasticsearch-scroll-time\")?.value?.trim?.() || \"1m\";")
                .contains("\"elasticsearch-hosts\"")
                .contains("\"elasticsearch-index\"")
                .contains("\"elasticsearch-read-mode\"");

        assertThat(template)
                .contains("data-operator-key=\"elasticsearchSource\"")
                .contains("id=\"elasticsearch-config-section\"")
                .contains("id=\"elasticsearch-hosts\"")
                .contains("id=\"elasticsearch-index\"")
                .contains("id=\"elasticsearch-read-mode\"");
    }

    @Test
    void scriptPersistsInfluxDbSourceConfigAndTogglesSourceSections() throws Exception {
        String script = loadScript();
        String template = loadTemplate();

        assertThat(script)
                .contains("operator: \"INFLUXDB_SOURCE\"")
                .contains("defaultName: t(\"studio.operator.influxDbSource\", \"InfluxDB Source\")")
                .contains("byId(\"influxdb-config-section\")?.classList.toggle(\"hidden\", operator !== \"INFLUXDB_SOURCE\");")
                .contains("function updateInfluxDbReadModeUI(")
                .contains("selectedNode.config.url = byId(\"influxdb-url\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.database = byId(\"influxdb-database\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.sql = byId(\"influxdb-sql\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.schema = parseJsonValue(byId(\"influxdb-schema\")?.value || \"{}\", {});")
                .contains("selectedNode.config.readMode = byId(\"influxdb-read-mode\")?.value || \"FULL\";")
                .contains("selectedNode.config.cursorField = byId(\"influxdb-cursor-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.queryTimeoutSeconds = Number(byId(\"influxdb-query-timeout-seconds\")?.value || 30);")
                .contains("\"influxdb-url\"")
                .contains("\"influxdb-database\"")
                .contains("\"influxdb-read-mode\"");

        assertThat(template)
                .contains("data-operator-key=\"influxDbSource\"")
                .contains("id=\"influxdb-config-section\"")
                .contains("id=\"influxdb-url\"")
                .contains("id=\"influxdb-database\"")
                .contains("id=\"influxdb-sql\"")
                .contains("id=\"influxdb-read-mode\"");
    }

    @Test
    void scriptPersistsHdfsFileSourceConfigAndTogglesSourceSections() throws Exception {
        String script = loadScript();
        String template = loadTemplate();

        assertThat(script)
                .contains("operator: \"HDFS_FILE_SOURCE\"")
                .contains("defaultName: t(\"studio.operator.hdfsFileSource\", \"HDFS File Source\")")
                .contains("byId(\"hdfs-file-config-section\")?.classList.toggle(\"hidden\", operator !== \"HDFS_FILE_SOURCE\");")
                .contains("function updateHdfsFileReadModeUI(")
                .contains("selectedNode.config[\"fs.defaultFS\"] = byId(\"hdfs-file-default-fs\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.path = byId(\"hdfs-file-path\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.file_format_type = byId(\"hdfs-file-format-type\")?.value || \"JSON\";")
                .contains("selectedNode.config.schema = parseJsonValue(byId(\"hdfs-file-schema\")?.value || \"{}\", {});")
                .contains("selectedNode.config.readMode = byId(\"hdfs-file-read-mode\")?.value || \"FULL\";")
                .contains("selectedNode.config.file_filter_pattern = byId(\"hdfs-file-filter-pattern\")?.value?.trim?.() || \"\";")
                .contains("\"hdfs-file-default-fs\"")
                .contains("\"hdfs-file-path\"")
                .contains("\"hdfs-file-format-type\"")
                .contains("\"hdfs-file-read-mode\"");

        assertThat(template)
                .contains("data-operator-key=\"hdfsFileSource\"")
                .contains("id=\"hdfs-file-config-section\"")
                .contains("id=\"hdfs-file-default-fs\"")
                .contains("id=\"hdfs-file-path\"")
                .contains("id=\"hdfs-file-format-type\"")
                .contains("id=\"hdfs-file-read-mode\"");
    }

    @Test
    void scriptPersistsJdbcSinkConfigAndTogglesSinkSections() throws Exception {
        String script = loadScript();
        String template = loadTemplate();

        assertThat(script)
                .contains("operator: \"JDBC_SINK\"")
                .contains("defaultName: t(\"studio.operator.jdbcSink\", \"JDBC Sink\")")
                .contains("function updateSinkConfigSections(")
                .contains("byId(\"kafka-sink-config-section\")?.classList.toggle(\"hidden\", operator !== \"KAFKA_SINK\");")
                .contains("byId(\"jdbc-sink-config-section\")?.classList.toggle(\"hidden\", operator !== \"JDBC_SINK\");")
                .contains("selectedNode.config.url = byId(\"jdbc-sink-url\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.driver = byId(\"jdbc-sink-driver\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.tablePath = byId(\"jdbc-sink-table-path\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.writeMode = byId(\"jdbc-sink-write-mode\")?.value || \"INSERT\";")
                .contains("selectedNode.config.fields = splitTopics(byId(\"jdbc-sink-fields\")?.value || \"\");")
                .contains("selectedNode.config.keyFields = splitTopics(byId(\"jdbc-sink-key-fields\")?.value || \"\");")
                .contains("selectedNode.config.batchSize = Number(byId(\"jdbc-sink-batch-size\")?.value || 500);")
                .contains("selectedNode.config.flushIntervalMillis = Number(byId(\"jdbc-sink-flush-interval-millis\")?.value || 5000);")
                .contains("\"jdbc-sink-url\"")
                .contains("\"jdbc-sink-driver\"")
                .contains("\"jdbc-sink-table-path\"")
                .contains("\"jdbc-sink-write-mode\"");

        assertThat(template)
                .contains("data-operator-key=\"jdbcSink\"")
                .contains("id=\"jdbc-sink-config-section\"")
                .contains("id=\"jdbc-sink-url\"")
                .contains("id=\"jdbc-sink-driver\"")
                .contains("id=\"jdbc-sink-table-path\"")
                .contains("id=\"jdbc-sink-write-mode\"")
                .contains("id=\"jdbc-sink-fields\"")
                .contains("id=\"jdbc-sink-key-fields\"");
    }

    @Test
    void scriptPersistsElasticsearchSinkConfigAndTogglesSinkSections() throws Exception {
        String script = loadScript();
        String template = loadTemplate();

        assertThat(script)
                .contains("operator: \"ELASTICSEARCH_SINK\"")
                .contains("defaultName: t(\"studio.operator.elasticsearchSink\", \"Elasticsearch Sink\")")
                .contains("byId(\"elasticsearch-sink-config-section\")?.classList.toggle(\"hidden\", operator !== \"ELASTICSEARCH_SINK\");")
                .contains("selectedNode.config.hosts = splitTopics(byId(\"elasticsearch-sink-hosts\")?.value || \"\");")
                .contains("selectedNode.config.index = byId(\"elasticsearch-sink-index\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.primaryKeys = splitTopics(byId(\"elasticsearch-sink-primary-keys\")?.value || \"\");")
                .contains("selectedNode.config.fields = splitTopics(byId(\"elasticsearch-sink-fields\")?.value || \"\");")
                .contains("selectedNode.config.maxBatchSize = Number(byId(\"elasticsearch-sink-max-batch-size\")?.value || 10);")
                .contains("selectedNode.config.maxRetryCount = Number(byId(\"elasticsearch-sink-max-retry-count\")?.value || 3);")
                .contains("\"elasticsearch-sink-hosts\"")
                .contains("\"elasticsearch-sink-index\"")
                .contains("\"elasticsearch-sink-primary-keys\"");

        assertThat(template)
                .contains("data-operator-key=\"elasticsearchSink\"")
                .contains("id=\"elasticsearch-sink-config-section\"")
                .contains("id=\"elasticsearch-sink-hosts\"")
                .contains("id=\"elasticsearch-sink-index\"")
                .contains("id=\"elasticsearch-sink-primary-keys\"")
                .contains("id=\"elasticsearch-sink-fields\"")
                .contains("id=\"elasticsearch-sink-auth-type\"");
    }

    @Test
    void scriptPersistsInfluxDbSinkConfigAndTogglesSinkSections() throws Exception {
        String script = loadScript();
        String template = loadTemplate();

        assertThat(script)
                .contains("operator: \"INFLUXDB_SINK\"")
                .contains("defaultName: t(\"studio.operator.influxDbSink\", \"InfluxDB Sink\")")
                .contains("byId(\"influxdb-sink-config-section\")?.classList.toggle(\"hidden\", operator !== \"INFLUXDB_SINK\");")
                .contains("selectedNode.config.url = byId(\"influxdb-sink-url\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.database = byId(\"influxdb-sink-database\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.measurement = byId(\"influxdb-sink-measurement\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.keyTime = byId(\"influxdb-sink-key-time\")?.value?.trim?.() || \"time\";")
                .contains("selectedNode.config.keyTags = splitTopics(byId(\"influxdb-sink-key-tags\")?.value || \"\");")
                .contains("selectedNode.config.fields = splitTopics(byId(\"influxdb-sink-fields\")?.value || \"\");")
                .contains("selectedNode.config.batchSize = Number(byId(\"influxdb-sink-batch-size\")?.value || 100);")
                .contains("selectedNode.config.maxRetries = Number(byId(\"influxdb-sink-max-retries\")?.value || 3);")
                .contains("\"influxdb-sink-url\"")
                .contains("\"influxdb-sink-database\"")
                .contains("\"influxdb-sink-measurement\"");

        assertThat(template)
                .contains("data-operator-key=\"influxDbSink\"")
                .contains("id=\"influxdb-sink-config-section\"")
                .contains("id=\"influxdb-sink-url\"")
                .contains("id=\"influxdb-sink-database\"")
                .contains("id=\"influxdb-sink-measurement\"")
                .contains("id=\"influxdb-sink-key-tags\"")
                .contains("id=\"influxdb-sink-fields\"");
    }

    @Test
    void scriptPersistsHdfsFileSinkConfigAndTogglesSinkSections() throws Exception {
        String script = loadScript();
        String template = loadTemplate();

        assertThat(script)
                .contains("operator: \"HDFS_FILE_SINK\"")
                .contains("defaultName: t(\"studio.operator.hdfsFileSink\", \"HDFS File Sink\")")
                .contains("byId(\"hdfs-file-sink-config-section\")?.classList.toggle(\"hidden\", operator !== \"HDFS_FILE_SINK\");")
                .contains("function updateHdfsFileSinkFilenameUI(")
                .contains("selectedNode.config[\"fs.defaultFS\"] = byId(\"hdfs-file-sink-default-fs\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.path = byId(\"hdfs-file-sink-path\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.tmp_path = byId(\"hdfs-file-sink-tmp-path\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.file_format_type = byId(\"hdfs-file-sink-format-type\")?.value || \"JSON\";")
                .contains("selectedNode.config.sink_columns = splitTopics(byId(\"hdfs-file-sink-columns\")?.value || \"\");")
                .contains("selectedNode.config.partition_by = splitTopics(byId(\"hdfs-file-sink-partition-by\")?.value || \"\");")
                .contains("selectedNode.config.batch_size = Number(byId(\"hdfs-file-sink-batch-size\")?.value || 1000);")
                .contains("\"hdfs-file-sink-default-fs\"")
                .contains("\"hdfs-file-sink-path\"")
                .contains("\"hdfs-file-sink-format-type\"");

        assertThat(template)
                .contains("data-operator-key=\"hdfsFileSink\"")
                .contains("id=\"hdfs-file-sink-config-section\"")
                .contains("id=\"hdfs-file-sink-default-fs\"")
                .contains("id=\"hdfs-file-sink-path\"")
                .contains("id=\"hdfs-file-sink-format-type\"")
                .contains("id=\"hdfs-file-sink-columns\"");
    }

    @Test
    void scriptStoresRenameMappingsViaRepeatableInputRows() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("function normalizeRenameMappings(")
                .contains("function renderRenameMappingInputs(")
                .contains("function collectRenameMappingInputs(")
                .contains("function removeRenameMappingInput(")
                .contains("function appendRenameMappingInput(")
                .contains("selectedNode.config.mapping = collectRenameMappingInputs();")
                .contains("renderRenameMappingInputs(selectedNode.config.mapping || {});")
                .contains("byId(\"add-rename-mapping-button\")?.addEventListener(\"click\", () => {")
                .contains("byId(\"rename-mapping-list\")?.addEventListener(\"input\", event => {")
                .contains("byId(\"rename-mapping-list\")?.addEventListener(\"click\", event => {")
                .contains("event.target.matches(\"[data-role='rename-source-field']\")")
                .contains("event.target.closest(\"[data-role='remove-rename-mapping']\")")
                .contains("function validateRenameConfig(")
                .contains("RENAME: validateRenameConfig")
                .contains("studio.validation.rename.mappingRequired")
                .doesNotContain("parseJsonValue(byId(\"rename-mapping\")?.value || \"{}\", {})")
                .doesNotContain("byId(\"rename-mapping\").value = JSON.stringify");
    }

    @Test
    void scriptPersistsStructuredPutValueConfig() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"PUT\"")
                .contains("defaultConfig: { field: \"\", valueMode: \"LITERAL\", value: \"\", referenceField: \"\", template: \"\", note: \"\" }")
                .contains("function inferPutValueMode(")
                .contains("function putReferenceField(")
                .contains("function putStoredValue(")
                .contains("function updatePutValueModeUI(")
                .contains("selectedNode.config.valueMode = byId(\"put-value-mode\")?.value || \"LITERAL\";")
                .contains("selectedNode.config.referenceField = byId(\"put-reference-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.template = byId(\"put-template-value\")?.value || \"\";")
                .contains("selectedNode.config.value = putStoredValue(selectedNode.config.valueMode,")
                .contains("const putValueMode = selectedNode.config.valueMode || inferPutValueMode(selectedNode.config.value || \"\");")
                .contains("byId(\"put-value-mode\").value = putValueMode;")
                .contains("byId(\"put-reference-field\").value = selectedNode.config.referenceField || putReferenceField(selectedNode.config.value || \"\");")
                .contains("byId(\"put-template-value\").value = selectedNode.config.template || selectedNode.config.value || \"\";")
                .contains("updatePutValueModeUI(putValueMode);")
                .contains("\"put-value-mode\"")
                .contains("\"put-literal-value\"")
                .contains("\"put-reference-field\"")
                .contains("\"put-template-value\"")
                .contains("byId(\"put-value-mode\")?.addEventListener(\"change\", () => {")
                .doesNotContain("selectedNode.config.value = byId(\"put-value\")?.value?.trim?.() || \"\";")
                .doesNotContain("\"put-value\"");
    }

    @Test
    void scriptPersistsStructuredPruneFieldsConfig() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("defaultConfig: { fields: [], note: \"\" }")
                .contains("function normalizePruneFields(")
                .contains("function renderPruneFieldInputs(")
                .contains("function collectPruneFieldInputs(")
                .contains("function removePruneFieldInput(")
                .contains("function appendPruneFieldInput(")
                .contains("selectedNode.config.fields = collectPruneFieldInputs();")
                .contains("renderPruneFieldInputs(selectedNode.config.fields || []);")
                .contains("byId(\"add-prune-field-button\")?.addEventListener(\"click\", () => {")
                .contains("byId(\"prune-field-list\")?.addEventListener(\"input\", event => {")
                .contains("byId(\"prune-field-list\")?.addEventListener(\"click\", event => {")
                .contains("event.target.matches(\"[data-role='prune-field-name']\")")
                .contains("event.target.closest(\"[data-role='remove-prune-field']\")")
                .doesNotContain("splitTopics(byId(\"prune-fields\")?.value || \"\")")
                .doesNotContain("byId(\"prune-fields\").value = (selectedNode.config.fields || []).join(\",\")")
                .doesNotContain("\"prune-fields\"");
    }

    @Test
    void scriptPersistsDeserializeTargetFieldConfig() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("defaultConfig: { field: \"\", targetField: \"\", format: \"JSON\", fieldNames: [], delimiter: \",\", note: \"\" }")
                .contains("selectedNode.config.targetField = byId(\"deserialize-target-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.field = byId(\"deserialize-field\")?.value?.trim?.() || \"\";")
                .contains("if (byId(\"deserialize-target-field\")) {")
                .contains("byId(\"deserialize-target-field\").value = selectedNode.config.targetField || \"\";")
                .contains("byId(\"deserialize-field\").value = selectedNode.config.field || \"\";")
                .contains("\"deserialize-target-field\"");
    }

    @Test
    void scriptPersistsTransformSerdeMultiFormatConfig() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("defaultConfig: { field: \"\", targetField: \"\", format: \"JSON\", fieldNames: [], delimiter: \",\", note: \"\" }")
                .contains("defaultConfig: { sourceFields: [], targetField: \"\", format: \"JSON\", delimiter: \",\", note: \"\" }")
                .contains("selectedNode.config.format = byId(\"deserialize-format\")?.value || \"JSON\";")
                .contains("selectedNode.config.fieldNames = splitTopics(byId(\"deserialize-field-names\")?.value || \"\");")
                .contains("selectedNode.config.delimiter = byId(\"deserialize-delimiter\")?.value || \",\";")
                .contains("selectedNode.config.sourceFields = splitTopics(byId(\"serialize-source-fields\")?.value || \"\");")
                .contains("selectedNode.config.targetField = byId(\"serialize-target-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.format = byId(\"serialize-format\")?.value || \"JSON\";")
                .contains("selectedNode.config.delimiter = byId(\"serialize-delimiter\")?.value || \",\";")
                .contains("byId(\"serialize-source-fields\").value = (selectedNode.config.sourceFields || []).join(\",\");")
                .contains("byId(\"serialize-target-field\").value = selectedNode.config.targetField || \"\";")
                .contains("byId(\"deserialize-format\").value = selectedNode.config.format || \"JSON\";")
                .contains("byId(\"deserialize-field-names\").value = (selectedNode.config.fieldNames || []).join(\",\");")
                .contains("byId(\"deserialize-delimiter\").value = selectedNode.config.delimiter || \",\";")
                .contains("byId(\"serialize-format\").value = selectedNode.config.format || \"JSON\";")
                .contains("byId(\"serialize-delimiter\").value = selectedNode.config.delimiter || \",\";")
                .contains("function updateDeserializeFormatUI(")
                .contains("byId(\"deserialize-field-names-wrapper\")?.classList.toggle(\"hidden\", format !== \"CSV\");")
                .contains("byId(\"deserialize-format\")?.addEventListener(\"change\", () => {");
    }

    @Test
    void scriptPersistsGrokCastAndEvalConfigs() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"GROK\"")
                .contains("runnableInRuntime: true")
                .contains("defaultConfig: { inputField: \"\", outputField: \"\", pattern: \"\", note: \"\" }")
                .contains("selectedNode.config.inputField = byId(\"grok-input-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.outputField = byId(\"grok-output-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.pattern = byId(\"grok-pattern\")?.value || \"\";")
                .contains("byId(\"grok-config\")?.classList.remove(\"hidden\");")
                .contains("byId(\"grok-input-field\").value = selectedNode.config.inputField || \"\";")
                .contains("byId(\"grok-output-field\").value = selectedNode.config.outputField || \"\";")
                .contains("byId(\"grok-pattern\").value = selectedNode.config.pattern || \"\";")
                .contains("operator: \"CAST\"")
                .contains("defaultConfig: { inputField: \"\", outputMode: \"OVERWRITE\", outputField: \"\", targetType: \"STRING\", note: \"\" }")
                .contains("function inferCastOutputMode(")
                .contains("function castStoredOutputField(")
                .contains("function updateCastOutputModeUI(")
                .contains("selectedNode.config.inputField = byId(\"cast-input-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.outputMode = byId(\"cast-output-mode\")?.value || \"OVERWRITE\";")
                .contains("selectedNode.config.outputField = castStoredOutputField(")
                .contains("selectedNode.config.targetType = byId(\"cast-target-type\")?.value || \"STRING\";")
                .contains("byId(\"cast-config\")?.classList.remove(\"hidden\");")
                .contains("const castOutputMode = selectedNode.config.outputMode || inferCastOutputMode(")
                .contains("byId(\"cast-output-mode\").value = castOutputMode;")
                .contains("byId(\"cast-target-type\").value = selectedNode.config.targetType || \"STRING\";")
                .contains("refreshStudioSelectValue(\"cast-target-type\");")
                .contains("updateCastOutputModeUI(castOutputMode);")
                .contains("operator: \"EVAL\"")
                .contains("defaultName: t(\"studio.operator.eval\", \"Field calculation\")")
                .contains("defaultConfig: { targetField: \"\", expression: \"\", outputMode: \"OVERWRITE\", errorStrategy: \"KEEP_ORIGINAL\", note: \"\" }")
                .contains("selectedNode.config.targetField = byId(\"eval-target-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.expression = byId(\"eval-expression\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.outputMode = byId(\"eval-output-mode\")?.value || \"OVERWRITE\";")
                .contains("selectedNode.config.errorStrategy = byId(\"eval-error-strategy\")?.value || \"KEEP_ORIGINAL\";")
                .contains("byId(\"eval-config\")?.classList.remove(\"hidden\");")
                .contains("byId(\"eval-target-field\").value = selectedNode.config.targetField || \"\";")
                .contains("byId(\"eval-expression\").value = selectedNode.config.expression || \"\";")
                .contains("byId(\"eval-output-mode\").value = selectedNode.config.outputMode || \"OVERWRITE\";")
                .contains("byId(\"eval-error-strategy\").value = selectedNode.config.errorStrategy || \"KEEP_ORIGINAL\";")
                .contains("\"grok-input-field\"")
                .contains("\"grok-output-field\"")
                .contains("\"grok-pattern\"")
                .contains("\"cast-input-field\"")
                .contains("\"cast-output-mode\"")
                .contains("\"cast-output-field\"")
                .contains("\"cast-target-type\"")
                .contains("\"eval-target-field\"")
                .contains("\"eval-expression\"")
                .contains("\"eval-output-mode\"")
                .contains("\"eval-error-strategy\"")
                .contains("byId(\"cast-output-mode\")?.addEventListener(\"change\", () => {")
                .doesNotContain("selectedNode.config.outputField = byId(\"cast-output-field\")?.value?.trim?.() || \"\";");
    }

    @Test
    void templateProvidesAggregateInspectorControls() throws Exception {
        String template = loadTemplate();

        assertThat(template)
                .contains("id=\"aggregate-config\"")
                .contains("id=\"aggregate-mode\"")
                .contains("id=\"aggregate-group-by\"")
                .contains("id=\"aggregate-window-type\"")
                .contains("id=\"aggregate-time-mode\"")
                .contains("id=\"aggregate-time-unit\"")
                .contains("id=\"aggregate-window-size\"")
                .contains("id=\"aggregate-window-slide\"")
                .contains("id=\"aggregate-watermark-delay\"")
                .contains("id=\"aggregate-count-window-size\"")
                .contains("id=\"aggregate-item-list\"")
                .contains("id=\"aggregate-item-template\"")
                .contains("id=\"add-aggregate-item-button\"");
    }

    @Test
    void scriptPersistsAggregateConfig() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"AGGREGATE\"")
                .contains("defaultName: t(\"studio.operator.aggregate\", \"Aggregate\")")
                .contains("defaultConfig: { mode: \"GLOBAL\", groupBy: [], windowType: \"TUMBLING_TIME\", timeMode: \"PROCESSING_TIME\", timeUnit: \"SECONDS\", windowSize: 60, windowSlide: 10, watermarkDelay: 30, eventTimeField: \"\", eventTimeUnit: \"MILLISECONDS\", outputMode: \"NESTED\", windowStartField: \"windowStart\", windowEndField: \"windowEnd\", countWindowSize: 100, aggregations: [{ function: \"COUNT\", field: \"\", outputField: \"count\" }], note: \"\" }")
                .contains("function normalizeAggregateItems(")
                .contains("function renderAggregateItems(")
                .contains("function collectAggregateItems(")
                .contains("function updateAggregateItemVisibility(")
                .contains("[data-role='aggregate-field-wrapper']")
                .contains("[data-role='aggregate-sort-field-wrapper']")
                .contains("[data-role='aggregate-sort-direction-wrapper']")
                .contains("functionValue === \"COUNT\"")
                .contains("function updateAggregateConfigUI(")
                .contains("selectedNode.config.mode = byId(\"aggregate-mode\")?.value || \"GLOBAL\";")
                .contains("selectedNode.config.groupBy = splitTopics(byId(\"aggregate-group-by\")?.value || \"\");")
                .contains("selectedNode.config.windowType = byId(\"aggregate-window-type\")?.value || \"TUMBLING_TIME\";")
                .contains("selectedNode.config.timeMode = byId(\"aggregate-time-mode\")?.value || \"PROCESSING_TIME\";")
                .contains("selectedNode.config.timeUnit = byId(\"aggregate-time-unit\")?.value || \"SECONDS\";")
                .contains("selectedNode.config.windowSize = Number(byId(\"aggregate-window-size\")?.value || 60);")
                .contains("selectedNode.config.windowSlide = Number(byId(\"aggregate-window-slide\")?.value || 10);")
                .contains("selectedNode.config.watermarkDelay = Number(byId(\"aggregate-watermark-delay\")?.value || 30);")
                .contains("selectedNode.config.eventTimeField = byId(\"aggregate-event-time-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.eventTimeUnit = byId(\"aggregate-event-time-unit\")?.value || \"MILLISECONDS\";")
                .contains("selectedNode.config.outputMode = byId(\"aggregate-output-mode\")?.value || \"NESTED\";")
                .contains("selectedNode.config.windowStartField = byId(\"aggregate-window-start-field\")?.value?.trim?.() || \"windowStart\";")
                .contains("selectedNode.config.windowEndField = byId(\"aggregate-window-end-field\")?.value?.trim?.() || \"windowEnd\";")
                .contains("selectedNode.config.countWindowSize = Number(byId(\"aggregate-count-window-size\")?.value || 100);")
                .contains("selectedNode.config.aggregations = collectAggregateItems();")
                .contains("byId(\"aggregate-config\")?.classList.remove(\"hidden\");")
                .contains("renderAggregateItems(selectedNode.config.aggregations || []);")
                .contains("updateAggregateConfigUI(selectedNode.config.mode || \"GLOBAL\", selectedNode.config.windowType || \"TUMBLING_TIME\", selectedNode.config.timeMode || \"PROCESSING_TIME\");")
                .contains("\"aggregate-mode\"")
                .contains("\"aggregate-group-by\"")
                .contains("\"aggregate-window-type\"")
                .contains("\"aggregate-time-mode\"")
                .contains("\"aggregate-time-unit\"")
                .contains("\"aggregate-window-size\"")
                .contains("\"aggregate-window-slide\"")
                .contains("\"aggregate-watermark-delay\"")
                .contains("\"aggregate-event-time-field\"")
                .contains("\"aggregate-event-time-unit\"")
                .contains("\"aggregate-output-mode\"")
                .contains("\"aggregate-window-start-field\"")
                .contains("\"aggregate-window-end-field\"")
                .contains("\"aggregate-count-window-size\"")
                .contains("byId(\"add-aggregate-item-button\")?.addEventListener(\"click\", () => {")
                .contains("byId(\"aggregate-item-list\")?.addEventListener(\"input\", event => {")
                .contains("byId(\"aggregate-item-list\")?.addEventListener(\"change\", event => {")
                .contains("byId(\"aggregate-item-list\")?.addEventListener(\"click\", event => {")
                .contains("event.target.matches(\"[data-role='aggregate-function']\")")
                .contains("event.target.matches(\"[data-role='aggregate-field']\")")
                .contains("event.target.matches(\"[data-role='aggregate-sort-field']\")")
                .contains("event.target.matches(\"[data-role='aggregate-output-field']\")")
                .contains("event.target.closest(\"[data-role='remove-aggregate-item']\")");
    }

    @Test
    void scriptPersistsAndValidatesDeduplicateConfig() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"DEDUPLICATE\"")
                .contains("defaultName: t(\"studio.operator.deduplicate\", \"Deduplicate\")")
                .contains("defaultConfig: {")
                .contains("eventTimeField: \"\"")
                .contains("windowSeconds: 300")
                .contains("watermarkDelaySeconds: 30")
                .contains("lateDataStrategy: \"DISCARD\"")
                .contains("selectedNode.config.keyFields = splitTopics(byId(\"deduplicate-key-fields\")?.value || \"\");")
                .contains("selectedNode.config.timeMode = byId(\"deduplicate-time-mode\")?.value || \"PROCESSING_TIME\";")
                .contains("selectedNode.config.ttlSeconds = Number(byId(\"deduplicate-ttl-seconds\")?.value || 3600);")
                .contains("selectedNode.config.eventTimeField = byId(\"deduplicate-event-time-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.windowSeconds = Number(byId(\"deduplicate-window-seconds\")?.value || 300);")
                .contains("selectedNode.config.watermarkDelaySeconds = Number(byId(\"deduplicate-watermark-delay-seconds\")?.value || 30);")
                .contains("selectedNode.config.keepStrategy = byId(\"deduplicate-keep-strategy\")?.value || \"FIRST\";")
                .contains("selectedNode.config.lateDataStrategy = byId(\"deduplicate-late-data-strategy\")?.value || \"DISCARD\";")
                .contains("selectedNode.config.duplicateStrategy = byId(\"deduplicate-duplicate-strategy\")?.value || \"DISCARD\";")
                .contains("byId(\"deduplicate-config\")?.classList.remove(\"hidden\");")
                .contains("byId(\"deduplicate-key-fields\").value = (selectedNode.config.keyFields || []).join(\",\");")
                .contains("byId(\"deduplicate-time-mode\").value = selectedNode.config.timeMode || \"PROCESSING_TIME\";")
                .contains("byId(\"deduplicate-ttl-seconds\").value = selectedNode.config.ttlSeconds || 3600;")
                .contains("byId(\"deduplicate-event-time-field\").value = selectedNode.config.eventTimeField || \"\";")
                .contains("byId(\"deduplicate-window-seconds\").value = selectedNode.config.windowSeconds || 300;")
                .contains("byId(\"deduplicate-watermark-delay-seconds\").value = selectedNode.config.watermarkDelaySeconds ?? 30;")
                .contains("byId(\"deduplicate-keep-strategy\").value = selectedNode.config.keepStrategy || \"FIRST\";")
                .contains("byId(\"deduplicate-late-data-strategy\").value = selectedNode.config.lateDataStrategy || \"DISCARD\";")
                .contains("byId(\"deduplicate-duplicate-strategy\").value = selectedNode.config.duplicateStrategy || \"DISCARD\";")
                .contains("updateDeduplicateTimeModeUI(selectedNode.config.timeMode || \"PROCESSING_TIME\");")
                .contains("validateDeduplicateConfig(node)")
                .contains("validateDeduplicateConfigs(definition)")
                .contains("studio.validation.deduplicate.keyFieldsRequired")
                .contains("studio.validation.deduplicate.ttlSecondsPositive")
                .contains("studio.validation.deduplicate.ttlSecondsInteger")
                .contains("studio.validation.deduplicate.eventTimeFieldRequired")
                .contains("studio.validation.deduplicate.windowSecondsPositive")
                .contains("studio.validation.deduplicate.watermarkDelaySecondsNonNegative")
                .contains("studio.validation.deduplicate.keepStrategyUnsupported")
                .contains("\"deduplicate-key-fields\"")
                .contains("\"deduplicate-time-mode\"")
                .contains("\"deduplicate-ttl-seconds\"")
                .contains("\"deduplicate-event-time-field\"")
                .contains("\"deduplicate-window-seconds\"")
                .contains("\"deduplicate-watermark-delay-seconds\"")
                .contains("\"deduplicate-keep-strategy\"")
                .contains("\"deduplicate-late-data-strategy\"")
                .contains("\"deduplicate-duplicate-strategy\"")
                .contains("operator: \"LOOKUP_ENRICH\"")
                .contains("defaultName: t(\"studio.operator.lookupEnrich\", \"Dimension enrichment\")")
                .contains("defaultConfig: {")
                .contains("entries: [{ key: \"\", value: \"\", valueType: \"STRING\" }]")
                .contains("function lookupEnrichEntryValueType(")
                .contains("function lookupEnrichEntryDisplayValue(")
                .contains("function lookupEnrichTypedValue(")
                .contains("valueType: item.querySelector(\"[data-role='lookup-enrich-entry-value-type']\")?.value || \"STRING\"")
                .contains("selectedNode.config.sourceField = byId(\"lookup-enrich-source-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.targetField = byId(\"lookup-enrich-target-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.entries = collectLookupEnrichEntries();")
                .contains("selectedNode.config.missingStrategy = byId(\"lookup-enrich-missing-strategy\")?.value || \"KEEP_ORIGINAL\";")
                .contains("selectedNode.config.overwriteTargetField = Boolean(byId(\"lookup-enrich-overwrite-target-field\")?.checked);")
                .contains("byId(\"lookup-enrich-config\")?.classList.remove(\"hidden\");")
                .contains("renderLookupEnrichEntries(selectedNode.config.entries || []);")
                .contains("validateLookupEnrichConfig(node)")
                .contains("validateLookupEnrichConfigs(definition)")
                .contains("studio.validation.lookupEnrich.sourceFieldRequired")
                .contains("studio.validation.lookupEnrich.targetFieldRequired")
                .contains("studio.validation.lookupEnrich.entriesRequired")
                .contains("studio.validation.lookupEnrich.entryRequired")
                .contains("studio.validation.lookupEnrich.duplicateKey")
                .contains("\"lookup-enrich-source-field\"")
                .contains("\"lookup-enrich-target-field\"")
                .contains("\"lookup-enrich-missing-strategy\"")
                .contains("\"lookup-enrich-overwrite-target-field\"")
                .contains("\"lookup-enrich-entry-list\"")
                .contains("[data-role='lookup-enrich-entry-value-type']");
    }

    @Test
    void scriptPersistsAndValidatesFlattenAndExplodeConfigs() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"FLATTEN\"")
                .contains("defaultName: t(\"studio.operator.flatten\", \"Flatten\")")
                .contains("defaultConfig: { sourceField: \"\", targetPrefix: \"\", delimiter: \"_\", removeSourceField: false, note: \"\" }")
                .contains("selectedNode.config.sourceField = byId(\"flatten-source-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.targetPrefix = byId(\"flatten-target-prefix\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.delimiter = byId(\"flatten-delimiter\")?.value || \"_\";")
                .contains("selectedNode.config.removeSourceField = Boolean(byId(\"flatten-remove-source-field\")?.checked);")
                .contains("byId(\"flatten-config\")?.classList.remove(\"hidden\");")
                .contains("byId(\"flatten-source-field\").value = selectedNode.config.sourceField || \"\";")
                .contains("byId(\"flatten-target-prefix\").value = selectedNode.config.targetPrefix || \"\";")
                .contains("byId(\"flatten-delimiter\").value = selectedNode.config.delimiter || \"_\";")
                .contains("byId(\"flatten-remove-source-field\").checked = Boolean(selectedNode.config.removeSourceField);")
                .contains("operator: \"EXPLODE\"")
                .contains("defaultName: t(\"studio.operator.explode\", \"Explode\")")
                .contains("defaultConfig: { sourceField: \"\", targetField: \"\", keepEmpty: false, note: \"\" }")
                .contains("selectedNode.config.sourceField = byId(\"explode-source-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.targetField = byId(\"explode-target-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.keepEmpty = Boolean(byId(\"explode-keep-empty\")?.checked);")
                .contains("byId(\"explode-config\")?.classList.remove(\"hidden\");")
                .contains("byId(\"explode-source-field\").value = selectedNode.config.sourceField || \"\";")
                .contains("byId(\"explode-target-field\").value = selectedNode.config.targetField || \"\";")
                .contains("byId(\"explode-keep-empty\").checked = Boolean(selectedNode.config.keepEmpty);")
                .contains("validateFlattenConfig(node)")
                .contains("validateExplodeConfig(node)")
                .contains("validateFlattenConfigs(definition)")
                .contains("validateExplodeConfigs(definition)")
                .contains("studio.validation.flatten.sourceFieldRequired")
                .contains("studio.validation.flatten.delimiterRequired")
                .contains("studio.validation.explode.sourceFieldRequired")
                .contains("studio.validation.explode.targetFieldRequired")
                .contains("\"flatten-source-field\"")
                .contains("\"flatten-target-prefix\"")
                .contains("\"flatten-delimiter\"")
                .contains("\"flatten-remove-source-field\"")
                .contains("\"explode-source-field\"")
                .contains("\"explode-target-field\"")
                .contains("\"explode-keep-empty\"");
    }

    @Test
    void scriptPersistsAndValidatesDataQualityConfig() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"DATA_QUALITY\"")
                .contains("defaultName: t(\"studio.operator.dataQuality\", \"Data validation\")")
                .contains("defaultConfig: { mode: \"DIRTY_PORT\", errorField: \"_streamcraft_quality_errors\", rules:")
                .contains("function normalizeDataQualityRules(")
                .contains("function renderDataQualityRules(")
                .contains("function collectDataQualityRules(")
                .contains("function appendDataQualityRule(")
                .contains("function removeDataQualityRule(")
                .contains("function updateDataQualityRuleUI(")
                .contains("function updateDataQualityModeUI(")
                .contains("selectedNode.config.mode = byId(\"data-quality-mode\")?.value || \"DIRTY_PORT\";")
                .contains("selectedNode.config.errorField = byId(\"data-quality-error-field\")?.value?.trim?.() || \"_streamcraft_quality_errors\";")
                .contains("selectedNode.config.rules = collectDataQualityRules().map(rule =>")
                .contains("ruleType: rule.ruleType || \"NOT_NULL\"")
                .contains("nextRule.valueType = rule.valueType || \"STRING\";")
                .contains("nextRule.minLength = Number(rule.minLength);")
                .contains("nextRule.enumValues = splitTopics(rule.enumValues || \"\");")
                .contains("byId(\"data-quality-config\")?.classList.remove(\"hidden\");")
                .contains("updateDataQualityModeUI(selectedNode.config.mode || \"DIRTY_PORT\");")
                .contains("renderDataQualityRules(selectedNode.config.rules || []);")
                .contains("validateDataQualityConfig(node)")
                .contains("validateDataQualityConfigs(definition)")
                .contains("studio.validation.dataQuality.rulesRequired")
                .contains("studio.validation.dataQuality.ruleFieldRequired")
                .contains("studio.validation.dataQuality.ruleTypeRequired")
                .contains("studio.validation.dataQuality.invalidPattern")
                .contains("\"data-quality-mode\"")
                .contains("\"data-quality-error-field\"")
                .contains("\"data-quality-rule-list\"")
                .contains("byId(\"add-data-quality-rule-button\")?.addEventListener(\"click\", () => {")
                .contains("byId(\"data-quality-rule-list\")?.addEventListener(\"input\", event => {")
                .contains("byId(\"data-quality-rule-list\")?.addEventListener(\"change\", event => {")
                .contains("byId(\"data-quality-rule-list\")?.addEventListener(\"click\", event => {")
                .contains("event.target.matches(\"[data-role='data-quality-rule-field']\")")
                .contains("event.target.matches(\"[data-role='data-quality-rule-kind']\")")
                .contains("event.target.matches(\"[data-role='data-quality-rule-value-type']\")")
                .contains("event.target.matches(\"[data-role='data-quality-rule-message']\")")
                .contains("event.target.closest(\"[data-role='remove-data-quality-rule']\")")
                .doesNotContain("studio.validation.dataQuality.duplicateField")
                .doesNotContain("rule fields must be unique");
    }

    @Test
    void scriptPersistsAndValidatesStructuredTimeDeriveConfig() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"TIME_DERIVE\"")
                .contains("defaultName: t(\"studio.operator.timeDerive\", \"Time derive\")")
                .contains("defaultConfig: { sourceField: \"\", sourceFormat: \"AUTO\", sourcePattern: \"\", sourceTimeZone: \"UTC\", outputTimeZone: \"UTC\", parseErrorStrategy: \"KEEP_ORIGINAL\", derivations:")
                .contains("function normalizeTimeDeriveItems(")
                .contains("function renderTimeDeriveItems(")
                .contains("function collectTimeDeriveItems(")
                .contains("function appendTimeDeriveItem(")
                .contains("function removeTimeDeriveItem(")
                .contains("function moveTimeDeriveItem(")
                .contains("selectedNode.config.sourcePattern = byId(\"time-derive-source-pattern\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.sourceTimeZone = byId(\"time-derive-source-time-zone\")?.value?.trim?.() || \"UTC\";")
                .contains("selectedNode.config.outputTimeZone = byId(\"time-derive-output-time-zone\")?.value?.trim?.() || \"UTC\";")
                .contains("selectedNode.config.parseErrorStrategy = byId(\"time-derive-parse-error-strategy\")?.value || \"KEEP_ORIGINAL\";")
                .contains("selectedNode.config.derivations = collectTimeDeriveItems();")
                .contains("renderTimeDeriveItems(selectedNode.config.derivations || []);")
                .contains("studio.validation.timeDerive.outputFieldRequired")
                .contains("studio.validation.timeDerive.duplicateOutputField")
                .contains("studio.validation.timeDerive.patternRequired")
                .doesNotContain("\"time-derive-derivations\"");
    }

    @Test
    void scriptPersistsAndValidatesLookupJoinConfig() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"LOOKUP_JOIN\"")
                .contains("defaultName: t(\"studio.operator.lookupJoin\", \"Dimension join\")")
                .contains("selectedNode.config.sourceField = byId(\"lookup-join-source-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.targetField = byId(\"lookup-join-target-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.joinType = byId(\"lookup-join-type\")?.value || \"LEFT\";")
                .contains("selectedNode.config.missingStrategy = byId(\"lookup-join-missing-strategy\")?.value || \"KEEP_ORIGINAL\";")
                .contains("selectedNode.config.overwriteTargetField = Boolean(byId(\"lookup-join-overwrite-target-field\")?.checked);")
                .contains("selectedNode.config.entries = collectLookupJoinEntries();")
                .contains("function normalizeLookupJoinEntries(")
                .contains("function renderLookupJoinEntries(")
                .contains("function collectLookupJoinEntries(")
                .contains("function appendLookupJoinEntry(")
                .contains("function removeLookupJoinEntry(")
                .contains("function appendLookupJoinField(")
                .contains("function removeLookupJoinField(")
                .contains("function updateLookupJoinTypeUI(")
                .contains("byId(\"lookup-join-config\")?.classList.remove(\"hidden\");")
                .contains("renderLookupJoinEntries(selectedNode.config.entries || []);")
                .contains("updateLookupJoinTypeUI(selectedNode.config.joinType || \"LEFT\");")
                .contains("validateLookupJoinConfig(node)")
                .contains("validateLookupJoinConfigs(definition)")
                .contains("studio.validation.lookupJoin.sourceFieldRequired")
                .contains("studio.validation.lookupJoin.targetFieldRequired")
                .contains("studio.validation.lookupJoin.entriesRequired")
                .contains("studio.validation.lookupJoin.entryRequired")
                .contains("studio.validation.lookupJoin.duplicateKey")
                .contains("\"lookup-join-source-field\"")
                .contains("\"lookup-join-target-field\"")
                .contains("\"lookup-join-type\"")
                .contains("\"lookup-join-missing-strategy\"")
                .contains("\"lookup-join-overwrite-target-field\"")
                .contains("\"lookup-join-entry-list\"")
                .contains("byId(\"add-lookup-join-entry-button\")?.addEventListener(\"click\", () => {")
                .contains("byId(\"lookup-join-entry-list\")?.addEventListener(\"input\", event => {")
                .contains("byId(\"lookup-join-entry-list\")?.addEventListener(\"change\", event => {")
                .contains("byId(\"lookup-join-entry-list\")?.addEventListener(\"click\", event => {")
                .contains("event.target.closest(\"[data-role='add-lookup-join-field']\")")
                .contains("event.target.closest(\"[data-role='remove-lookup-join-field']\")")
                .contains("event.target.closest(\"[data-role='remove-lookup-join-entry']\")")
                .doesNotContain("parseJsonValue(byId(\"lookup-join-entries\")?.value || \"[]\", [])")
                .doesNotContain("\"lookup-join-entries\"");
    }

    @Test
    void scriptPersistsAndValidatesStreamJoinConfigAndRendersDualInputPorts() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("operator: \"STREAM_JOIN\"")
                .contains("defaultName: t(\"studio.operator.streamJoin\", \"Two-stream join\")")
                .contains("defaultConfig: {")
                .contains("leftKeyField: \"\"")
                .contains("rightKeyField: \"\"")
                .contains("windowBefore: 60")
                .contains("windowAfter: 60")
                .contains("lateDataStrategy: \"DROP\"")
                .contains("selectedNode.config.leftKeyField = byId(\"stream-join-left-key-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.rightKeyField = byId(\"stream-join-right-key-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.targetField = byId(\"stream-join-target-field\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.joinType = byId(\"stream-join-type\")?.value || \"LEFT\";")
                .contains("selectedNode.config.timeMode = byId(\"stream-join-time-mode\")?.value || \"PROCESSING_TIME\";")
                .contains("selectedNode.config.timeUnit = byId(\"stream-join-time-unit\")?.value || \"SECONDS\";")
                .contains("selectedNode.config.windowBefore = Number(byId(\"stream-join-window-before\")?.value || 60);")
                .contains("selectedNode.config.windowAfter = Number(byId(\"stream-join-window-after\")?.value || 60);")
                .contains("selectedNode.config.watermarkDelay = Number(byId(\"stream-join-watermark-delay\")?.value || 30);")
                .contains("selectedNode.config.lateDataStrategy = byId(\"stream-join-late-data-strategy\")?.value || \"DROP\";")
                .contains("byId(\"stream-join-config\")?.classList.remove(\"hidden\");")
                .contains("byId(\"stream-join-late-data-strategy\").value = selectedNode.config.lateDataStrategy || \"DROP\";")
                .contains("updateStreamJoinConfigUI(selectedNode.config.joinType || \"LEFT\", selectedNode.config.timeMode || \"PROCESSING_TIME\");")
                .contains("function updateStreamJoinConfigUI(joinType, timeMode)")
                .contains("byId(\"stream-join-missing-strategy-wrapper\")?.classList.toggle(\"hidden\", joinType === \"INNER\");")
                .contains("byId(\"stream-join-watermark-delay-wrapper\")?.classList.toggle(\"hidden\", timeMode !== \"EVENT_TIME\");")
                .contains("validateStreamJoinConfig(node, definition)")
                .contains("validateStreamJoinConfigs(definition)")
                .contains("studio.validation.streamJoin.leftKeyFieldRequired")
                .contains("studio.validation.streamJoin.inputPortsRequired")
                .contains("node.operator === \"STREAM_JOIN\"")
                .contains("id: \"left\", label: \"\"")
                .contains("id: \"right\", label: \"\"")
                .doesNotContain("label: t(\"studio.port.left\", \"L\")")
                .doesNotContain("label: t(\"studio.port.right\", \"R\")")
                .contains("port-left-stack")
                .contains("\"stream-join-left-key-field\"")
                .contains("\"stream-join-right-key-field\"")
                .contains("\"stream-join-target-field\"")
                .contains("\"stream-join-type\"")
                .contains("\"stream-join-time-mode\"")
                .contains("\"stream-join-time-unit\"")
                .contains("\"stream-join-window-before\"")
                .contains("\"stream-join-window-after\"")
                .contains("\"stream-join-watermark-delay\"")
                .contains("\"stream-join-late-data-strategy\"");
    }

    @Test
    void scriptBindsStreamJoinSegmentedControlsToExistingSelectState() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("function bindSegmentedControls()")
                .contains("function syncSegmentedControlValue(selectId)")
                .contains("data-segmented-control")
                .contains("data-segmented-value")
                .contains("select.dispatchEvent(new Event(\"input\", { bubbles: true }));")
                .contains("select.dispatchEvent(new Event(\"change\", { bubbles: true }));")
                .contains("syncSegmentedControlValue(id);")
                .contains("bindSegmentedControls();");
    }

    @Test
    void scriptValidatesAggregateConfigBeforeSavePreviewAndRun() throws Exception {
        String script = loadScript();

        assertThat(script)
                .contains("function validateAggregateConfig(node)")
                .contains("function validateAggregateConfigs(definition)")
                .contains("const aggregateValidation = validateAggregateConfigs(definition);")
                .contains("if (!aggregateValidation.ok)")
                .contains("return aggregateValidation;")
                .contains("config.mode === \"GROUPED\" && (!Array.isArray(config.groupBy) || config.groupBy.length === 0)")
                .contains("studio.validation.aggregate.groupByRequired")
                .contains("!Array.isArray(config.aggregations) || config.aggregations.length === 0")
                .contains("studio.validation.aggregate.aggregationsRequired")
                .contains("aggregation.function !== \"COUNT\" && !aggregation.field")
                .contains("studio.validation.aggregate.aggregationFieldRequired")
                .contains("!aggregation.outputField")
                .contains("studio.validation.aggregate.outputFieldRequired")
                .contains("config.windowType === \"COUNT\" && !isPositiveNumber(config.countWindowSize)")
                .contains("studio.validation.aggregate.countWindowSizePositive")
                .contains("config.windowType === \"COUNT\" && !isIntegerNumber(config.countWindowSize)")
                .contains("studio.validation.aggregate.countWindowSizeInteger")
                .contains("config.windowType !== \"COUNT\" && !isPositiveNumber(config.windowSize)")
                .contains("studio.validation.aggregate.windowSizePositive")
                .contains("config.windowType !== \"COUNT\" && !isIntegerNumber(config.windowSize)")
                .contains("studio.validation.aggregate.windowSizeInteger")
                .contains("config.windowType === \"SLIDING_TIME\" && !isPositiveNumber(config.windowSlide)")
                .contains("studio.validation.aggregate.windowSlidePositive")
                .contains("config.windowType === \"SLIDING_TIME\" && !isIntegerNumber(config.windowSlide)")
                .contains("studio.validation.aggregate.windowSlideInteger")
                .contains("config.windowType === \"SLIDING_TIME\" && Number(config.windowSlide) > Number(config.windowSize)")
                .contains("studio.validation.aggregate.windowSlideMaxWindowSize")
                .contains("config.timeMode === \"EVENT_TIME\" && config.windowType !== \"COUNT\" && !isNonNegativeNumber(config.watermarkDelay)")
                .contains("studio.validation.aggregate.watermarkDelayNonNegative")
                .contains("config.timeMode === \"EVENT_TIME\" && config.windowType !== \"COUNT\" && !isIntegerNumber(config.watermarkDelay)")
                .contains("studio.validation.aggregate.watermarkDelayInteger");
    }

    @Test
    void scriptPersistsCustomCodeConfig() throws Exception {
        String script = loadScript();
        String template = loadTemplate();

        assertThat(script)
                .contains("operator: \"CUSTOM_CODE\"")
                .contains("defaultName: t(\"studio.operator.customCode\", \"Custom code\")")
                .contains("defaultConfig: { language: \"JAVA\", compilePattern: \"SOURCE_CODE\", className: \"MyTransform\", sourceCode: ")
                .contains("selectedNode.config.className = byId(\"custom-code-class-name\")?.value?.trim?.() || \"\";")
                .contains("selectedNode.config.sourceCode = byId(\"custom-code-source\")?.value || \"\";")
                .contains("selectedNode.config.errorStrategy = byId(\"custom-code-error-strategy\")?.value || \"KEEP_ORIGINAL\";")
                .contains("byId(\"custom-code-config\")?.classList.remove(\"hidden\");")
                .contains("refreshStudioSelectValue(\"custom-code-error-strategy\");");

        assertThat(template)
                .contains("data-operator-key=\"customCode\"")
                .contains("id=\"custom-code-config\"")
                .contains("id=\"custom-code-class-name\"")
                .contains("id=\"custom-code-source\"")
                .contains("id=\"custom-code-error-strategy\"");
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
