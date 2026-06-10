const STUDIO_BOOTSTRAP = document.getElementById("studio-bootstrap");
const PAGE_MODE_EDITOR = "editor";
const PAGE_MODE_MONITOR = "monitor";
const MONITOR_REFRESH_INTERVAL_MS = 5000;
const MONITOR_REFRESH_TIMEOUT_MS = 4500;
const NODE_WIDTH = 236;
const NODE_HEIGHT = 120;
const CANVAS_NODE_PADDING = 24;
const DEFAULT_SOURCE_PORT_ID = "output-0";
const DEFAULT_TARGET_PORT_ID = "input-0";
const FILTER_OUTPUT_PORT_IDS = ["true", "false"];
const DATA_QUALITY_OUTPUT_PORT_IDS = [DEFAULT_SOURCE_PORT_ID, "dirty"];
const FILTER_PORT_STACK_OUTSET_X = 6;
const DEFAULT_RUN_PARALLELISM = 1;
let edgeRefreshFrameId = null;

function t(key, fallback, params) {
    return window.StreamCraftI18n?.t?.(key, fallback, params) ?? fallback ?? key;
}

const HEADER_STATUS_META = {
    NEW: {
        label: t("studio.status.new", "Unsaved"),
        className: "sc-pill bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border border-slate-300 dark:border-slate-600"
    },
    SAVED: {
        label: t("studio.status.saved", "Saved"),
        className: "sc-pill bg-blue-50 dark:bg-blue-500/10 text-blue-700 dark:text-blue-200 border border-blue-200 dark:border-blue-500/30"
    },
    DIRTY: {
        label: t("studio.status.dirty", "Unsaved changes"),
        className: "sc-pill bg-amber-50 dark:bg-amber-500/10 text-amber-700 dark:text-amber-200 border border-amber-200 dark:border-amber-500/30"
    },
    RUNNING: {
        label: t("studio.status.running", "Running"),
        className: "sc-pill bg-emerald-50 dark:bg-emerald-500/10 text-emerald-700 dark:text-emerald-200 border border-emerald-200 dark:border-emerald-500/30"
    },
    STOPPED: {
        label: t("studio.status.stopped", "Stopped"),
        className: "sc-pill bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border border-slate-300 dark:border-slate-600"
    },
    FAILED: {
        label: t("studio.status.failed", "Run failed"),
        className: "sc-pill bg-red-50 dark:bg-red-500/10 text-red-700 dark:text-red-200 border border-red-200 dark:border-red-500/30"
    },
    UNKNOWN: {
        label: t("studio.status.unknown", "Unknown status"),
        className: "sc-pill bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border border-slate-300 dark:border-slate-600"
    }
};

const OPERATOR_CATALOG = {
    kafkaSource: {
        type: "SOURCE",
        operator: "KAFKA_SOURCE",
        defaultName: "Kafka Source",
        runnableInRuntime: true,
        defaultConfig: {
            bootstrapServers: "",
            topics: [],
            groupId: "",
            consumeMode: "earliest",
            authType: "NONE",
            username: "",
            password: "",
            scramMechanism: "SCRAM-SHA-512",
            format: "JSON",
            sampleData: [""]
        }
    },
    jdbcSource: {
        type: "SOURCE",
        operator: "JDBC_SOURCE",
        defaultName: t("studio.operator.jdbcSource", "JDBC Source"),
        runnableInRuntime: true,
        defaultConfig: {
            url: "",
            driver: "",
            username: "",
            password: "",
            query: "",
            tablePath: "",
            readMode: "FULL",
            cursorField: "",
            cursorType: "STRING",
            initialCursorValue: "",
            pollIntervalMillis: 5000,
            fetchSize: 1000,
            maxPolls: 0,
            idField: "",
            timestampField: "",
            format: "JSON",
            sampleData: [""]
        }
    },
    elasticsearchSource: {
        type: "SOURCE",
        operator: "ELASTICSEARCH_SOURCE",
        defaultName: t("studio.operator.elasticsearchSource", "Elasticsearch Source"),
        runnableInRuntime: true,
        defaultConfig: {
            hosts: [],
            index: "",
            source: [],
            query: {},
            readMode: "FULL",
            cursorField: "",
            cursorType: "STRING",
            initialCursorValue: "",
            pollIntervalMillis: 5000,
            scrollSize: 100,
            scrollTime: "1m",
            maxPolls: 0,
            idField: "",
            timestampField: "",
            authType: "NONE",
            username: "",
            password: "",
            apiKey: "",
            format: "JSON",
            sampleData: [""]
        }
    },
    influxDbSource: {
        type: "SOURCE",
        operator: "INFLUXDB_SOURCE",
        defaultName: t("studio.operator.influxDbSource", "InfluxDB Source"),
        runnableInRuntime: true,
        defaultConfig: {
            url: "",
            database: "",
            sql: "",
            schema: {},
            epoch: "ms",
            queryTimeoutSeconds: 30,
            connectTimeoutMillis: 10000,
            readMode: "FULL",
            cursorField: "",
            cursorType: "STRING",
            initialCursorValue: "",
            pollIntervalMillis: 5000,
            maxPolls: 0,
            idField: "",
            timestampField: "",
            username: "",
            password: "",
            format: "JSON",
            sampleData: [""]
        }
    },
    hdfsFileSource: {
        type: "SOURCE",
        operator: "HDFS_FILE_SOURCE",
        defaultName: t("studio.operator.hdfsFileSource", "HDFS File Source"),
        runnableInRuntime: true,
        defaultConfig: {
            "fs.defaultFS": "",
            path: "",
            file_format_type: "JSON",
            schema: {},
            read_columns: [],
            field_delimiter: "\\001",
            row_delimiter: "\\n",
            skip_header_row_number: 0,
            csv_use_header_line: false,
            encoding: "UTF-8",
            compress_codec: "none",
            parse_partition_from_path: false,
            file_filter_pattern: "",
            readMode: "FULL",
            pollIntervalMillis: 5000,
            maxPolls: 0,
            idField: "",
            timestampField: "",
            hdfs_site_path: "",
            kerberos_principal: "",
            kerberos_keytab_path: "",
            format: "JSON",
            sampleData: [""]
        }
    },
    put: {
        type: "TRANSFORMER",
        operator: "PUT",
        defaultName: t("studio.operator.put", "Put"),
        runnableInRuntime: true,
        defaultConfig: { field: "", valueMode: "LITERAL", value: "", referenceField: "", template: "", note: "" }
    },
    prune: {
        type: "TRANSFORMER",
        operator: "PRUNE",
        defaultName: t("studio.operator.prune", "Prune"),
        runnableInRuntime: true,
        defaultConfig: { fields: [], note: "" }
    },
    rename: {
        type: "TRANSFORMER",
        operator: "RENAME",
        defaultName: t("studio.operator.rename", "Field mapping"),
        runnableInRuntime: true,
        defaultConfig: { mapping: {}, note: "" }
    },
    deserialize: {
        type: "TRANSFORMER",
        operator: "DESERIALIZE",
        defaultName: t("studio.operator.deserialize", "Deserialize"),
        runnableInRuntime: true,
        defaultConfig: { field: "", targetField: "", format: "JSON", fieldNames: [], delimiter: ",", note: "" }
    },
    serialize: {
        type: "TRANSFORMER",
        operator: "SERIALIZE",
        defaultName: t("studio.operator.serialize", "Serialize"),
        runnableInRuntime: true,
        defaultConfig: { sourceFields: [], targetField: "", format: "JSON", delimiter: ",", note: "" }
    },
    filter: {
        type: "TRANSFORMER",
        operator: "FILTER",
        defaultName: t("studio.operator.filter", "Filter"),
        runnableInRuntime: true,
        defaultConfig: { condition: "", note: "" }
    },
    grok: {
        type: "TRANSFORMER",
        operator: "GROK",
        defaultName: t("studio.operator.grok", "Grok"),
        runnableInRuntime: true,
        defaultConfig: { inputField: "", outputField: "", pattern: "", note: "" }
    },
    cast: {
        type: "TRANSFORMER",
        operator: "CAST",
        defaultName: t("studio.operator.cast", "Cast"),
        runnableInRuntime: true,
        defaultConfig: { inputField: "", outputMode: "OVERWRITE", outputField: "", targetType: "STRING", note: "" }
    },
    eval: {
        type: "TRANSFORMER",
        operator: "EVAL",
        defaultName: t("studio.operator.eval", "Field calculation"),
        runnableInRuntime: true,
        defaultConfig: { targetField: "", expression: "", outputMode: "OVERWRITE", errorStrategy: "KEEP_ORIGINAL", note: "" }
    },
    deduplicate: {
        type: "TRANSFORMER",
        operator: "DEDUPLICATE",
        defaultName: t("studio.operator.deduplicate", "Deduplicate"),
        runnableInRuntime: true,
        defaultConfig: {
            keyFields: [],
            timeMode: "PROCESSING_TIME",
            ttlSeconds: 3600,
            eventTimeField: "",
            windowSeconds: 300,
            watermarkDelaySeconds: 30,
            keepStrategy: "FIRST",
            lateDataStrategy: "DISCARD",
            duplicateStrategy: "DISCARD",
            note: ""
        }
    },
    lookupEnrich: {
        type: "TRANSFORMER",
        operator: "LOOKUP_ENRICH",
        defaultName: t("studio.operator.lookupEnrich", "Dimension enrichment"),
        runnableInRuntime: true,
        defaultConfig: {
            sourceField: "",
            targetField: "",
            entries: [{ key: "", value: "", valueType: "STRING" }],
            missingStrategy: "KEEP_ORIGINAL",
            overwriteTargetField: false,
            note: ""
        }
    },
    lookupJoin: {
        type: "TRANSFORMER",
        operator: "LOOKUP_JOIN",
        defaultName: t("studio.operator.lookupJoin", "Dimension join"),
        runnableInRuntime: true,
        defaultConfig: {
            sourceField: "",
            targetField: "",
            joinType: "LEFT",
            missingStrategy: "KEEP_ORIGINAL",
            overwriteTargetField: false,
            entries: [{ key: "", fields: {} }],
            note: ""
        }
    },
    streamJoin: {
        type: "TRANSFORMER",
        operator: "STREAM_JOIN",
        defaultName: t("studio.operator.streamJoin", "Two-stream join"),
        runnableInRuntime: true,
        defaultConfig: {
            leftKeyField: "",
            rightKeyField: "",
            targetField: "",
            joinType: "LEFT",
            missingStrategy: "KEEP_ORIGINAL",
            overwriteTargetField: false,
            timeMode: "PROCESSING_TIME",
            timeUnit: "SECONDS",
            windowBefore: 60,
            windowAfter: 60,
            watermarkDelay: 30,
            lateDataStrategy: "DROP",
            note: ""
        }
    },
    flatten: {
        type: "TRANSFORMER",
        operator: "FLATTEN",
        defaultName: t("studio.operator.flatten", "Flatten"),
        runnableInRuntime: true,
        defaultConfig: { sourceField: "", targetPrefix: "", delimiter: "_", removeSourceField: false, note: "" }
    },
    explode: {
        type: "TRANSFORMER",
        operator: "EXPLODE",
        defaultName: t("studio.operator.explode", "Explode"),
        runnableInRuntime: true,
        defaultConfig: { sourceField: "", targetField: "", keepEmpty: false, note: "" }
    },
    dataQuality: {
        type: "TRANSFORMER",
        operator: "DATA_QUALITY",
        defaultName: t("studio.operator.dataQuality", "Data validation"),
        runnableInRuntime: true,
        defaultConfig: { mode: "DIRTY_PORT", errorField: "_streamcraft_quality_errors", rules: [{ field: "", ruleType: "NOT_NULL", valueType: "STRING", min: "", max: "", minLength: "", maxLength: "", enumValues: "", pattern: "", customMessage: "" }], note: "" }
    },
    timeDerive: {
        type: "TRANSFORMER",
        operator: "TIME_DERIVE",
        defaultName: t("studio.operator.timeDerive", "Time derive"),
        runnableInRuntime: true,
        defaultConfig: { sourceField: "", sourceFormat: "AUTO", sourcePattern: "", sourceTimeZone: "UTC", outputTimeZone: "UTC", parseErrorStrategy: "KEEP_ORIGINAL", derivations: [{ outputField: "dt", type: "DATE", pattern: "" }], note: "" }
    },
    maskHash: {
        type: "TRANSFORMER",
        operator: "MASK_HASH",
        defaultName: t("studio.operator.maskHash", "Sensitive field handling"),
        runnableInRuntime: true,
        defaultConfig: { rules: [{ sourceField: "", targetField: "", action: "MASK", algorithm: "SHA256", salt: "", maskChar: "*", keepFirst: 3, keepLast: 4 }], note: "" }
    },
    caseWhen: {
        type: "TRANSFORMER",
        operator: "CASE_WHEN",
        defaultName: t("studio.operator.caseWhen", "Case when"),
        runnableInRuntime: true,
        defaultConfig: { targetField: "", cases: [{ condition: "", value: "" }], defaultMode: "NONE", defaultValue: "", defaultExpression: "", note: "" }
    },
    route: {
        type: "TRANSFORMER",
        operator: "ROUTE",
        defaultName: t("studio.operator.route", "Route"),
        runnableInRuntime: true,
        defaultConfig: { matchMode: "FIRST_MATCH", includeUnmatched: true, unmatchedPort: "unmatched", routes: [{ portId: "matched", condition: "" }], note: "" }
    },
    aggregate: {
        type: "TRANSFORMER",
        operator: "AGGREGATE",
        defaultName: t("studio.operator.aggregate", "Aggregate"),
        runnableInRuntime: true,
        defaultConfig: { mode: "GLOBAL", groupBy: [], windowType: "TUMBLING_TIME", timeMode: "PROCESSING_TIME", timeUnit: "SECONDS", windowSize: 60, windowSlide: 10, watermarkDelay: 30, eventTimeField: "", eventTimeUnit: "MILLISECONDS", outputMode: "NESTED", windowStartField: "windowStart", windowEndField: "windowEnd", countWindowSize: 100, aggregations: [{ function: "COUNT", field: "", outputField: "count" }], note: "" }
    },
    customCode: {
        type: "TRANSFORMER",
        operator: "CUSTOM_CODE",
        defaultName: t("studio.operator.customCode", "Custom code"),
        runnableInRuntime: true,
        defaultConfig: { language: "JAVA", compilePattern: "SOURCE_CODE", className: "MyTransform", sourceCode: "import com.streamcraft.core.model.DataEntity;\nimport com.streamcraft.core.runtime.transform.custom.CustomTransform;\nimport com.streamcraft.core.runtime.transform.custom.CustomTransformContext;\n\npublic class MyTransform implements CustomTransform {\n    public DataEntity process(DataEntity input, CustomTransformContext context) throws Exception {\n        return input;\n    }\n}", errorStrategy: "KEEP_ORIGINAL", note: "" }
    },
    kafkaSink: {
        type: "SINK",
        operator: "KAFKA_SINK",
        defaultName: "Kafka Sink",
        runnableInRuntime: true,
        defaultConfig: {
            bootstrapServers: "",
            topic: "",
            deliveryGuarantee: "AT_LEAST_ONCE",
            authType: "NONE",
            username: "",
            password: "",
            scramMechanism: "SCRAM-SHA-512",
            format: "JSON",
            messageField: "_streamcraft_message"
        }
    },
    jdbcSink: {
        type: "SINK",
        operator: "JDBC_SINK",
        defaultName: t("studio.operator.jdbcSink", "JDBC Sink"),
        runnableInRuntime: true,
        defaultConfig: {
            url: "",
            driver: "",
            username: "",
            password: "",
            tablePath: "",
            writeMode: "INSERT",
            fields: [],
            keyFields: [],
            batchSize: 500,
            flushIntervalMillis: 5000
        }
    },
    elasticsearchSink: {
        type: "SINK",
        operator: "ELASTICSEARCH_SINK",
        defaultName: t("studio.operator.elasticsearchSink", "Elasticsearch Sink"),
        runnableInRuntime: true,
        defaultConfig: {
            hosts: [],
            index: "",
            indexType: "",
            primaryKeys: [],
            keyDelimiter: "_",
            fields: [],
            maxBatchSize: 10,
            flushIntervalMillis: 5000,
            maxRetryCount: 3,
            authType: "NONE",
            username: "",
            password: "",
            apiKeyId: "",
            apiKey: "",
            apiKeyEncoded: ""
        }
    },
    influxDbSink: {
        type: "SINK",
        operator: "INFLUXDB_SINK",
        defaultName: t("studio.operator.influxDbSink", "InfluxDB Sink"),
        runnableInRuntime: true,
        defaultConfig: {
            url: "",
            database: "",
            measurement: "",
            keyTime: "time",
            keyTags: [],
            fields: [],
            batchSize: 100,
            maxRetries: 3,
            retryBackoffMultiplierMillis: 100,
            maxRetryBackoffMillis: 1000,
            connectTimeoutMillis: 10000,
            flushIntervalMillis: 5000,
            precision: "ms",
            username: "",
            password: ""
        }
    },
    hdfsFileSink: {
        type: "SINK",
        operator: "HDFS_FILE_SINK",
        defaultName: t("studio.operator.hdfsFileSink", "HDFS File Sink"),
        runnableInRuntime: true,
        defaultConfig: {
            "fs.defaultFS": "",
            path: "",
            tmp_path: "/tmp/streamcraft/hdfs-file",
            file_format_type: "JSON",
            sink_columns: [],
            partition_by: [],
            partition_dir_expression: "",
            is_partition_field_write_in_file: true,
            custom_filename: false,
            file_name_expression: "part-${now}",
            filename_time_format: "yyyyMMddHHmmss",
            batch_size: 1000,
            flushIntervalMillis: 5000,
            field_delimiter: "\\001",
            row_delimiter: "\\n",
            csv_use_header_line: false,
            encoding: "UTF-8",
            compress_codec: "none",
            hdfs_site_path: "",
            kerberos_principal: "",
            kerberos_keytab_path: ""
        }
    }
};

const state = {
    currentPipelineId: STUDIO_BOOTSTRAP?.dataset.pipelineId || null,
    pageMode: STUDIO_BOOTSTRAP?.dataset.pageMode || PAGE_MODE_EDITOR,
    lastRunStatus: null,
    lastRunMessage: "",
    pipelineMeta: { name: "", description: "" },
    runtimeTarget: null,
    nodes: [],
    edges: [],
    selectedNodeId: null,
    selectedEdgeId: null,
    dragState: null,
    connectState: null,
    hasUnsavedChanges: false,
    requestState: { saving: false, running: false, stopping: false },
    preview: { outputs: [], error: "", running: false },
    contextMenu: { visible: false, nodeId: null, x: 0, y: 0 },
    monitorRefreshTimer: null,
    lastMonitorRefreshAt: null,
    monitorLastSampleAt: null,
    monitorRefreshInFlight: false,
    monitorRefreshRequestId: 0,
    monitorMetricsByNodeId: new Map(),
    previousMonitorTotalsByNodeId: new Map()
};

function isEditorMode() {
    return state.pageMode === PAGE_MODE_EDITOR;
}

function isMonitorMode() {
    return state.pageMode === PAGE_MODE_MONITOR;
}

function markDirty() {
    state.hasUnsavedChanges = true;
}

function clearDirty() {
    state.hasUnsavedChanges = false;
}

function byId(id) {
    return document.getElementById(id);
}

function refreshStudioSelectValue(id) {
    window.StudioSelectEnhancer?.refreshValue(id);
    syncSegmentedControlValue(id);
}

function refreshStudioSelectOptions(id) {
    window.StudioSelectEnhancer?.refreshOptions(id);
}

function syncSegmentedControlValue(selectId) {
    const select = byId(selectId);
    if (!select) {
        return;
    }
    const value = select.value || select.options?.[0]?.value || "";
    document.querySelectorAll(`[data-segmented-control][data-target-select="${selectId}"]`).forEach(control => {
        control.querySelectorAll("[data-segmented-value]").forEach(button => {
            const active = button.dataset.segmentedValue === value;
            button.classList.toggle("is-active", active);
            button.setAttribute("aria-pressed", String(active));
        });
    });
}

function bindSegmentedControls() {
    document.querySelectorAll("[data-segmented-control]").forEach(control => {
        if (control.dataset.segmentedBound === "true") {
            return;
        }
        const selectId = control.dataset.targetSelect || "";
        const select = byId(selectId);
        if (!select) {
            return;
        }

        control.dataset.segmentedBound = "true";
        const buttons = Array.from(control.querySelectorAll("[data-segmented-value]"));
        buttons.forEach((button, index) => {
            button.addEventListener("click", () => {
                const nextValue = button.dataset.segmentedValue || "";
                if (!nextValue) {
                    return;
                }
                const changed = select.value !== nextValue;
                select.value = nextValue;
                syncSegmentedControlValue(selectId);
                if (changed) {
                    select.dispatchEvent(new Event("input", { bubbles: true }));
                    select.dispatchEvent(new Event("change", { bubbles: true }));
                }
            });
            button.addEventListener("keydown", event => {
                const keyOffsets = { ArrowLeft: -1, ArrowUp: -1, ArrowRight: 1, ArrowDown: 1 };
                let nextIndex = null;
                if (event.key === "Home") {
                    nextIndex = 0;
                } else if (event.key === "End") {
                    nextIndex = buttons.length - 1;
                } else if (Object.prototype.hasOwnProperty.call(keyOffsets, event.key)) {
                    nextIndex = (index + keyOffsets[event.key] + buttons.length) % buttons.length;
                }
                if (nextIndex === null) {
                    return;
                }
                event.preventDefault();
                buttons[nextIndex]?.focus();
                buttons[nextIndex]?.click();
            });
        });
        syncSegmentedControlValue(selectId);
    });
}

function splitTopics(value) {
    return String(value || "")
        .split(",")
        .map(item => item.trim())
        .filter(Boolean);
}

function positiveIntegerValue(id, fallback) {
    const value = Number(byId(id)?.value);
    return Number.isInteger(value) && value > 0 ? value : fallback;
}

function cloneConfig(config) {
    return JSON.parse(JSON.stringify(config || {}));
}

function parseJsonValue(value, fallback) {
    try {
        return JSON.parse(value);
    } catch (error) {
        return fallback;
    }
}

function inferPutValueMode(value) {
    const text = String(value || "");
    if (/^\$\{[^{}]+}$/.test(text)) {
        return "FIELD_REFERENCE";
    }
    return text.includes("${") ? "TEMPLATE" : "LITERAL";
}

function putReferenceField(value) {
    const match = String(value || "").match(/^\$\{([^{}]+)}$/);
    return match ? match[1] : "";
}

function putStoredValue(valueMode, literalValue, referenceField, template) {
    if (valueMode === "FIELD_REFERENCE") {
        return referenceField ? `\${${referenceField}}` : "";
    }
    if (valueMode === "TEMPLATE") {
        return template || "";
    }
    return literalValue || "";
}

function updatePutValueModeUI(valueMode) {
    byId("put-literal-value-wrapper")?.classList.toggle("hidden", valueMode !== "LITERAL");
    byId("put-reference-field-wrapper")?.classList.toggle("hidden", valueMode !== "FIELD_REFERENCE");
    byId("put-template-wrapper")?.classList.toggle("hidden", valueMode !== "TEMPLATE");
}

function inferCastOutputMode(inputField, outputField) {
    const input = String(inputField || "").trim();
    const output = String(outputField || "").trim();
    return !output || output === input ? "OVERWRITE" : "NEW_FIELD";
}

function castStoredOutputField(inputField, outputMode, outputField) {
    if (outputMode === "NEW_FIELD") {
        return String(outputField || "").trim();
    }
    return String(inputField || "").trim();
}

function updateCastOutputModeUI(outputMode) {
    byId("cast-output-field-wrapper")?.classList.toggle("hidden", outputMode !== "NEW_FIELD");
}

function updateDeduplicateTimeModeUI(timeMode) {
    const isEventTime = timeMode === "EVENT_TIME";
    byId("deduplicate-processing-time-section")?.classList.toggle("hidden", isEventTime);
    byId("deduplicate-event-time-section")?.classList.toggle("hidden", !isEventTime);
    const keepStrategy = byId("deduplicate-keep-strategy");
    const eventTimeLatest = keepStrategy?.querySelector("option[value='EVENT_TIME_LATEST']");
    if (eventTimeLatest) {
        eventTimeLatest.disabled = !isEventTime;
    }
    if (!isEventTime && keepStrategy?.value === "EVENT_TIME_LATEST") {
        keepStrategy.value = "FIRST";
        refreshStudioSelectValue("deduplicate-keep-strategy");
    }
}

function showMessage(kind, text) {
    const box = byId("studio-message");
    if (!box) {
        return;
    }

    box.classList.remove(
        "hidden",
        "border-emerald-500/30",
        "bg-emerald-500/10",
        "text-emerald-700",
        "dark:text-emerald-200",
        "border-red-500/30",
        "bg-red-500/10",
        "text-red-700",
        "dark:text-red-200",
        "border-blue-500/30",
        "bg-blue-500/10",
        "text-blue-700",
        "dark:text-blue-200"
    );

    if (kind === "success") {
        box.classList.add("border-emerald-500/30", "bg-emerald-500/10", "text-emerald-700", "dark:text-emerald-200");
    } else if (kind === "error") {
        box.classList.add("border-red-500/30", "bg-red-500/10", "text-red-700", "dark:text-red-200");
    } else {
        box.classList.add("border-blue-500/30", "bg-blue-500/10", "text-blue-700", "dark:text-blue-200");
    }

    box.textContent = text;
}

function hideNodeContextMenu() {
    state.contextMenu.visible = false;
    state.contextMenu.nodeId = null;
    state.contextMenu.x = 0;
    state.contextMenu.y = 0;
}

function showNodeContextMenu(nodeId, event) {
    if (!isEditorMode()) {
        return;
    }

    event.preventDefault();
    event.stopPropagation();
    selectNode(nodeId);

    const container = byId("canvas-container") || byId("canvas-drop-zone");
    const rect = container?.getBoundingClientRect();
    state.contextMenu.visible = true;
    state.contextMenu.nodeId = nodeId;
    state.contextMenu.x = rect ? event.clientX - rect.left : event.clientX;
    state.contextMenu.y = rect ? event.clientY - rect.top : event.clientY;
    renderNodeContextMenu();
}

function catalogEntryForOperator(operator) {
    return Object.values(OPERATOR_CATALOG).find(item => item.operator === operator) || null;
}

function toStudioNodeType(type) {
    return type === "TRANSFORM" ? "TRANSFORMER" : type;
}

function toDefinitionNodeType(type) {
    return type === "TRANSFORMER" ? "TRANSFORM" : type;
}

function normalizeRunStatus(status) {
    const value = String(status || "").toUpperCase();
    return HEADER_STATUS_META[value] ? value : "UNKNOWN";
}

function escapeHtml(value) {
    return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}

function formatMetricCount(value) {
    const numericValue = Number(value);
    if (!Number.isFinite(numericValue)) {
        return "--";
    }
    if (Math.abs(numericValue) >= 1000000000) {
        return `${(numericValue / 1000000000).toFixed(1)}B`;
    }
    if (Math.abs(numericValue) >= 1000000) {
        return `${(numericValue / 1000000).toFixed(1)}M`;
    }
    if (Math.abs(numericValue) >= 1000) {
        return `${(numericValue / 1000).toFixed(1)}K`;
    }
    return `${numericValue}`;
}

function formatMetricRate(value) {
    const numericValue = Number(value);
    if (!Number.isFinite(numericValue)) {
        return "--";
    }
    return `${formatMetricCount(Number(numericValue.toFixed(2)))}/s`;
}

function calculateRate(currentValue, previousValue, intervalSeconds) {
    const current = Number(currentValue);
    const previous = Number(previousValue);
    if (!Number.isFinite(current) || !Number.isFinite(previous) || current < previous || intervalSeconds <= 0) {
        return null;
    }
    return (current - previous) / intervalSeconds;
}

function monitorMetricsForNode(nodeId) {
    return state.monitorMetricsByNodeId.get(nodeId) || null;
}

function findNodeById(nodeId) {
    return state.nodes.find(node => node.id === nodeId);
}

function nodeDisplayTitle(node) {
    return String(node?.displayName || node?.name || "").trim();
}

function syncPipelineMetaFromForm() {
    state.pipelineMeta.name = byId("pipeline-name")?.value?.trim?.() ?? state.pipelineMeta.name;
    state.pipelineMeta.description = byId("pipeline-description")?.value?.trim?.() ?? state.pipelineMeta.description;
}

function updateRuntimeResourceControls() {
    const resourcesPanel = byId("pipeline-runtime-resources");
    const hasRuntimeTarget = Boolean(state.runtimeTarget);

    resourcesPanel?.classList.toggle("hidden", !hasRuntimeTarget);

    if (resourcesPanel) {
        resourcesPanel.dataset.runtimeTargetType = state.runtimeTarget?.type || "";
    }
}

function runResourcePayload() {
    const payload = {};
    payload.parallelism = positiveIntegerValue("run-parallelism", DEFAULT_RUN_PARALLELISM);
    return payload;
}

function normalizeSourceSampleData(sampleData) {
    if (!Array.isArray(sampleData) || sampleData.length === 0) {
        return [""];
    }
    return sampleData.map(item => item == null ? "" : String(item));
}

function renderSourceSampleInputs(sampleData) {
    const container = byId("source-sample-list");
    const template = byId("source-sample-item-template");
    if (!container || !template) {
        return;
    }

    container.innerHTML = "";
    const normalizedSamples = normalizeSourceSampleData(sampleData);
    const showRemoveButton = normalizedSamples.length > 1;
    normalizedSamples.forEach(sample => {
        const fragment = template.content.cloneNode(true);
        const input = fragment.querySelector("[data-role='source-sample-input']");
        const removeButton = fragment.querySelector("[data-role='remove-source-sample']");
        if (input) {
            input.value = sample;
        }
        if (removeButton) {
            removeButton.classList.toggle("hidden", !showRemoveButton);
        }
        container.appendChild(fragment);
    });
}

function normalizeRenameMappings(mapping) {
    if (!mapping || typeof mapping !== "object" || Array.isArray(mapping)) {
        return [{ sourceField: "", targetField: "" }];
    }

    const rows = Object.entries(mapping)
        .map(([sourceField, targetField]) => ({
            sourceField: String(sourceField ?? ""),
            targetField: targetField == null ? "" : String(targetField)
        }))
        .filter(item => item.sourceField || item.targetField);
    return rows.length > 0 ? rows : [{ sourceField: "", targetField: "" }];
}

function renderRenameMappingInputs(mapping) {
    renderRenameMappingRows(normalizeRenameMappings(mapping));
}

function normalizePruneFields(fields) {
    if (!Array.isArray(fields) || fields.length === 0) {
        return [{ field: "" }];
    }

    const rows = fields
        .map(field => ({ field: field == null ? "" : String(field).trim() }))
        .filter(item => item.field);
    return rows.length > 0 ? rows : [{ field: "" }];
}

function renderPruneFieldInputs(fields) {
    renderPruneFieldRows(normalizePruneFields(fields));
}

function collectPruneFieldInputs() {
    const container = byId("prune-field-list");
    if (!container) {
        return [];
    }

    return Array.from(container.querySelectorAll("[data-role='prune-field-item']"))
        .map(item => item.querySelector("[data-role='prune-field-name']")?.value?.trim?.() || "")
        .filter(Boolean);
}

function collectPruneFieldRows() {
    const container = byId("prune-field-list");
    if (!container) {
        return [{ field: "" }];
    }

    const rows = Array.from(container.querySelectorAll("[data-role='prune-field-item']"))
        .map(item => ({
            field: item.querySelector("[data-role='prune-field-name']")?.value?.trim?.() || ""
        }));
    return rows.length > 0 ? rows : [{ field: "" }];
}

function collectRenameMappingInputs() {
    const container = byId("rename-mapping-list");
    if (!container) {
        return {};
    }

    return Array.from(container.querySelectorAll("[data-role='rename-mapping-item']"))
        .reduce((mapping, item) => {
            const sourceField = item.querySelector("[data-role='rename-source-field']")?.value?.trim?.() || "";
            const targetField = item.querySelector("[data-role='rename-target-field']")?.value?.trim?.() || "";
            if (!sourceField || !targetField) {
                return mapping;
            }
            mapping[sourceField] = targetField;
            return mapping;
        }, {});
}

function collectRenameMappingRows() {
    const container = byId("rename-mapping-list");
    if (!container) {
        return [{ sourceField: "", targetField: "" }];
    }

    const rows = Array.from(container.querySelectorAll("[data-role='rename-mapping-item']"))
        .map(item => ({
            sourceField: item.querySelector("[data-role='rename-source-field']")?.value?.trim?.() || "",
            targetField: item.querySelector("[data-role='rename-target-field']")?.value?.trim?.() || ""
        }));
    return rows.length > 0 ? rows : [{ sourceField: "", targetField: "" }];
}

function defaultLookupEnrichEntry() {
    return { key: "", value: "", valueType: "STRING" };
}

function lookupEnrichEntryValueType(value) {
    if (typeof value === "boolean") {
        return "BOOLEAN";
    }
    if (typeof value === "number") {
        return "NUMBER";
    }
    if (value != null && typeof value === "object") {
        return "JSON";
    }
    return "STRING";
}

function lookupEnrichEntryDisplayValue(value, valueType) {
    if (value == null) {
        return "";
    }
    if (valueType === "JSON" && typeof value === "object") {
        return JSON.stringify(value);
    }
    return String(value);
}

function lookupEnrichTypedValue(value, valueType) {
    const textValue = String(value ?? "");
    switch (valueType) {
        case "NUMBER": {
            const numberValue = Number(textValue);
            return Number.isFinite(numberValue) ? numberValue : textValue;
        }
        case "BOOLEAN":
            return textValue.trim().toLowerCase() === "true";
        case "JSON":
            return parseJsonValue(textValue, textValue);
        case "STRING":
        default:
            return textValue;
    }
}

function lookupEnrichEntryHasValue(entry) {
    return entry?.value !== undefined && entry?.value !== null && String(entry.value).trim() !== "";
}

function normalizeLookupEnrichEntries(entries) {
    if (!Array.isArray(entries) || entries.length === 0) {
        return [defaultLookupEnrichEntry()];
    }

    const rows = entries
        .map(item => {
            const valueType = String(item?.valueType || lookupEnrichEntryValueType(item?.value) || "STRING");
            return {
                key: item?.key == null ? "" : String(item.key),
                value: lookupEnrichEntryDisplayValue(item?.value, valueType),
                valueType
            };
        })
        .filter(item => item.key || item.value);
    return rows.length > 0 ? rows : [defaultLookupEnrichEntry()];
}

function renderLookupEnrichEntries(entries) {
    const container = byId("lookup-enrich-entry-list");
    const template = byId("lookup-enrich-entry-template");
    if (!container || !template) {
        return;
    }

    container.innerHTML = "";
    const normalizedRows = normalizeLookupEnrichEntries(entries);
    const showRemoveButton = normalizedRows.length > 1;
    normalizedRows.forEach(item => {
        const fragment = template.content.cloneNode(true);
        const keyInput = fragment.querySelector("[data-role='lookup-enrich-entry-key']");
        const valueInput = fragment.querySelector("[data-role='lookup-enrich-entry-value']");
        const typeSelect = fragment.querySelector("[data-role='lookup-enrich-entry-value-type']");
        const removeButton = fragment.querySelector("[data-role='remove-lookup-enrich-entry']");
        if (keyInput) {
            keyInput.value = item.key;
        }
        if (valueInput) {
            valueInput.value = item.value;
        }
        if (typeSelect) {
            typeSelect.value = item.valueType || "STRING";
        }
        if (removeButton) {
            removeButton.classList.toggle("hidden", !showRemoveButton);
        }
        container.appendChild(fragment);
    });
}

function collectLookupEnrichEntries() {
    const container = byId("lookup-enrich-entry-list");
    if (!container) {
        return [defaultLookupEnrichEntry()];
    }

    const rows = Array.from(container.querySelectorAll("[data-role='lookup-enrich-entry-item']"))
        .map(item => ({
            key: item.querySelector("[data-role='lookup-enrich-entry-key']")?.value?.trim?.() || "",
            value: item.querySelector("[data-role='lookup-enrich-entry-value']")?.value?.trim?.() || "",
            valueType: item.querySelector("[data-role='lookup-enrich-entry-value-type']")?.value || "STRING"
        }))
        .filter(item => item.key || item.value);
    return rows.length > 0
        ? rows.map(item => ({ ...item, value: lookupEnrichTypedValue(item.value, item.valueType || "STRING") }))
        : [defaultLookupEnrichEntry()];
}

function defaultLookupJoinField() {
    return { name: "", value: "", valueType: "STRING" };
}

function defaultLookupJoinEntry() {
    return { key: "", fields: [defaultLookupJoinField()] };
}

function lookupJoinFieldValueType(value) {
    if (typeof value === "boolean") {
        return "BOOLEAN";
    }
    if (typeof value === "number") {
        return "NUMBER";
    }
    if (value != null && typeof value === "object") {
        return "JSON";
    }
    return "STRING";
}

function lookupJoinFieldDisplayValue(value, valueType) {
    if (value == null) {
        return "";
    }
    if (valueType === "JSON" && typeof value === "object") {
        return JSON.stringify(value);
    }
    return String(value);
}

function lookupJoinTypedValue(value, valueType) {
    const textValue = String(value ?? "");
    switch (valueType) {
        case "NUMBER": {
            const numberValue = Number(textValue);
            return Number.isFinite(numberValue) ? numberValue : textValue;
        }
        case "BOOLEAN":
            return textValue.trim().toLowerCase() === "true";
        case "JSON":
            return parseJsonValue(textValue, textValue);
        case "STRING":
        default:
            return textValue;
    }
}

function normalizeLookupJoinFields(fields) {
    if (Array.isArray(fields)) {
        const rows = fields
            .map(field => {
                const valueType = String(field?.valueType || lookupJoinFieldValueType(field?.value) || "STRING");
                return {
                    name: field?.name == null ? "" : String(field.name),
                    value: lookupJoinFieldDisplayValue(field?.value, valueType),
                    valueType
                };
            })
            .filter(field => field.name || field.value);
        return rows.length > 0 ? rows : [defaultLookupJoinField()];
    }

    if (fields && typeof fields === "object") {
        const rows = Object.entries(fields).map(([name, value]) => {
            const valueType = lookupJoinFieldValueType(value);
            return {
                name,
                value: lookupJoinFieldDisplayValue(value, valueType),
                valueType
            };
        });
        return rows.length > 0 ? rows : [defaultLookupJoinField()];
    }

    return [defaultLookupJoinField()];
}

function normalizeLookupJoinEntries(entries) {
    if (!Array.isArray(entries) || entries.length === 0) {
        return [defaultLookupJoinEntry()];
    }

    const rows = entries
        .map(item => ({
            key: item?.key == null ? "" : String(item.key),
            fields: normalizeLookupJoinFields(item?.fields)
        }))
        .filter(item => item.key || item.fields.some(field => field.name || field.value));
    return rows.length > 0 ? rows : [defaultLookupJoinEntry()];
}

function renderLookupJoinEntries(entries) {
    const container = byId("lookup-join-entry-list");
    const template = byId("lookup-join-entry-template");
    const fieldTemplate = byId("lookup-join-field-template");
    if (!container || !template || !fieldTemplate) {
        return;
    }

    container.innerHTML = "";
    const normalizedEntries = normalizeLookupJoinEntries(entries);
    const showRemoveEntryButton = normalizedEntries.length > 1;
    normalizedEntries.forEach(entry => {
        const fragment = template.content.cloneNode(true);
        const keyInput = fragment.querySelector("[data-role='lookup-join-entry-key']");
        const fieldList = fragment.querySelector("[data-role='lookup-join-field-list']");
        const removeEntryButton = fragment.querySelector("[data-role='remove-lookup-join-entry']");
        if (keyInput) {
            keyInput.value = entry.key;
        }
        if (removeEntryButton) {
            removeEntryButton.classList.toggle("hidden", !showRemoveEntryButton);
        }
        if (fieldList) {
            const showRemoveFieldButton = entry.fields.length > 1;
            entry.fields.forEach(field => {
                const fieldFragment = fieldTemplate.content.cloneNode(true);
                const nameInput = fieldFragment.querySelector("[data-role='lookup-join-field-name']");
                const valueInput = fieldFragment.querySelector("[data-role='lookup-join-field-value']");
                const typeSelect = fieldFragment.querySelector("[data-role='lookup-join-field-type']");
                const removeFieldButton = fieldFragment.querySelector("[data-role='remove-lookup-join-field']");
                if (nameInput) {
                    nameInput.value = field.name;
                }
                if (valueInput) {
                    valueInput.value = field.value;
                }
                if (typeSelect) {
                    typeSelect.value = field.valueType || "STRING";
                }
                if (removeFieldButton) {
                    removeFieldButton.classList.toggle("hidden", !showRemoveFieldButton);
                }
                fieldList.appendChild(fieldFragment);
            });
        }
        container.appendChild(fragment);
    });
}

function collectLookupJoinEntryRows() {
    const container = byId("lookup-join-entry-list");
    if (!container) {
        return [defaultLookupJoinEntry()];
    }

    const rows = Array.from(container.querySelectorAll("[data-role='lookup-join-entry-item']"))
        .map(item => ({
            key: item.querySelector("[data-role='lookup-join-entry-key']")?.value?.trim?.() || "",
            fields: Array.from(item.querySelectorAll("[data-role='lookup-join-field-item']"))
                .map(fieldItem => ({
                    name: fieldItem.querySelector("[data-role='lookup-join-field-name']")?.value?.trim?.() || "",
                    value: fieldItem.querySelector("[data-role='lookup-join-field-value']")?.value || "",
                    valueType: fieldItem.querySelector("[data-role='lookup-join-field-type']")?.value || "STRING"
                }))
        }))
        .filter(item => item.key || item.fields.some(field => field.name || field.value));
    return rows.length > 0 ? rows : [defaultLookupJoinEntry()];
}

function collectLookupJoinEntries() {
    const rows = collectLookupJoinEntryRows()
        .map(item => {
            const fields = item.fields.reduce((result, field) => {
                if (!field.name) {
                    return result;
                }
                result[field.name] = lookupJoinTypedValue(field.value, field.valueType || "STRING");
                return result;
            }, {});
            return { key: item.key, fields };
        })
        .filter(item => item.key || Object.keys(item.fields).length > 0);
    return rows.length > 0 ? rows : [{ key: "", fields: {} }];
}

function appendLookupJoinEntry() {
    const nextRows = collectLookupJoinEntryRows();
    nextRows.push(defaultLookupJoinEntry());
    renderLookupJoinEntries(nextRows);
}

function removeLookupJoinEntry(index) {
    const nextRows = collectLookupJoinEntryRows().filter((_, rowIndex) => rowIndex !== index);
    renderLookupJoinEntries(nextRows.length > 0 ? nextRows : [defaultLookupJoinEntry()]);
}

function appendLookupJoinField(entryIndex) {
    const nextRows = collectLookupJoinEntryRows();
    const entry = nextRows[entryIndex];
    if (!entry) {
        return;
    }
    entry.fields.push(defaultLookupJoinField());
    renderLookupJoinEntries(nextRows);
}

function removeLookupJoinField(entryIndex, fieldIndex) {
    const nextRows = collectLookupJoinEntryRows();
    const entry = nextRows[entryIndex];
    if (!entry) {
        return;
    }
    entry.fields = entry.fields.filter((_, rowIndex) => rowIndex !== fieldIndex);
    if (entry.fields.length === 0) {
        entry.fields = [defaultLookupJoinField()];
    }
    renderLookupJoinEntries(nextRows);
}

function updateLookupJoinTypeUI(joinType) {
    const isInnerJoin = joinType === "INNER";
    byId("lookup-join-missing-strategy-wrapper")?.classList.toggle("hidden", isInnerJoin);
}

function defaultDataQualityRule() {
    return { field: "", ruleType: "NOT_NULL", valueType: "STRING", min: "", max: "", minLength: "", maxLength: "", enumValues: "", pattern: "", customMessage: "" };
}

function normalizeDataQualityRules(rules) {
    if (!Array.isArray(rules) || rules.length === 0) {
        return [defaultDataQualityRule()];
    }

    const rows = rules.map(item => ({
        field: item?.field == null ? "" : String(item.field),
        ruleType: item?.ruleType == null ? "NOT_NULL" : String(item.ruleType || "NOT_NULL"),
        valueType: item?.valueType == null ? "STRING" : String(item.valueType || "STRING"),
        min: item?.min == null ? "" : String(item.min),
        max: item?.max == null ? "" : String(item.max),
        minLength: item?.minLength == null ? "" : String(item.minLength),
        maxLength: item?.maxLength == null ? "" : String(item.maxLength),
        enumValues: Array.isArray(item?.enumValues) ? item.enumValues.join(",") : String(item?.enumValues || ""),
        pattern: item?.pattern == null ? "" : String(item.pattern),
        customMessage: item?.customMessage == null ? "" : String(item.customMessage)
    })).filter(item => item.field || item.ruleType || item.valueType || item.min || item.max || item.minLength || item.maxLength || item.enumValues || item.pattern || item.customMessage);
    return rows.length > 0 ? rows : [defaultDataQualityRule()];
}

function updateDataQualityRuleUI(item) {
    if (!item) {
        return;
    }
    const ruleType = item.querySelector("[data-role='data-quality-rule-kind']")?.value || "NOT_NULL";
    item.querySelector("[data-role='data-quality-value-type-wrapper']")?.classList.toggle("hidden", ruleType !== "TYPE");
    item.querySelector("[data-role='data-quality-range-wrapper']")?.classList.toggle("hidden", ruleType !== "RANGE");
    item.querySelector("[data-role='data-quality-length-wrapper']")?.classList.toggle("hidden", ruleType !== "LENGTH");
    item.querySelector("[data-role='data-quality-enum-wrapper']")?.classList.toggle("hidden", ruleType !== "ENUM");
    item.querySelector("[data-role='data-quality-regex-wrapper']")?.classList.toggle("hidden", ruleType !== "REGEX");
}

function updateDataQualityModeUI(mode) {
    byId("data-quality-error-field-wrapper")?.classList.toggle("hidden", mode !== "MARK_ERROR" && mode !== "DIRTY_PORT");
}

function renderDataQualityRules(rules) {
    const container = byId("data-quality-rule-list");
    const template = byId("data-quality-rule-template");
    if (!container || !template) {
        return;
    }

    container.innerHTML = "";
    const normalizedRules = normalizeDataQualityRules(rules);
    const showRemoveButton = normalizedRules.length > 1;
    normalizedRules.forEach(item => {
        const fragment = template.content.cloneNode(true);
        const fieldInput = fragment.querySelector("[data-role='data-quality-rule-field']");
        const ruleTypeSelect = fragment.querySelector("[data-role='data-quality-rule-kind']");
        const valueTypeSelect = fragment.querySelector("[data-role='data-quality-rule-value-type']");
        const minInput = fragment.querySelector("[data-role='data-quality-rule-min']");
        const maxInput = fragment.querySelector("[data-role='data-quality-rule-max']");
        const minLengthInput = fragment.querySelector("[data-role='data-quality-rule-min-length']");
        const maxLengthInput = fragment.querySelector("[data-role='data-quality-rule-max-length']");
        const enumInput = fragment.querySelector("[data-role='data-quality-rule-enum-values']");
        const patternInput = fragment.querySelector("[data-role='data-quality-rule-pattern']");
        const customMessageInput = fragment.querySelector("[data-role='data-quality-rule-message']");
        const removeButton = fragment.querySelector("[data-role='remove-data-quality-rule']");
        if (fieldInput) fieldInput.value = item.field || "";
        if (ruleTypeSelect) ruleTypeSelect.value = item.ruleType || "NOT_NULL";
        if (valueTypeSelect) valueTypeSelect.value = item.valueType || "STRING";
        if (minInput) minInput.value = item.min || "";
        if (maxInput) maxInput.value = item.max || "";
        if (minLengthInput) minLengthInput.value = item.minLength || "";
        if (maxLengthInput) maxLengthInput.value = item.maxLength || "";
        if (enumInput) enumInput.value = item.enumValues || "";
        if (patternInput) patternInput.value = item.pattern || "";
        if (customMessageInput) customMessageInput.value = item.customMessage || "";
        if (removeButton) {
            removeButton.classList.toggle("hidden", !showRemoveButton);
        }
        updateDataQualityRuleUI(fragment.querySelector("[data-role='data-quality-rule-item']"));
        container.appendChild(fragment);
    });
}

function collectDataQualityRules() {
    const container = byId("data-quality-rule-list");
    if (!container) {
        return [defaultDataQualityRule()];
    }

    const rows = Array.from(container.querySelectorAll("[data-role='data-quality-rule-item']"))
        .map(item => ({
            field: item.querySelector("[data-role='data-quality-rule-field']")?.value?.trim?.() || "",
            ruleType: item.querySelector("[data-role='data-quality-rule-kind']")?.value || "NOT_NULL",
            valueType: item.querySelector("[data-role='data-quality-rule-value-type']")?.value || "STRING",
            min: item.querySelector("[data-role='data-quality-rule-min']")?.value?.trim?.() || "",
            max: item.querySelector("[data-role='data-quality-rule-max']")?.value?.trim?.() || "",
            minLength: item.querySelector("[data-role='data-quality-rule-min-length']")?.value?.trim?.() || "",
            maxLength: item.querySelector("[data-role='data-quality-rule-max-length']")?.value?.trim?.() || "",
            enumValues: (item.querySelector("[data-role='data-quality-rule-enum-values']")?.value || "").trim(),
            pattern: item.querySelector("[data-role='data-quality-rule-pattern']")?.value?.trim?.() || "",
            customMessage: item.querySelector("[data-role='data-quality-rule-message']")?.value?.trim?.() || ""
        }))
        .filter(item => item.field || item.ruleType || item.valueType || item.min || item.max || item.minLength || item.maxLength || item.enumValues || item.pattern || item.customMessage);
    return rows.length > 0 ? rows : [defaultDataQualityRule()];
}

function appendDataQualityRule() {
    const nextRows = collectDataQualityRules();
    nextRows.push(defaultDataQualityRule());
    renderDataQualityRules(nextRows);
}

function removeDataQualityRule(index) {
    const nextRows = collectDataQualityRules().filter((_, rowIndex) => rowIndex !== index);
    renderDataQualityRules(nextRows.length > 0 ? nextRows : [defaultDataQualityRule()]);
}

function defaultTimeDeriveItem() {
    return { outputField: "", type: "DATE", pattern: "" };
}

function normalizeTimeDeriveItems(derivations) {
    if (!Array.isArray(derivations) || derivations.length === 0) {
        return [defaultTimeDeriveItem()];
    }

    const rows = derivations
        .map(item => ({
            outputField: item?.outputField == null ? "" : String(item.outputField).trim(),
            type: item?.type == null ? "DATE" : String(item.type).trim() || "DATE",
            pattern: item?.pattern == null ? "" : String(item.pattern).trim()
        }))
        .filter(item => item.outputField || item.pattern);
    return rows.length > 0 ? rows : [defaultTimeDeriveItem()];
}

function timeDeriveRequiresPattern(type) {
    return type === "FORMAT";
}

function updateTimeDeriveSourceFormatUI(sourceFormat) {
    byId("time-derive-source-pattern-wrapper")?.classList.toggle("hidden", sourceFormat !== "PATTERN");
}

function updateTimeDeriveDerivationUI(item) {
    const type = item.querySelector("[data-role='time-derive-type']")?.value || "DATE";
    item.querySelector("[data-role='time-derive-pattern-wrapper']")?.classList.toggle("hidden", !timeDeriveRequiresPattern(type));
}

function renderTimeDeriveItems(derivations) {
    const container = byId("time-derive-derivation-list");
    const template = byId("time-derive-derivation-template");
    if (!container || !template) {
        return;
    }

    container.innerHTML = "";
    const normalizedItems = normalizeTimeDeriveItems(derivations);
    const showRemoveButton = normalizedItems.length > 1;
    normalizedItems.forEach((item, index) => {
        const fragment = template.content.cloneNode(true);
        const outputFieldInput = fragment.querySelector("[data-role='time-derive-output-field']");
        const typeSelect = fragment.querySelector("[data-role='time-derive-type']");
        const patternInput = fragment.querySelector("[data-role='time-derive-pattern']");
        const moveUpButton = fragment.querySelector("[data-role='move-time-derive-up']");
        const moveDownButton = fragment.querySelector("[data-role='move-time-derive-down']");
        const removeButton = fragment.querySelector("[data-role='remove-time-derive']");
        if (outputFieldInput) {
            outputFieldInput.value = item.outputField;
        }
        if (typeSelect) {
            typeSelect.value = item.type || "DATE";
        }
        if (patternInput) {
            patternInput.value = item.pattern || "";
        }
        if (moveUpButton) {
            moveUpButton.disabled = index === 0;
        }
        if (moveDownButton) {
            moveDownButton.disabled = index === normalizedItems.length - 1;
        }
        if (removeButton) {
            removeButton.classList.toggle("hidden", !showRemoveButton);
        }
        container.appendChild(fragment);
        updateTimeDeriveDerivationUI(container.lastElementChild);
    });
}

function collectTimeDeriveItems() {
    const container = byId("time-derive-derivation-list");
    if (!container) {
        return [defaultTimeDeriveItem()];
    }

    const rows = Array.from(container.querySelectorAll("[data-role='time-derive-derivation-item']"))
        .map(item => {
            const outputField = item.querySelector("[data-role='time-derive-output-field']")?.value?.trim?.() || "";
            const type = item.querySelector("[data-role='time-derive-type']")?.value || "DATE";
            const pattern = item.querySelector("[data-role='time-derive-pattern']")?.value?.trim?.() || "";
            return { outputField, type, pattern: timeDeriveRequiresPattern(type) ? pattern : "" };
        })
        .filter(item => item.outputField || item.pattern);
    return rows.length > 0 ? rows : [defaultTimeDeriveItem()];
}

function appendTimeDeriveItem() {
    const derivations = collectTimeDeriveItems();
    derivations.push(defaultTimeDeriveItem());
    renderTimeDeriveItems(derivations);
}

function removeTimeDeriveItem(index) {
    const derivations = collectTimeDeriveItems().filter((_, itemIndex) => itemIndex !== index);
    renderTimeDeriveItems(derivations.length > 0 ? derivations : [defaultTimeDeriveItem()]);
}

function moveTimeDeriveItem(index, offset) {
    const derivations = collectTimeDeriveItems();
    const nextIndex = index + offset;
    if (nextIndex < 0 || nextIndex >= derivations.length) {
        return;
    }
    const [item] = derivations.splice(index, 1);
    derivations.splice(nextIndex, 0, item);
    renderTimeDeriveItems(derivations);
}

function appendLookupEnrichEntry(key = "", value = "", valueType = "STRING") {
    const nextRows = collectLookupEnrichEntries();
    nextRows.push({ key, value, valueType });
    renderLookupEnrichEntries(nextRows);
}

function removeLookupEnrichEntry(index) {
    const nextRows = collectLookupEnrichEntries().filter((_, rowIndex) => rowIndex !== index);
    renderLookupEnrichEntries(nextRows.length > 0 ? nextRows : [defaultLookupEnrichEntry()]);
}

function appendPruneFieldInput(field = "") {
    const nextRows = collectPruneFieldRows();
    nextRows.push({ field });
    renderPruneFieldRows(nextRows);
}

function removePruneFieldInput(index) {
    const nextRows = collectPruneFieldRows().filter((_, rowIndex) => rowIndex !== index);
    renderPruneFieldRows(nextRows);
}

function renderPruneFieldRows(rows) {
    const container = byId("prune-field-list");
    const template = byId("prune-field-item-template");
    if (!container || !template) {
        return;
    }

    container.innerHTML = "";
    const normalizedRows = Array.isArray(rows) && rows.length > 0 ? rows : [{ field: "" }];
    const showRemoveButton = normalizedRows.length > 1;
    normalizedRows.forEach(item => {
        const fragment = template.content.cloneNode(true);
        const fieldInput = fragment.querySelector("[data-role='prune-field-name']");
        const removeButton = fragment.querySelector("[data-role='remove-prune-field']");
        if (fieldInput) {
            fieldInput.value = item.field;
        }
        if (removeButton) {
            removeButton.classList.toggle("hidden", !showRemoveButton);
        }
        container.appendChild(fragment);
    });
}

function appendRenameMappingInput(sourceField = "", targetField = "") {
    const nextRows = collectRenameMappingRows();
    nextRows.push({ sourceField, targetField });
    renderRenameMappingRows(nextRows);
}

function removeRenameMappingInput(index) {
    const nextRows = collectRenameMappingRows().filter((_, rowIndex) => rowIndex !== index);
    renderRenameMappingRows(nextRows);
}

function renderRenameMappingRows(rows) {
    const container = byId("rename-mapping-list");
    const template = byId("rename-mapping-item-template");
    if (!container || !template) {
        return;
    }

    container.innerHTML = "";
    const normalizedRows = Array.isArray(rows) && rows.length > 0 ? rows : [{ sourceField: "", targetField: "" }];
    const showRemoveButton = normalizedRows.length > 1;
    normalizedRows.forEach(item => {
        const fragment = template.content.cloneNode(true);
        const sourceInput = fragment.querySelector("[data-role='rename-source-field']");
        const targetInput = fragment.querySelector("[data-role='rename-target-field']");
        const removeButton = fragment.querySelector("[data-role='remove-rename-mapping']");
        if (sourceInput) {
            sourceInput.value = item.sourceField;
        }
        if (targetInput) {
            targetInput.value = item.targetField;
        }
        if (removeButton) {
            removeButton.classList.toggle("hidden", !showRemoveButton);
        }
        container.appendChild(fragment);
    });
}

function normalizeAggregateItems(aggregations) {
    if (!Array.isArray(aggregations) || aggregations.length === 0) {
        return [{ function: "COUNT", field: "", outputField: "count" }];
    }

    const rows = aggregations
        .map(item => ({
            function: String(item?.function || "COUNT").toUpperCase(),
            field: item?.field == null ? "" : String(item.field),
            outputField: item?.outputField == null ? "" : String(item.outputField),
            sortField: item?.sortField == null ? "" : String(item.sortField),
            sortDirection: String(item?.sortDirection || "DESC").toUpperCase(),
            limit: item?.limit == null ? 10 : Number(item.limit)
        }))
        .filter(item => item.function || item.field || item.outputField);
    return rows.length > 0 ? rows : [{ function: "COUNT", field: "", outputField: "count" }];
}

function renderAggregateItems(aggregations) {
    renderAggregateRows(normalizeAggregateItems(aggregations));
}

function collectAggregateItems() {
    const container = byId("aggregate-item-list");
    if (!container) {
        return [{ function: "COUNT", field: "", outputField: "count" }];
    }

    const rows = Array.from(container.querySelectorAll("[data-role='aggregate-item']"))
        .map(item => ({
            function: item.querySelector("[data-role='aggregate-function']")?.value || "COUNT",
            field: item.querySelector("[data-role='aggregate-field']")?.value?.trim?.() || "",
            outputField: item.querySelector("[data-role='aggregate-output-field']")?.value?.trim?.() || "",
            sortField: item.querySelector("[data-role='aggregate-sort-field']")?.value?.trim?.() || "",
            sortDirection: item.querySelector("[data-role='aggregate-sort-direction']")?.value || "DESC",
            limit: Number(item.querySelector("[data-role='aggregate-limit']")?.value || 10)
        }))
        .filter(item => item.function || item.field || item.outputField);
    return rows.length > 0 ? rows : [{ function: "COUNT", field: "", outputField: "count" }];
}

function appendAggregateItem() {
    const nextRows = collectAggregateItems();
    nextRows.push({ function: "COUNT", field: "", outputField: "", limit: 10 });
    renderAggregateRows(nextRows);
}

function removeAggregateItem(index) {
    const nextRows = collectAggregateItems().filter((_, rowIndex) => rowIndex !== index);
    renderAggregateRows(nextRows.length > 0 ? nextRows : [{ function: "COUNT", field: "", outputField: "count" }]);
}

function updateAggregateItemVisibility(item) {
    const functionValue = item.querySelector("[data-role='aggregate-function']")?.value || "COUNT";
    const isTopN = functionValue === "TOP_N";
    item.querySelector("[data-role='aggregate-field-wrapper']")?.classList.toggle("hidden", functionValue === "COUNT");
    item.querySelector("[data-role='aggregate-sort-field-wrapper']")?.classList.toggle("hidden", !isTopN);
    item.querySelector("[data-role='aggregate-sort-direction-wrapper']")?.classList.toggle("hidden", !isTopN);
    item.querySelector("[data-role='aggregate-limit-wrapper']")?.classList.toggle("hidden", !isTopN);
}

function renderAggregateRows(rows) {
    const container = byId("aggregate-item-list");
    const template = byId("aggregate-item-template");
    if (!container || !template) {
        return;
    }

    container.innerHTML = "";
    const normalizedRows = normalizeAggregateItems(rows);
    const showRemoveButton = normalizedRows.length > 1;
    normalizedRows.forEach(item => {
        const fragment = template.content.cloneNode(true);
        const functionSelect = fragment.querySelector("[data-role='aggregate-function']");
        const fieldInput = fragment.querySelector("[data-role='aggregate-field']");
        const outputInput = fragment.querySelector("[data-role='aggregate-output-field']");
        const sortFieldInput = fragment.querySelector("[data-role='aggregate-sort-field']");
        const sortDirectionSelect = fragment.querySelector("[data-role='aggregate-sort-direction']");
        const limitInput = fragment.querySelector("[data-role='aggregate-limit']");
        const removeButton = fragment.querySelector("[data-role='remove-aggregate-item']");
        if (functionSelect) {
            functionSelect.value = item.function || "COUNT";
        }
        if (fieldInput) {
            fieldInput.value = item.field || "";
        }
        if (outputInput) {
            outputInput.value = item.outputField || "";
        }
        if (sortFieldInput) {
            sortFieldInput.value = item.sortField || "";
        }
        if (sortDirectionSelect) {
            sortDirectionSelect.value = item.sortDirection || "DESC";
        }
        if (limitInput) {
            limitInput.value = item.limit || 10;
        }
        if (removeButton) {
            removeButton.classList.toggle("hidden", !showRemoveButton);
        }
        container.appendChild(fragment);
        updateAggregateItemVisibility(container.lastElementChild);
    });
}

function normalizePreviewOutputRecords(records) {
    if (!Array.isArray(records) || records.length === 0) {
        return [];
    }
    return records.map(record => {
        if (record == null) {
            return "";
        }
        if (typeof record === "object") {
            return JSON.stringify(record, null, 2);
        }
        return String(record);
    });
}

function renderPreviewOutputRecords(records) {
    const template = byId("preview-output-item-template");
    const normalizedRecords = normalizePreviewOutputRecords(records);
    if (!template || normalizedRecords.length === 0) {
        return "";
    }

    const host = document.createElement("div");
    host.className = "space-y-3";
    normalizedRecords.forEach(record => {
        const fragment = template.content.cloneNode(true);
        const input = fragment.querySelector("[data-role='preview-output-record']");
        if (input) {
            input.textContent = record;
        }
        host.appendChild(fragment);
    });
    return host.innerHTML;
}

function collectSourceSampleInputs() {
    const container = byId("source-sample-list");
    if (!container) {
        return [""];
    }

    const values = Array.from(container.querySelectorAll("[data-role='source-sample-input']"))
        .map(input => input.value ?? "");
    return values.length > 0 ? values : [""];
}

function appendSourceSampleInput(value = "") {
    const nextSamples = collectSourceSampleInputs();
    nextSamples.push(value);
    renderSourceSampleInputs(nextSamples);
}

function removeSourceSampleInput(index) {
    const nextSamples = collectSourceSampleInputs().filter((_, sampleIndex) => sampleIndex !== index);
    renderSourceSampleInputs(nextSamples.length > 0 ? nextSamples : [""]);
}

function normalizeRouteItems(routes) {
    if (!Array.isArray(routes) || routes.length === 0) {
        return [{ portId: "matched", condition: "" }];
    }

    const rows = routes
        .map(route => ({
            portId: route?.portId == null ? "" : String(route.portId).trim(),
            condition: route?.condition == null ? "" : String(route.condition).trim()
        }))
        .filter(route => route.portId || route.condition);
    return rows.length > 0 ? rows : [{ portId: "matched", condition: "" }];
}

function renderRouteItems(routes) {
    const container = byId("route-rule-list");
    const template = byId("route-rule-template");
    if (!container || !template) {
        return;
    }

    container.innerHTML = "";
    const normalizedRoutes = normalizeRouteItems(routes);
    const showRemoveButton = normalizedRoutes.length > 1;
    normalizedRoutes.forEach((route, index) => {
        const fragment = template.content.cloneNode(true);
        const portInput = fragment.querySelector("[data-role='route-port-id']");
        const conditionInput = fragment.querySelector("[data-role='route-condition']");
        const moveUpButton = fragment.querySelector("[data-role='move-route-up']");
        const moveDownButton = fragment.querySelector("[data-role='move-route-down']");
        const removeButton = fragment.querySelector("[data-role='remove-route']");
        if (portInput) {
            portInput.value = route.portId;
        }
        if (conditionInput) {
            conditionInput.value = route.condition;
        }
        if (moveUpButton) {
            moveUpButton.disabled = index === 0;
        }
        if (moveDownButton) {
            moveDownButton.disabled = index === normalizedRoutes.length - 1;
        }
        if (removeButton) {
            removeButton.classList.toggle("hidden", !showRemoveButton);
        }
        container.appendChild(fragment);
    });
}

function collectRouteItems() {
    const container = byId("route-rule-list");
    if (!container) {
        return [{ portId: "matched", condition: "" }];
    }

    const rows = Array.from(container.querySelectorAll("[data-role='route-rule-item']"))
        .map(item => ({
            portId: item.querySelector("[data-role='route-port-id']")?.value?.trim?.() || "",
            condition: item.querySelector("[data-role='route-condition']")?.value?.trim?.() || ""
        }))
        .filter(route => route.portId || route.condition);
    return rows.length > 0 ? rows : [{ portId: "matched", condition: "" }];
}

function appendRouteItem() {
    const routes = collectRouteItems();
    routes.push({ portId: "", condition: "" });
    renderRouteItems(routes);
}

function removeRouteItem(index) {
    const routes = collectRouteItems().filter((_, routeIndex) => routeIndex !== index);
    renderRouteItems(routes.length > 0 ? routes : [{ portId: "matched", condition: "" }]);
}

function moveRouteItem(index, offset) {
    const routes = collectRouteItems();
    const nextIndex = index + offset;
    if (nextIndex < 0 || nextIndex >= routes.length) {
        return;
    }
    const [route] = routes.splice(index, 1);
    routes.splice(nextIndex, 0, route);
    renderRouteItems(routes);
}

function defaultMaskHashRule() {
    return { sourceField: "", targetField: "", action: "MASK", algorithm: "SHA256", salt: "", maskChar: "*", keepFirst: 3, keepLast: 4 };
}

function normalizeMaskHashAction(action) {
    return action === "HASH" ? "HASH" : "MASK";
}

function normalizeMaskHashRules(rules) {
    if (!Array.isArray(rules) || rules.length === 0) {
        return [defaultMaskHashRule()];
    }

    const rows = rules
        .map(rule => ({
            sourceField: rule?.sourceField == null ? "" : String(rule.sourceField).trim(),
            targetField: rule?.targetField == null ? "" : String(rule.targetField).trim(),
            action: normalizeMaskHashAction(rule?.action),
            algorithm: rule?.algorithm == null ? "SHA256" : String(rule.algorithm).trim() || "SHA256",
            salt: rule?.salt == null ? "" : String(rule.salt),
            maskChar: rule?.maskChar == null ? "*" : String(rule.maskChar || "*").slice(0, 1),
            keepFirst: rule?.keepFirst == null ? 3 : rule.keepFirst,
            keepLast: rule?.keepLast == null ? 4 : rule.keepLast
        }))
        .filter(rule => rule.sourceField || rule.targetField);
    return rows.length > 0 ? rows : [defaultMaskHashRule()];
}

function updateMaskHashActionUI(item) {
    const action = item.querySelector("[data-role='mask-hash-action']")?.value || "MASK";
    item.querySelector("[data-role='mask-hash-mask-fields']")?.classList.toggle("hidden", action !== "MASK");
    item.querySelector("[data-role='mask-hash-hash-fields']")?.classList.toggle("hidden", action !== "HASH");
}

function renderMaskHashRules(rules) {
    const container = byId("mask-hash-rule-list");
    const template = byId("mask-hash-rule-template");
    if (!container || !template) {
        return;
    }

    container.innerHTML = "";
    const normalizedRules = normalizeMaskHashRules(rules);
    const showRemoveButton = normalizedRules.length > 1;
    normalizedRules.forEach((rule, index) => {
        const fragment = template.content.cloneNode(true);
        const sourceFieldInput = fragment.querySelector("[data-role='mask-hash-source-field']");
        const targetFieldInput = fragment.querySelector("[data-role='mask-hash-target-field']");
        const actionSelect = fragment.querySelector("[data-role='mask-hash-action']");
        const maskCharInput = fragment.querySelector("[data-role='mask-hash-mask-char']");
        const keepFirstInput = fragment.querySelector("[data-role='mask-hash-keep-first']");
        const keepLastInput = fragment.querySelector("[data-role='mask-hash-keep-last']");
        const algorithmSelect = fragment.querySelector("[data-role='mask-hash-algorithm']");
        const saltInput = fragment.querySelector("[data-role='mask-hash-salt']");
        const moveUpButton = fragment.querySelector("[data-role='move-mask-hash-up']");
        const moveDownButton = fragment.querySelector("[data-role='move-mask-hash-down']");
        const removeButton = fragment.querySelector("[data-role='remove-mask-hash']");
        if (sourceFieldInput) {
            sourceFieldInput.value = rule.sourceField;
        }
        if (targetFieldInput) {
            targetFieldInput.value = rule.targetField;
        }
        if (actionSelect) {
            actionSelect.value = rule.action;
        }
        if (maskCharInput) {
            maskCharInput.value = rule.maskChar || "*";
        }
        if (keepFirstInput) {
            keepFirstInput.value = rule.keepFirst;
        }
        if (keepLastInput) {
            keepLastInput.value = rule.keepLast;
        }
        if (algorithmSelect) {
            algorithmSelect.value = rule.algorithm || "SHA256";
        }
        if (saltInput) {
            saltInput.value = rule.salt || "";
        }
        if (moveUpButton) {
            moveUpButton.disabled = index === 0;
        }
        if (moveDownButton) {
            moveDownButton.disabled = index === normalizedRules.length - 1;
        }
        if (removeButton) {
            removeButton.classList.toggle("hidden", !showRemoveButton);
        }
        container.appendChild(fragment);
        updateMaskHashActionUI(container.lastElementChild);
    });
}

function collectMaskHashRules() {
    const container = byId("mask-hash-rule-list");
    if (!container) {
        return [defaultMaskHashRule()];
    }

    const rows = Array.from(container.querySelectorAll("[data-role='mask-hash-rule-item']"))
        .map(item => {
            const sourceField = item.querySelector("[data-role='mask-hash-source-field']")?.value?.trim?.() || "";
            const targetField = item.querySelector("[data-role='mask-hash-target-field']")?.value?.trim?.() || "";
            const action = normalizeMaskHashAction(item.querySelector("[data-role='mask-hash-action']")?.value || "MASK");
            const algorithm = item.querySelector("[data-role='mask-hash-algorithm']")?.value || "SHA256";
            const salt = item.querySelector("[data-role='mask-hash-salt']")?.value || "";
            const maskChar = item.querySelector("[data-role='mask-hash-mask-char']")?.value || "";
            const keepFirstValue = item.querySelector("[data-role='mask-hash-keep-first']")?.value?.trim?.() || "";
            const keepLastValue = item.querySelector("[data-role='mask-hash-keep-last']")?.value?.trim?.() || "";
            if (action === "HASH") {
                return { sourceField, targetField, action, algorithm, salt };
            }
            return {
                sourceField,
                targetField,
                action,
                maskChar,
                keepFirst: keepFirstValue === "" ? "" : Number(keepFirstValue),
                keepLast: keepLastValue === "" ? "" : Number(keepLastValue)
            };
        })
        .filter(rule => rule.sourceField || rule.targetField);
    return rows.length > 0 ? rows : [defaultMaskHashRule()];
}

function appendMaskHashRule() {
    const rules = collectMaskHashRules();
    rules.push(defaultMaskHashRule());
    renderMaskHashRules(rules);
}

function removeMaskHashRule(index) {
    const rules = collectMaskHashRules().filter((_, ruleIndex) => ruleIndex !== index);
    renderMaskHashRules(rules.length > 0 ? rules : [defaultMaskHashRule()]);
}

function moveMaskHashRule(index, offset) {
    const rules = collectMaskHashRules();
    const nextIndex = index + offset;
    if (nextIndex < 0 || nextIndex >= rules.length) {
        return;
    }
    const [rule] = rules.splice(index, 1);
    rules.splice(nextIndex, 0, rule);
    renderMaskHashRules(rules);
}

function caseValueMode(item) {
    if (item?.valueMode) {
        return item.valueMode;
    }
    return Object.prototype.hasOwnProperty.call(item || {}, "expression") ? "EXPRESSION" : "VALUE";
}

function normalizeCaseWhenItems(cases) {
    if (!Array.isArray(cases) || cases.length === 0) {
        return [{ condition: "", valueMode: "VALUE", value: "", expression: "" }];
    }

    const rows = cases
        .map(item => ({
            condition: item?.condition == null ? "" : String(item.condition).trim(),
            valueMode: caseValueMode(item),
            value: item?.value == null ? "" : String(item.value),
            expression: item?.expression == null ? "" : String(item.expression).trim()
        }))
        .filter(item => item.condition || item.value || item.expression);
    return rows.length > 0 ? rows : [{ condition: "", valueMode: "VALUE", value: "", expression: "" }];
}

function updateCaseWhenValueModeUI(item) {
    const mode = item.querySelector("[data-role='case-when-value-mode']")?.value || "VALUE";
    item.querySelector("[data-role='case-when-value-wrapper']")?.classList.toggle("hidden", mode !== "VALUE");
    item.querySelector("[data-role='case-when-expression-wrapper']")?.classList.toggle("hidden", mode !== "EXPRESSION");
}

function renderCaseWhenItems(cases) {
    const container = byId("case-when-rule-list");
    const template = byId("case-when-rule-template");
    if (!container || !template) {
        return;
    }

    container.innerHTML = "";
    const normalizedCases = normalizeCaseWhenItems(cases);
    const showRemoveButton = normalizedCases.length > 1;
    normalizedCases.forEach((item, index) => {
        const fragment = template.content.cloneNode(true);
        const conditionInput = fragment.querySelector("[data-role='case-when-condition']");
        const modeSelect = fragment.querySelector("[data-role='case-when-value-mode']");
        const valueInput = fragment.querySelector("[data-role='case-when-value']");
        const expressionInput = fragment.querySelector("[data-role='case-when-expression']");
        const moveUpButton = fragment.querySelector("[data-role='move-case-when-up']");
        const moveDownButton = fragment.querySelector("[data-role='move-case-when-down']");
        const removeButton = fragment.querySelector("[data-role='remove-case-when']");
        if (conditionInput) {
            conditionInput.value = item.condition;
        }
        if (modeSelect) {
            modeSelect.value = item.valueMode || "VALUE";
        }
        if (valueInput) {
            valueInput.value = item.value || "";
        }
        if (expressionInput) {
            expressionInput.value = item.expression || "";
        }
        if (moveUpButton) {
            moveUpButton.disabled = index === 0;
        }
        if (moveDownButton) {
            moveDownButton.disabled = index === normalizedCases.length - 1;
        }
        if (removeButton) {
            removeButton.classList.toggle("hidden", !showRemoveButton);
        }
        container.appendChild(fragment);
        updateCaseWhenValueModeUI(container.lastElementChild);
    });
}

function collectCaseWhenItems() {
    const container = byId("case-when-rule-list");
    if (!container) {
        return [{ condition: "", value: "" }];
    }

    const rows = Array.from(container.querySelectorAll("[data-role='case-when-rule-item']"))
        .map(item => {
            const condition = item.querySelector("[data-role='case-when-condition']")?.value?.trim?.() || "";
            const valueMode = item.querySelector("[data-role='case-when-value-mode']")?.value || "VALUE";
            const value = item.querySelector("[data-role='case-when-value']")?.value || "";
            const expression = item.querySelector("[data-role='case-when-expression']")?.value?.trim?.() || "";
            return valueMode === "EXPRESSION"
                    ? { condition, expression }
                    : { condition, value };
        })
        .filter(item => item.condition || item.value || item.expression);
    return rows.length > 0 ? rows : [{ condition: "", value: "" }];
}

function appendCaseWhenItem() {
    const cases = collectCaseWhenItems();
    cases.push({ condition: "", value: "" });
    renderCaseWhenItems(cases);
}

function removeCaseWhenItem(index) {
    const cases = collectCaseWhenItems().filter((_, caseIndex) => caseIndex !== index);
    renderCaseWhenItems(cases.length > 0 ? cases : [{ condition: "", value: "" }]);
}

function moveCaseWhenItem(index, offset) {
    const cases = collectCaseWhenItems();
    const nextIndex = index + offset;
    if (nextIndex < 0 || nextIndex >= cases.length) {
        return;
    }
    const [item] = cases.splice(index, 1);
    cases.splice(nextIndex, 0, item);
    renderCaseWhenItems(cases);
}

function updateSinkFormatUI(format) {
    byId("sink-message-field-wrapper")?.classList.toggle("hidden", format !== "TEXT");
}

function updateSinkConfigSections(operator) {
    byId("kafka-sink-config-section")?.classList.toggle("hidden", operator !== "KAFKA_SINK");
    byId("jdbc-sink-config-section")?.classList.toggle("hidden", operator !== "JDBC_SINK");
    byId("elasticsearch-sink-config-section")?.classList.toggle("hidden", operator !== "ELASTICSEARCH_SINK");
    byId("influxdb-sink-config-section")?.classList.toggle("hidden", operator !== "INFLUXDB_SINK");
    byId("hdfs-file-sink-config-section")?.classList.toggle("hidden", operator !== "HDFS_FILE_SINK");
}

function updateJdbcSinkWriteModeUI(writeMode) {
    byId("jdbc-sink-key-fields-wrapper")?.classList.toggle("hidden", writeMode !== "UPSERT");
}

function updateHdfsFileSinkFilenameUI(customFilename) {
    byId("hdfs-file-sink-filename-config")?.classList.toggle("hidden", !customFilename);
}

function updateSourceConfigSections(operator) {
    byId("kafka-config-section")?.classList.toggle("hidden", operator !== "KAFKA_SOURCE");
    byId("jdbc-config-section")?.classList.toggle("hidden", operator !== "JDBC_SOURCE");
    byId("elasticsearch-config-section")?.classList.toggle("hidden", operator !== "ELASTICSEARCH_SOURCE");
    byId("influxdb-config-section")?.classList.toggle("hidden", operator !== "INFLUXDB_SOURCE");
    byId("hdfs-file-config-section")?.classList.toggle("hidden", operator !== "HDFS_FILE_SOURCE");
}

function updateJdbcReadModeUI(readMode) {
    byId("jdbc-incremental-config")?.classList.toggle("hidden", readMode !== "INCREMENTAL");
}

function updateElasticsearchReadModeUI(readMode) {
    byId("elasticsearch-incremental-config")?.classList.toggle("hidden", readMode !== "INCREMENTAL");
}

function updateInfluxDbReadModeUI(readMode) {
    byId("influxdb-incremental-config")?.classList.toggle("hidden", readMode !== "INCREMENTAL");
}

function updateHdfsFileReadModeUI(readMode) {
    byId("hdfs-file-incremental-config")?.classList.toggle("hidden", readMode !== "INCREMENTAL");
}

function updateElasticsearchAuthUI(authType) {
    byId("elasticsearch-basic-auth")?.classList.toggle("hidden", authType !== "BASIC");
    byId("elasticsearch-api-key-auth")?.classList.toggle("hidden", authType !== "API_KEY");
}

function updateDeserializeFormatUI(format) {
    byId("deserialize-field-names-wrapper")?.classList.toggle("hidden", format !== "CSV");
    byId("deserialize-delimiter-wrapper")?.classList.toggle("hidden", format !== "CSV");
}

function updateSerializeFormatUI(format) {
    byId("serialize-delimiter-wrapper")?.classList.toggle("hidden", format !== "CSV");
}

function updateAggregateConfigUI(mode, windowType, timeMode) {
    const isGrouped = mode === "GROUPED";
    const isCountWindow = windowType === "COUNT";
    const isSlidingTime = windowType === "SLIDING_TIME";
    const isEventTime = timeMode === "EVENT_TIME";
    byId("aggregate-group-by-wrapper")?.classList.toggle("hidden", !isGrouped);
    byId("aggregate-time-window-config")?.classList.toggle("hidden", isCountWindow);
    byId("aggregate-count-window-config")?.classList.toggle("hidden", !isCountWindow);
    byId("aggregate-window-slide-wrapper")?.classList.toggle("hidden", !isSlidingTime);
    byId("aggregate-event-time-config")?.classList.toggle("hidden", !isEventTime || isCountWindow);
    byId("aggregate-watermark-delay-wrapper")?.classList.toggle("hidden", !isEventTime || isCountWindow);
    updateAggregateOutputModeUI(byId("aggregate-output-mode")?.value || "NESTED");
}

function updateAggregateOutputModeUI(outputMode) {
    byId("aggregate-flat-output-config")?.classList.toggle("hidden", outputMode !== "FLAT");
}

function updateRouteConfigUI(includeUnmatched) {
    byId("route-unmatched-port-wrapper")?.classList.toggle("hidden", !includeUnmatched);
}

function updateCaseWhenDefaultModeUI(defaultMode) {
    byId("case-when-default-value-wrapper")?.classList.toggle("hidden", defaultMode !== "VALUE");
    byId("case-when-default-expression-wrapper")?.classList.toggle("hidden", defaultMode !== "EXPRESSION");
}

function updateSourceAuthUI(authType) {
    const showCredentials = authType !== "NONE";
    byId("source-auth-credentials")?.classList.toggle("hidden", !showCredentials);
    byId("source-scram-mechanism-wrapper")?.classList.toggle("hidden", authType !== "SASL_SCRAM");
}

function updateSinkAuthUI(authType) {
    const showCredentials = authType !== "NONE";
    byId("sink-auth-credentials")?.classList.toggle("hidden", !showCredentials);
    byId("sink-scram-mechanism-wrapper")?.classList.toggle("hidden", authType !== "SASL_SCRAM");
}

function updateElasticsearchSinkAuthUI(authType) {
    byId("elasticsearch-sink-basic-auth")?.classList.toggle("hidden", authType !== "BASIC");
    byId("elasticsearch-sink-api-key-auth")?.classList.toggle("hidden", authType !== "API_KEY");
    byId("elasticsearch-sink-api-key-encoded-auth")?.classList.toggle("hidden", authType !== "API_KEY_ENCODED");
}

function readKafkaAuthConfig(authTypeSelectId, usernameInputId, passwordInputId, scramSelectId) {
    const authType = byId(authTypeSelectId)?.value || "NONE";
    const username = byId(usernameInputId)?.value?.trim?.() || "";
    const password = byId(passwordInputId)?.value || "";
    const scramMechanism = byId(scramSelectId)?.value || "";

    if (authType === "NONE") {
        return { authType, username: "", password: "", scramMechanism: "" };
    }
    if (authType === "SASL_PLAIN") {
        return { authType, username, password, scramMechanism: "" };
    }
    return { authType, username, password, scramMechanism };
}

function syncSelectedNodeConfigFromInspector() {
    const selectedNode = findNodeById(state.selectedNodeId);
    if (!selectedNode) {
        return;
    }

    selectedNode.displayName = byId("node-display-name")?.value?.trim?.() || "";

    if (selectedNode.type === "SOURCE") {
        selectedNode.config.sampleData = collectSourceSampleInputs();
        if (selectedNode.operator === "JDBC_SOURCE") {
            selectedNode.config.url = byId("jdbc-url")?.value?.trim?.() || "";
            selectedNode.config.driver = byId("jdbc-driver")?.value?.trim?.() || "";
            selectedNode.config.username = byId("jdbc-username")?.value?.trim?.() || "";
            selectedNode.config.password = byId("jdbc-password")?.value || "";
            selectedNode.config.query = byId("jdbc-query")?.value?.trim?.() || "";
            selectedNode.config.tablePath = byId("jdbc-table-path")?.value?.trim?.() || "";
            selectedNode.config.readMode = byId("jdbc-read-mode")?.value || "FULL";
            selectedNode.config.cursorField = byId("jdbc-cursor-field")?.value?.trim?.() || "";
            selectedNode.config.cursorType = byId("jdbc-cursor-type")?.value || "STRING";
            selectedNode.config.initialCursorValue = byId("jdbc-initial-cursor-value")?.value?.trim?.() || "";
            selectedNode.config.pollIntervalMillis = Number(byId("jdbc-poll-interval-millis")?.value || 5000);
            selectedNode.config.fetchSize = Number(byId("jdbc-fetch-size")?.value || 1000);
            selectedNode.config.maxPolls = Number(byId("jdbc-max-polls")?.value || 0);
            selectedNode.config.idField = byId("jdbc-id-field")?.value?.trim?.() || "";
            selectedNode.config.timestampField = byId("jdbc-timestamp-field")?.value?.trim?.() || "";
            selectedNode.config.format = "JSON";
        } else if (selectedNode.operator === "ELASTICSEARCH_SOURCE") {
            selectedNode.config.hosts = splitTopics(byId("elasticsearch-hosts")?.value || "");
            selectedNode.config.index = byId("elasticsearch-index")?.value?.trim?.() || "";
            selectedNode.config.source = splitTopics(byId("elasticsearch-source-fields")?.value || "");
            selectedNode.config.query = parseJsonValue(byId("elasticsearch-query")?.value || "{}", {});
            selectedNode.config.readMode = byId("elasticsearch-read-mode")?.value || "FULL";
            selectedNode.config.cursorField = byId("elasticsearch-cursor-field")?.value?.trim?.() || "";
            selectedNode.config.cursorType = byId("elasticsearch-cursor-type")?.value || "STRING";
            selectedNode.config.initialCursorValue = byId("elasticsearch-initial-cursor-value")?.value?.trim?.() || "";
            selectedNode.config.pollIntervalMillis = Number(byId("elasticsearch-poll-interval-millis")?.value || 5000);
            selectedNode.config.scrollSize = Number(byId("elasticsearch-scroll-size")?.value || 100);
            selectedNode.config.scrollTime = byId("elasticsearch-scroll-time")?.value?.trim?.() || "1m";
            selectedNode.config.maxPolls = Number(byId("elasticsearch-max-polls")?.value || 0);
            selectedNode.config.idField = byId("elasticsearch-id-field")?.value?.trim?.() || "";
            selectedNode.config.timestampField = byId("elasticsearch-timestamp-field")?.value?.trim?.() || "";
            selectedNode.config.authType = byId("elasticsearch-auth-type")?.value || "NONE";
            selectedNode.config.username = selectedNode.config.authType === "BASIC"
                ? byId("elasticsearch-username")?.value?.trim?.() || ""
                : "";
            selectedNode.config.password = selectedNode.config.authType === "BASIC"
                ? byId("elasticsearch-password")?.value || ""
                : "";
            selectedNode.config.apiKey = selectedNode.config.authType === "API_KEY"
                ? byId("elasticsearch-api-key")?.value?.trim?.() || ""
                : "";
            selectedNode.config.format = "JSON";
        } else if (selectedNode.operator === "INFLUXDB_SOURCE") {
            selectedNode.config.url = byId("influxdb-url")?.value?.trim?.() || "";
            selectedNode.config.database = byId("influxdb-database")?.value?.trim?.() || "";
            selectedNode.config.sql = byId("influxdb-sql")?.value?.trim?.() || "";
            selectedNode.config.schema = parseJsonValue(byId("influxdb-schema")?.value || "{}", {});
            selectedNode.config.epoch = byId("influxdb-epoch")?.value?.trim?.() || "ms";
            selectedNode.config.queryTimeoutSeconds = Number(byId("influxdb-query-timeout-seconds")?.value || 30);
            selectedNode.config.connectTimeoutMillis = Number(byId("influxdb-connect-timeout-millis")?.value || 10000);
            selectedNode.config.readMode = byId("influxdb-read-mode")?.value || "FULL";
            selectedNode.config.cursorField = byId("influxdb-cursor-field")?.value?.trim?.() || "";
            selectedNode.config.cursorType = byId("influxdb-cursor-type")?.value || "STRING";
            selectedNode.config.initialCursorValue = byId("influxdb-initial-cursor-value")?.value?.trim?.() || "";
            selectedNode.config.pollIntervalMillis = Number(byId("influxdb-poll-interval-millis")?.value || 5000);
            selectedNode.config.maxPolls = Number(byId("influxdb-max-polls")?.value || 0);
            selectedNode.config.idField = byId("influxdb-id-field")?.value?.trim?.() || "";
            selectedNode.config.timestampField = byId("influxdb-timestamp-field")?.value?.trim?.() || "";
            selectedNode.config.username = byId("influxdb-username")?.value?.trim?.() || "";
            selectedNode.config.password = byId("influxdb-password")?.value || "";
            selectedNode.config.format = "JSON";
        } else if (selectedNode.operator === "HDFS_FILE_SOURCE") {
            selectedNode.config["fs.defaultFS"] = byId("hdfs-file-default-fs")?.value?.trim?.() || "";
            selectedNode.config.path = byId("hdfs-file-path")?.value?.trim?.() || "";
            selectedNode.config.file_format_type = byId("hdfs-file-format-type")?.value || "JSON";
            selectedNode.config.schema = parseJsonValue(byId("hdfs-file-schema")?.value || "{}", {});
            selectedNode.config.read_columns = splitTopics(byId("hdfs-file-read-columns")?.value || "");
            selectedNode.config.field_delimiter = byId("hdfs-file-field-delimiter")?.value || "\\001";
            selectedNode.config.row_delimiter = byId("hdfs-file-row-delimiter")?.value || "\\n";
            selectedNode.config.skip_header_row_number = Number(byId("hdfs-file-skip-header-row-number")?.value || 0);
            selectedNode.config.csv_use_header_line = Boolean(byId("hdfs-file-csv-use-header-line")?.checked);
            selectedNode.config.encoding = byId("hdfs-file-encoding")?.value?.trim?.() || "UTF-8";
            selectedNode.config.compress_codec = byId("hdfs-file-compress-codec")?.value?.trim?.() || "none";
            selectedNode.config.parse_partition_from_path = Boolean(byId("hdfs-file-parse-partition-from-path")?.checked);
            selectedNode.config.file_filter_pattern = byId("hdfs-file-filter-pattern")?.value?.trim?.() || "";
            selectedNode.config.readMode = byId("hdfs-file-read-mode")?.value || "FULL";
            selectedNode.config.pollIntervalMillis = Number(byId("hdfs-file-poll-interval-millis")?.value || 5000);
            selectedNode.config.maxPolls = Number(byId("hdfs-file-max-polls")?.value || 0);
            selectedNode.config.idField = byId("hdfs-file-id-field")?.value?.trim?.() || "";
            selectedNode.config.timestampField = byId("hdfs-file-timestamp-field")?.value?.trim?.() || "";
            selectedNode.config.hdfs_site_path = byId("hdfs-file-hdfs-site-path")?.value?.trim?.() || "";
            selectedNode.config.kerberos_principal = byId("hdfs-file-kerberos-principal")?.value?.trim?.() || "";
            selectedNode.config.kerberos_keytab_path = byId("hdfs-file-kerberos-keytab-path")?.value?.trim?.() || "";
            selectedNode.config.format = selectedNode.config.file_format_type;
        } else {
            selectedNode.config.bootstrapServers = byId("source-bootstrap-servers")?.value?.trim?.() || "";
            selectedNode.config.topics = splitTopics(byId("source-topics")?.value || "");
            selectedNode.config.groupId = byId("source-group-id")?.value?.trim?.() || "";
            selectedNode.config.consumeMode = byId("source-consume-mode")?.value || "earliest";
            const authConfig = readKafkaAuthConfig(
                "source-auth-type",
                "source-auth-username",
                "source-auth-password",
                "source-scram-mechanism"
            );
            Object.assign(selectedNode.config, authConfig);
            selectedNode.config.format = byId("source-format")?.value || "JSON";
        }
    } else if (selectedNode.type === "TRANSFORMER") {
        switch (selectedNode.operator) {
            case "PUT":
                selectedNode.config.field = byId("put-field")?.value?.trim?.() || "";
                selectedNode.config.valueMode = byId("put-value-mode")?.value || "LITERAL";
                selectedNode.config.referenceField = byId("put-reference-field")?.value?.trim?.() || "";
                selectedNode.config.template = byId("put-template-value")?.value || "";
                selectedNode.config.value = putStoredValue(selectedNode.config.valueMode,
                        byId("put-literal-value")?.value?.trim?.() || "",
                        selectedNode.config.referenceField,
                        selectedNode.config.template);
                break;
            case "PRUNE":
                selectedNode.config.fields = collectPruneFieldInputs();
                break;
            case "RENAME":
                selectedNode.config.mapping = collectRenameMappingInputs();
                break;
            case "DESERIALIZE":
                selectedNode.config.field = byId("deserialize-field")?.value?.trim?.() || "";
                selectedNode.config.targetField = byId("deserialize-target-field")?.value?.trim?.() || "";
                selectedNode.config.format = byId("deserialize-format")?.value || "JSON";
                selectedNode.config.fieldNames = splitTopics(byId("deserialize-field-names")?.value || "");
                selectedNode.config.delimiter = byId("deserialize-delimiter")?.value || ",";
                break;
            case "SERIALIZE":
                selectedNode.config.sourceFields = splitTopics(byId("serialize-source-fields")?.value || "");
                selectedNode.config.targetField = byId("serialize-target-field")?.value?.trim?.() || "";
                selectedNode.config.format = byId("serialize-format")?.value || "JSON";
                selectedNode.config.delimiter = byId("serialize-delimiter")?.value || ",";
                break;
            case "FILTER":
                selectedNode.config.condition = byId("filter-condition")?.value?.trim?.() || "";
                break;
            case "GROK":
                selectedNode.config.inputField = byId("grok-input-field")?.value?.trim?.() || "";
                selectedNode.config.outputField = byId("grok-output-field")?.value?.trim?.() || "";
                selectedNode.config.pattern = byId("grok-pattern")?.value || "";
                break;
            case "CAST":
                selectedNode.config.inputField = byId("cast-input-field")?.value?.trim?.() || "";
                selectedNode.config.outputMode = byId("cast-output-mode")?.value || "OVERWRITE";
                selectedNode.config.outputField = castStoredOutputField(
                        selectedNode.config.inputField,
                        selectedNode.config.outputMode,
                        byId("cast-output-field")?.value?.trim?.() || "");
                selectedNode.config.targetType = byId("cast-target-type")?.value || "STRING";
                break;
            case "EVAL":
                selectedNode.config.targetField = byId("eval-target-field")?.value?.trim?.() || "";
                selectedNode.config.expression = byId("eval-expression")?.value?.trim?.() || "";
                selectedNode.config.outputMode = byId("eval-output-mode")?.value || "OVERWRITE";
                selectedNode.config.errorStrategy = byId("eval-error-strategy")?.value || "KEEP_ORIGINAL";
                break;
            case "DEDUPLICATE":
                selectedNode.config.keyFields = splitTopics(byId("deduplicate-key-fields")?.value || "");
                selectedNode.config.timeMode = byId("deduplicate-time-mode")?.value || "PROCESSING_TIME";
                selectedNode.config.ttlSeconds = Number(byId("deduplicate-ttl-seconds")?.value || 3600);
                selectedNode.config.eventTimeField = byId("deduplicate-event-time-field")?.value?.trim?.() || "";
                selectedNode.config.windowSeconds = Number(byId("deduplicate-window-seconds")?.value || 300);
                selectedNode.config.watermarkDelaySeconds = Number(byId("deduplicate-watermark-delay-seconds")?.value || 30);
                selectedNode.config.keepStrategy = byId("deduplicate-keep-strategy")?.value || "FIRST";
                selectedNode.config.lateDataStrategy = byId("deduplicate-late-data-strategy")?.value || "DISCARD";
                selectedNode.config.duplicateStrategy = byId("deduplicate-duplicate-strategy")?.value || "DISCARD";
                break;
            case "LOOKUP_ENRICH":
                selectedNode.config.sourceField = byId("lookup-enrich-source-field")?.value?.trim?.() || "";
                selectedNode.config.targetField = byId("lookup-enrich-target-field")?.value?.trim?.() || "";
                selectedNode.config.entries = collectLookupEnrichEntries();
                selectedNode.config.missingStrategy = byId("lookup-enrich-missing-strategy")?.value || "KEEP_ORIGINAL";
                selectedNode.config.overwriteTargetField = Boolean(byId("lookup-enrich-overwrite-target-field")?.checked);
                break;
            case "LOOKUP_JOIN":
                selectedNode.config.sourceField = byId("lookup-join-source-field")?.value?.trim?.() || "";
                selectedNode.config.targetField = byId("lookup-join-target-field")?.value?.trim?.() || "";
                selectedNode.config.joinType = byId("lookup-join-type")?.value || "LEFT";
                selectedNode.config.missingStrategy = byId("lookup-join-missing-strategy")?.value || "KEEP_ORIGINAL";
                selectedNode.config.overwriteTargetField = Boolean(byId("lookup-join-overwrite-target-field")?.checked);
                selectedNode.config.entries = collectLookupJoinEntries();
                break;
            case "STREAM_JOIN":
                selectedNode.config.leftKeyField = byId("stream-join-left-key-field")?.value?.trim?.() || "";
                selectedNode.config.rightKeyField = byId("stream-join-right-key-field")?.value?.trim?.() || "";
                selectedNode.config.targetField = byId("stream-join-target-field")?.value?.trim?.() || "";
                selectedNode.config.joinType = byId("stream-join-type")?.value || "LEFT";
                selectedNode.config.missingStrategy = byId("stream-join-missing-strategy")?.value || "KEEP_ORIGINAL";
                selectedNode.config.overwriteTargetField = Boolean(byId("stream-join-overwrite-target-field")?.checked);
                selectedNode.config.timeMode = byId("stream-join-time-mode")?.value || "PROCESSING_TIME";
                selectedNode.config.timeUnit = byId("stream-join-time-unit")?.value || "SECONDS";
                selectedNode.config.windowBefore = Number(byId("stream-join-window-before")?.value || 60);
                selectedNode.config.windowAfter = Number(byId("stream-join-window-after")?.value || 60);
                selectedNode.config.watermarkDelay = Number(byId("stream-join-watermark-delay")?.value || 30);
                selectedNode.config.lateDataStrategy = byId("stream-join-late-data-strategy")?.value || "DROP";
                break;
            case "FLATTEN":
                selectedNode.config.sourceField = byId("flatten-source-field")?.value?.trim?.() || "";
                selectedNode.config.targetPrefix = byId("flatten-target-prefix")?.value?.trim?.() || "";
                selectedNode.config.delimiter = byId("flatten-delimiter")?.value || "_";
                selectedNode.config.removeSourceField = Boolean(byId("flatten-remove-source-field")?.checked);
                break;
            case "EXPLODE":
                selectedNode.config.sourceField = byId("explode-source-field")?.value?.trim?.() || "";
                selectedNode.config.targetField = byId("explode-target-field")?.value?.trim?.() || "";
                selectedNode.config.keepEmpty = Boolean(byId("explode-keep-empty")?.checked);
                break;
            case "DATA_QUALITY":
                selectedNode.config.mode = byId("data-quality-mode")?.value || "DIRTY_PORT";
                selectedNode.config.errorField = byId("data-quality-error-field")?.value?.trim?.() || "_streamcraft_quality_errors";
                selectedNode.config.rules = collectDataQualityRules().map(rule => {
                    const nextRule = {
                        field: rule.field,
                        ruleType: rule.ruleType || "NOT_NULL",
                        customMessage: rule.customMessage || ""
                    };
                    if (rule.ruleType === "TYPE") {
                        nextRule.valueType = rule.valueType || "STRING";
                    }
                    if (rule.ruleType === "RANGE") {
                        if (rule.min !== "") nextRule.min = Number(rule.min);
                        if (rule.max !== "") nextRule.max = Number(rule.max);
                    }
                    if (rule.ruleType === "LENGTH") {
                        if (rule.minLength !== "") nextRule.minLength = Number(rule.minLength);
                        if (rule.maxLength !== "") nextRule.maxLength = Number(rule.maxLength);
                    }
                    if (rule.ruleType === "ENUM") {
                        nextRule.enumValues = splitTopics(rule.enumValues || "");
                    }
                    if (rule.ruleType === "REGEX") {
                        nextRule.pattern = rule.pattern || "";
                    }
                    return nextRule;
                });
                break;
            case "TIME_DERIVE":
                selectedNode.config.sourceField = byId("time-derive-source-field")?.value?.trim?.() || "";
                selectedNode.config.sourceFormat = byId("time-derive-source-format")?.value || "AUTO";
                selectedNode.config.sourcePattern = byId("time-derive-source-pattern")?.value?.trim?.() || "";
                selectedNode.config.sourceTimeZone = byId("time-derive-source-time-zone")?.value?.trim?.() || "UTC";
                selectedNode.config.outputTimeZone = byId("time-derive-output-time-zone")?.value?.trim?.() || "UTC";
                selectedNode.config.parseErrorStrategy = byId("time-derive-parse-error-strategy")?.value || "KEEP_ORIGINAL";
                selectedNode.config.derivations = collectTimeDeriveItems();
                break;
            case "MASK_HASH":
                selectedNode.config.rules = collectMaskHashRules();
                break;
            case "CASE_WHEN":
                selectedNode.config.targetField = byId("case-when-target-field")?.value?.trim?.() || "";
                selectedNode.config.cases = collectCaseWhenItems();
                const caseWhenDefaultMode = byId("case-when-default-mode")?.value || "NONE";
                selectedNode.config.defaultMode = caseWhenDefaultMode;
                selectedNode.config.defaultValue = caseWhenDefaultMode === "VALUE" ? byId("case-when-default-value")?.value || "" : "";
                selectedNode.config.defaultExpression = caseWhenDefaultMode === "EXPRESSION" ? byId("case-when-default-expression")?.value?.trim?.() || "" : "";
                break;
            case "ROUTE":
                selectedNode.config.matchMode = byId("route-match-mode")?.value || "FIRST_MATCH";
                selectedNode.config.includeUnmatched = Boolean(byId("route-include-unmatched")?.checked);
                selectedNode.config.unmatchedPort = byId("route-unmatched-port")?.value?.trim?.() || "unmatched";
                selectedNode.config.routes = collectRouteItems();
                break;
            case "AGGREGATE":
                selectedNode.config.mode = byId("aggregate-mode")?.value || "GLOBAL";
                selectedNode.config.groupBy = splitTopics(byId("aggregate-group-by")?.value || "");
                selectedNode.config.windowType = byId("aggregate-window-type")?.value || "TUMBLING_TIME";
                selectedNode.config.timeMode = byId("aggregate-time-mode")?.value || "PROCESSING_TIME";
                selectedNode.config.timeUnit = byId("aggregate-time-unit")?.value || "SECONDS";
                selectedNode.config.windowSize = Number(byId("aggregate-window-size")?.value || 60);
                selectedNode.config.windowSlide = Number(byId("aggregate-window-slide")?.value || 10);
                selectedNode.config.watermarkDelay = Number(byId("aggregate-watermark-delay")?.value || 30);
                selectedNode.config.eventTimeField = byId("aggregate-event-time-field")?.value?.trim?.() || "";
                selectedNode.config.eventTimeUnit = byId("aggregate-event-time-unit")?.value || "MILLISECONDS";
                selectedNode.config.outputMode = byId("aggregate-output-mode")?.value || "NESTED";
                selectedNode.config.windowStartField = byId("aggregate-window-start-field")?.value?.trim?.() || "windowStart";
                selectedNode.config.windowEndField = byId("aggregate-window-end-field")?.value?.trim?.() || "windowEnd";
                selectedNode.config.countWindowSize = Number(byId("aggregate-count-window-size")?.value || 100);
                selectedNode.config.aggregations = collectAggregateItems();
                break;
            case "CUSTOM_CODE":
                selectedNode.config.language = "JAVA";
                selectedNode.config.compilePattern = "SOURCE_CODE";
                selectedNode.config.className = byId("custom-code-class-name")?.value?.trim?.() || "";
                selectedNode.config.sourceCode = byId("custom-code-source")?.value || "";
                selectedNode.config.errorStrategy = byId("custom-code-error-strategy")?.value || "KEEP_ORIGINAL";
                break;
            default:
                break;
        }
        selectedNode.config.note = byId("transform-note")?.value?.trim?.() || "";
    } else if (selectedNode.type === "SINK") {
        if (selectedNode.operator === "JDBC_SINK") {
            selectedNode.config.url = byId("jdbc-sink-url")?.value?.trim?.() || "";
            selectedNode.config.driver = byId("jdbc-sink-driver")?.value?.trim?.() || "";
            selectedNode.config.username = byId("jdbc-sink-username")?.value?.trim?.() || "";
            selectedNode.config.password = byId("jdbc-sink-password")?.value || "";
            selectedNode.config.tablePath = byId("jdbc-sink-table-path")?.value?.trim?.() || "";
            selectedNode.config.writeMode = byId("jdbc-sink-write-mode")?.value || "INSERT";
            selectedNode.config.fields = splitTopics(byId("jdbc-sink-fields")?.value || "");
            selectedNode.config.keyFields = splitTopics(byId("jdbc-sink-key-fields")?.value || "");
            selectedNode.config.batchSize = Number(byId("jdbc-sink-batch-size")?.value || 500);
            selectedNode.config.flushIntervalMillis = Number(byId("jdbc-sink-flush-interval-millis")?.value || 5000);
        } else if (selectedNode.operator === "ELASTICSEARCH_SINK") {
            selectedNode.config.hosts = splitTopics(byId("elasticsearch-sink-hosts")?.value || "");
            selectedNode.config.index = byId("elasticsearch-sink-index")?.value?.trim?.() || "";
            selectedNode.config.indexType = byId("elasticsearch-sink-index-type")?.value?.trim?.() || "";
            selectedNode.config.primaryKeys = splitTopics(byId("elasticsearch-sink-primary-keys")?.value || "");
            selectedNode.config.keyDelimiter = byId("elasticsearch-sink-key-delimiter")?.value || "_";
            selectedNode.config.fields = splitTopics(byId("elasticsearch-sink-fields")?.value || "");
            selectedNode.config.maxBatchSize = Number(byId("elasticsearch-sink-max-batch-size")?.value || 10);
            selectedNode.config.flushIntervalMillis = Number(byId("elasticsearch-sink-flush-interval-millis")?.value || 5000);
            selectedNode.config.maxRetryCount = Number(byId("elasticsearch-sink-max-retry-count")?.value || 3);
            selectedNode.config.authType = byId("elasticsearch-sink-auth-type")?.value || "NONE";
            selectedNode.config.username = selectedNode.config.authType === "BASIC"
                ? byId("elasticsearch-sink-username")?.value?.trim?.() || ""
                : "";
            selectedNode.config.password = selectedNode.config.authType === "BASIC"
                ? byId("elasticsearch-sink-password")?.value || ""
                : "";
            selectedNode.config.apiKeyId = selectedNode.config.authType === "API_KEY"
                ? byId("elasticsearch-sink-api-key-id")?.value?.trim?.() || ""
                : "";
            selectedNode.config.apiKey = selectedNode.config.authType === "API_KEY"
                ? byId("elasticsearch-sink-api-key")?.value?.trim?.() || ""
                : "";
            selectedNode.config.apiKeyEncoded = selectedNode.config.authType === "API_KEY_ENCODED"
                ? byId("elasticsearch-sink-api-key-encoded")?.value?.trim?.() || ""
                : "";
        } else if (selectedNode.operator === "INFLUXDB_SINK") {
            selectedNode.config.url = byId("influxdb-sink-url")?.value?.trim?.() || "";
            selectedNode.config.database = byId("influxdb-sink-database")?.value?.trim?.() || "";
            selectedNode.config.measurement = byId("influxdb-sink-measurement")?.value?.trim?.() || "";
            selectedNode.config.keyTime = byId("influxdb-sink-key-time")?.value?.trim?.() || "time";
            selectedNode.config.keyTags = splitTopics(byId("influxdb-sink-key-tags")?.value || "");
            selectedNode.config.fields = splitTopics(byId("influxdb-sink-fields")?.value || "");
            selectedNode.config.batchSize = Number(byId("influxdb-sink-batch-size")?.value || 100);
            selectedNode.config.maxRetries = Number(byId("influxdb-sink-max-retries")?.value || 3);
            selectedNode.config.retryBackoffMultiplierMillis = Number(byId("influxdb-sink-retry-backoff-multiplier-millis")?.value || 100);
            selectedNode.config.maxRetryBackoffMillis = Number(byId("influxdb-sink-max-retry-backoff-millis")?.value || 1000);
            selectedNode.config.connectTimeoutMillis = Number(byId("influxdb-sink-connect-timeout-millis")?.value || 10000);
            selectedNode.config.flushIntervalMillis = Number(byId("influxdb-sink-flush-interval-millis")?.value || 5000);
            selectedNode.config.precision = byId("influxdb-sink-precision")?.value || "ms";
            selectedNode.config.username = byId("influxdb-sink-username")?.value?.trim?.() || "";
            selectedNode.config.password = byId("influxdb-sink-password")?.value || "";
        } else if (selectedNode.operator === "HDFS_FILE_SINK") {
            selectedNode.config["fs.defaultFS"] = byId("hdfs-file-sink-default-fs")?.value?.trim?.() || "";
            selectedNode.config.path = byId("hdfs-file-sink-path")?.value?.trim?.() || "";
            selectedNode.config.tmp_path = byId("hdfs-file-sink-tmp-path")?.value?.trim?.() || "";
            selectedNode.config.file_format_type = byId("hdfs-file-sink-format-type")?.value || "JSON";
            selectedNode.config.sink_columns = splitTopics(byId("hdfs-file-sink-columns")?.value || "");
            selectedNode.config.partition_by = splitTopics(byId("hdfs-file-sink-partition-by")?.value || "");
            selectedNode.config.partition_dir_expression = byId("hdfs-file-sink-partition-dir-expression")?.value?.trim?.() || "";
            selectedNode.config.is_partition_field_write_in_file = Boolean(byId("hdfs-file-sink-partition-field-write-in-file")?.checked);
            selectedNode.config.custom_filename = Boolean(byId("hdfs-file-sink-custom-filename")?.checked);
            selectedNode.config.file_name_expression = byId("hdfs-file-sink-file-name-expression")?.value?.trim?.() || "part-${now}";
            selectedNode.config.filename_time_format = byId("hdfs-file-sink-filename-time-format")?.value?.trim?.() || "yyyyMMddHHmmss";
            selectedNode.config.batch_size = Number(byId("hdfs-file-sink-batch-size")?.value || 1000);
            selectedNode.config.flushIntervalMillis = Number(byId("hdfs-file-sink-flush-interval-millis")?.value || 5000);
            selectedNode.config.field_delimiter = byId("hdfs-file-sink-field-delimiter")?.value || "\\001";
            selectedNode.config.row_delimiter = byId("hdfs-file-sink-row-delimiter")?.value || "\\n";
            selectedNode.config.csv_use_header_line = Boolean(byId("hdfs-file-sink-csv-use-header-line")?.checked);
            selectedNode.config.encoding = byId("hdfs-file-sink-encoding")?.value?.trim?.() || "UTF-8";
            selectedNode.config.compress_codec = byId("hdfs-file-sink-compress-codec")?.value?.trim?.() || "none";
            selectedNode.config.hdfs_site_path = byId("hdfs-file-sink-hdfs-site-path")?.value?.trim?.() || "";
            selectedNode.config.kerberos_principal = byId("hdfs-file-sink-kerberos-principal")?.value?.trim?.() || "";
            selectedNode.config.kerberos_keytab_path = byId("hdfs-file-sink-kerberos-keytab-path")?.value?.trim?.() || "";
        } else {
            selectedNode.config.bootstrapServers = byId("sink-bootstrap-servers")?.value?.trim?.() || "";
            selectedNode.config.topic = byId("sink-topic")?.value?.trim?.() || "";
            selectedNode.config.deliveryGuarantee = byId("sink-delivery-guarantee")?.value || "AT_LEAST_ONCE";
            const authConfig = readKafkaAuthConfig(
                "sink-auth-type",
                "sink-auth-username",
                "sink-auth-password",
                "sink-scram-mechanism"
            );
            Object.assign(selectedNode.config, authConfig);
            selectedNode.config.format = byId("sink-format")?.value || "JSON";
            selectedNode.config.messageField = byId("sink-message-field")?.value?.trim?.() || "_streamcraft_message";
        }
    }
}

function fillMetaForm() {
    if (byId("pipeline-name")) {
        byId("pipeline-name").value = state.pipelineMeta.name || "";
    }
    if (byId("pipeline-description")) {
        byId("pipeline-description").value = state.pipelineMeta.description || "";
    }
    updateRuntimeResourceControls();
}

function fillInspectorFromSelectedNode() {
    const selectedNode = findNodeById(state.selectedNodeId);
    if (!selectedNode) {
        return;
    }

    if (byId("node-display-name")) {
        byId("node-display-name").value = selectedNode.displayName || "";
    }

    if (selectedNode.type === "SOURCE") {
        renderSourceSampleInputs(selectedNode.config.sampleData || [""]);
        updateSourceConfigSections(selectedNode.operator);
        if (byId("source-bootstrap-servers")) {
            byId("source-bootstrap-servers").value = selectedNode.config.bootstrapServers || "";
        }
        if (byId("source-topics")) {
            byId("source-topics").value = (selectedNode.config.topics || []).join(",");
        }
        if (byId("source-group-id")) {
            byId("source-group-id").value = selectedNode.config.groupId || "";
        }
        if (byId("source-consume-mode")) {
            byId("source-consume-mode").value = selectedNode.config.consumeMode || "earliest";
            refreshStudioSelectValue("source-consume-mode");
        }
        if (byId("source-auth-type")) {
            byId("source-auth-type").value = selectedNode.config.authType || "NONE";
            refreshStudioSelectValue("source-auth-type");
        }
        if (byId("source-auth-username")) {
            byId("source-auth-username").value = selectedNode.config.username || "";
        }
        if (byId("source-auth-password")) {
            byId("source-auth-password").value = selectedNode.config.password || "";
        }
        if (byId("source-scram-mechanism")) {
            byId("source-scram-mechanism").value = selectedNode.config.scramMechanism || "SCRAM-SHA-512";
            refreshStudioSelectValue("source-scram-mechanism");
        }
        if (byId("source-format")) {
            byId("source-format").value = selectedNode.config.format || "JSON";
            refreshStudioSelectValue("source-format");
        }
        if (byId("jdbc-url")) {
            byId("jdbc-url").value = selectedNode.config.url || "";
        }
        if (byId("jdbc-driver")) {
            byId("jdbc-driver").value = selectedNode.config.driver || "";
        }
        if (byId("jdbc-username")) {
            byId("jdbc-username").value = selectedNode.config.username || "";
        }
        if (byId("jdbc-password")) {
            byId("jdbc-password").value = selectedNode.config.password || "";
        }
        if (byId("jdbc-query")) {
            byId("jdbc-query").value = selectedNode.config.query || "";
        }
        if (byId("jdbc-table-path")) {
            byId("jdbc-table-path").value = selectedNode.config.tablePath || "";
        }
        if (byId("jdbc-read-mode")) {
            byId("jdbc-read-mode").value = selectedNode.config.readMode || "FULL";
            refreshStudioSelectValue("jdbc-read-mode");
        }
        if (byId("jdbc-cursor-field")) {
            byId("jdbc-cursor-field").value = selectedNode.config.cursorField || "";
        }
        if (byId("jdbc-cursor-type")) {
            byId("jdbc-cursor-type").value = selectedNode.config.cursorType || "STRING";
            refreshStudioSelectValue("jdbc-cursor-type");
        }
        if (byId("jdbc-initial-cursor-value")) {
            byId("jdbc-initial-cursor-value").value = selectedNode.config.initialCursorValue || "";
        }
        if (byId("jdbc-poll-interval-millis")) {
            byId("jdbc-poll-interval-millis").value = selectedNode.config.pollIntervalMillis || 5000;
        }
        if (byId("jdbc-fetch-size")) {
            byId("jdbc-fetch-size").value = selectedNode.config.fetchSize || 1000;
        }
        if (byId("jdbc-max-polls")) {
            byId("jdbc-max-polls").value = selectedNode.config.maxPolls || 0;
        }
        if (byId("jdbc-id-field")) {
            byId("jdbc-id-field").value = selectedNode.config.idField || "";
        }
        if (byId("jdbc-timestamp-field")) {
            byId("jdbc-timestamp-field").value = selectedNode.config.timestampField || "";
        }
        if (byId("elasticsearch-hosts")) {
            byId("elasticsearch-hosts").value = (selectedNode.config.hosts || []).join(",");
        }
        if (byId("elasticsearch-index")) {
            byId("elasticsearch-index").value = selectedNode.config.index || "";
        }
        if (byId("elasticsearch-source-fields")) {
            byId("elasticsearch-source-fields").value = (selectedNode.config.source || []).join(",");
        }
        if (byId("elasticsearch-query")) {
            byId("elasticsearch-query").value = JSON.stringify(selectedNode.config.query || {}, null, 2);
        }
        if (byId("elasticsearch-read-mode")) {
            byId("elasticsearch-read-mode").value = selectedNode.config.readMode || "FULL";
            refreshStudioSelectValue("elasticsearch-read-mode");
        }
        if (byId("elasticsearch-cursor-field")) {
            byId("elasticsearch-cursor-field").value = selectedNode.config.cursorField || "";
        }
        if (byId("elasticsearch-cursor-type")) {
            byId("elasticsearch-cursor-type").value = selectedNode.config.cursorType || "STRING";
            refreshStudioSelectValue("elasticsearch-cursor-type");
        }
        if (byId("elasticsearch-initial-cursor-value")) {
            byId("elasticsearch-initial-cursor-value").value = selectedNode.config.initialCursorValue || "";
        }
        if (byId("elasticsearch-poll-interval-millis")) {
            byId("elasticsearch-poll-interval-millis").value = selectedNode.config.pollIntervalMillis || 5000;
        }
        if (byId("elasticsearch-scroll-size")) {
            byId("elasticsearch-scroll-size").value = selectedNode.config.scrollSize || 100;
        }
        if (byId("elasticsearch-scroll-time")) {
            byId("elasticsearch-scroll-time").value = selectedNode.config.scrollTime || "1m";
        }
        if (byId("elasticsearch-max-polls")) {
            byId("elasticsearch-max-polls").value = selectedNode.config.maxPolls || 0;
        }
        if (byId("elasticsearch-id-field")) {
            byId("elasticsearch-id-field").value = selectedNode.config.idField || "";
        }
        if (byId("elasticsearch-timestamp-field")) {
            byId("elasticsearch-timestamp-field").value = selectedNode.config.timestampField || "";
        }
        if (byId("elasticsearch-auth-type")) {
            byId("elasticsearch-auth-type").value = selectedNode.config.authType || "NONE";
            refreshStudioSelectValue("elasticsearch-auth-type");
        }
        if (byId("elasticsearch-username")) {
            byId("elasticsearch-username").value = selectedNode.config.username || "";
        }
        if (byId("elasticsearch-password")) {
            byId("elasticsearch-password").value = selectedNode.config.password || "";
        }
        if (byId("elasticsearch-api-key")) {
            byId("elasticsearch-api-key").value = selectedNode.config.apiKey || "";
        }
        if (byId("influxdb-url")) {
            byId("influxdb-url").value = selectedNode.config.url || "";
        }
        if (byId("influxdb-database")) {
            byId("influxdb-database").value = selectedNode.config.database || "";
        }
        if (byId("influxdb-sql")) {
            byId("influxdb-sql").value = selectedNode.config.sql || "";
        }
        if (byId("influxdb-schema")) {
            byId("influxdb-schema").value = JSON.stringify(selectedNode.config.schema || {}, null, 2);
        }
        if (byId("influxdb-epoch")) {
            byId("influxdb-epoch").value = selectedNode.config.epoch || "ms";
            refreshStudioSelectValue("influxdb-epoch");
        }
        if (byId("influxdb-query-timeout-seconds")) {
            byId("influxdb-query-timeout-seconds").value = selectedNode.config.queryTimeoutSeconds || 30;
        }
        if (byId("influxdb-connect-timeout-millis")) {
            byId("influxdb-connect-timeout-millis").value = selectedNode.config.connectTimeoutMillis || 10000;
        }
        if (byId("influxdb-read-mode")) {
            byId("influxdb-read-mode").value = selectedNode.config.readMode || "FULL";
            refreshStudioSelectValue("influxdb-read-mode");
        }
        if (byId("influxdb-cursor-field")) {
            byId("influxdb-cursor-field").value = selectedNode.config.cursorField || "";
        }
        if (byId("influxdb-cursor-type")) {
            byId("influxdb-cursor-type").value = selectedNode.config.cursorType || "STRING";
            refreshStudioSelectValue("influxdb-cursor-type");
        }
        if (byId("influxdb-initial-cursor-value")) {
            byId("influxdb-initial-cursor-value").value = selectedNode.config.initialCursorValue || "";
        }
        if (byId("influxdb-poll-interval-millis")) {
            byId("influxdb-poll-interval-millis").value = selectedNode.config.pollIntervalMillis || 5000;
        }
        if (byId("influxdb-max-polls")) {
            byId("influxdb-max-polls").value = selectedNode.config.maxPolls || 0;
        }
        if (byId("influxdb-id-field")) {
            byId("influxdb-id-field").value = selectedNode.config.idField || "";
        }
        if (byId("influxdb-timestamp-field")) {
            byId("influxdb-timestamp-field").value = selectedNode.config.timestampField || "";
        }
        if (byId("influxdb-username")) {
            byId("influxdb-username").value = selectedNode.config.username || "";
        }
        if (byId("influxdb-password")) {
            byId("influxdb-password").value = selectedNode.config.password || "";
        }
        if (byId("hdfs-file-default-fs")) {
            byId("hdfs-file-default-fs").value = selectedNode.config["fs.defaultFS"] || "";
        }
        if (byId("hdfs-file-path")) {
            byId("hdfs-file-path").value = selectedNode.config.path || "";
        }
        if (byId("hdfs-file-format-type")) {
            byId("hdfs-file-format-type").value = selectedNode.config.file_format_type || "JSON";
            refreshStudioSelectValue("hdfs-file-format-type");
        }
        if (byId("hdfs-file-schema")) {
            byId("hdfs-file-schema").value = JSON.stringify(selectedNode.config.schema || {}, null, 2);
        }
        if (byId("hdfs-file-read-columns")) {
            byId("hdfs-file-read-columns").value = (selectedNode.config.read_columns || []).join(",");
        }
        if (byId("hdfs-file-read-mode")) {
            byId("hdfs-file-read-mode").value = selectedNode.config.readMode || "FULL";
            refreshStudioSelectValue("hdfs-file-read-mode");
        }
        if (byId("hdfs-file-poll-interval-millis")) {
            byId("hdfs-file-poll-interval-millis").value = selectedNode.config.pollIntervalMillis || 5000;
        }
        if (byId("hdfs-file-max-polls")) {
            byId("hdfs-file-max-polls").value = selectedNode.config.maxPolls || 0;
        }
        if (byId("hdfs-file-filter-pattern")) {
            byId("hdfs-file-filter-pattern").value = selectedNode.config.file_filter_pattern || "";
        }
        if (byId("hdfs-file-parse-partition-from-path")) {
            byId("hdfs-file-parse-partition-from-path").checked = Boolean(selectedNode.config.parse_partition_from_path);
        }
        if (byId("hdfs-file-field-delimiter")) {
            byId("hdfs-file-field-delimiter").value = selectedNode.config.field_delimiter || "\\001";
        }
        if (byId("hdfs-file-row-delimiter")) {
            byId("hdfs-file-row-delimiter").value = selectedNode.config.row_delimiter || "\\n";
        }
        if (byId("hdfs-file-skip-header-row-number")) {
            byId("hdfs-file-skip-header-row-number").value = selectedNode.config.skip_header_row_number ?? 0;
        }
        if (byId("hdfs-file-csv-use-header-line")) {
            byId("hdfs-file-csv-use-header-line").checked = Boolean(selectedNode.config.csv_use_header_line);
        }
        if (byId("hdfs-file-encoding")) {
            byId("hdfs-file-encoding").value = selectedNode.config.encoding || "UTF-8";
        }
        if (byId("hdfs-file-compress-codec")) {
            byId("hdfs-file-compress-codec").value = selectedNode.config.compress_codec || "none";
        }
        if (byId("hdfs-file-id-field")) {
            byId("hdfs-file-id-field").value = selectedNode.config.idField || "";
        }
        if (byId("hdfs-file-timestamp-field")) {
            byId("hdfs-file-timestamp-field").value = selectedNode.config.timestampField || "";
        }
        if (byId("hdfs-file-hdfs-site-path")) {
            byId("hdfs-file-hdfs-site-path").value = selectedNode.config.hdfs_site_path || "";
        }
        if (byId("hdfs-file-kerberos-principal")) {
            byId("hdfs-file-kerberos-principal").value = selectedNode.config.kerberos_principal || "";
        }
        if (byId("hdfs-file-kerberos-keytab-path")) {
            byId("hdfs-file-kerberos-keytab-path").value = selectedNode.config.kerberos_keytab_path || "";
        }
        updateJdbcReadModeUI(selectedNode.config.readMode || "FULL");
        updateElasticsearchReadModeUI(selectedNode.config.readMode || "FULL");
        updateInfluxDbReadModeUI(selectedNode.config.readMode || "FULL");
        updateHdfsFileReadModeUI(selectedNode.config.readMode || "FULL");
        updateElasticsearchAuthUI(selectedNode.config.authType || "NONE");
        updateSourceAuthUI(selectedNode.config.authType || "NONE");
    } else if (selectedNode.type === "TRANSFORMER") {
        document.querySelectorAll(".transform-specific-config").forEach(element => {
            element.classList.add("hidden");
        });

        switch (selectedNode.operator) {
            case "PUT":
                byId("put-config")?.classList.remove("hidden");
                if (byId("put-field")) {
                    byId("put-field").value = selectedNode.config.field || "";
                }
                const putValueMode = selectedNode.config.valueMode || inferPutValueMode(selectedNode.config.value || "");
                if (byId("put-value-mode")) {
                    byId("put-value-mode").value = putValueMode;
                    refreshStudioSelectValue("put-value-mode");
                }
                if (byId("put-literal-value")) {
                    byId("put-literal-value").value = putValueMode === "LITERAL" ? selectedNode.config.value || "" : "";
                }
                if (byId("put-reference-field")) {
                    byId("put-reference-field").value = selectedNode.config.referenceField || putReferenceField(selectedNode.config.value || "");
                }
                if (byId("put-template-value")) {
                    byId("put-template-value").value = selectedNode.config.template || selectedNode.config.value || "";
                }
                updatePutValueModeUI(putValueMode);
                break;
            case "PRUNE":
                byId("prune-config")?.classList.remove("hidden");
                renderPruneFieldInputs(selectedNode.config.fields || []);
                break;
            case "RENAME":
                byId("rename-config")?.classList.remove("hidden");
                renderRenameMappingInputs(selectedNode.config.mapping || {});
                break;
            case "DESERIALIZE":
                byId("deserialize-config")?.classList.remove("hidden");
                if (byId("deserialize-field")) {
                    byId("deserialize-field").value = selectedNode.config.field || "";
                }
                if (byId("deserialize-target-field")) {
                    byId("deserialize-target-field").value = selectedNode.config.targetField || "";
                }
                if (byId("deserialize-format")) {
                    byId("deserialize-format").value = selectedNode.config.format || "JSON";
                    refreshStudioSelectValue("deserialize-format");
                }
                if (byId("deserialize-field-names")) {
                    byId("deserialize-field-names").value = (selectedNode.config.fieldNames || []).join(",");
                }
                if (byId("deserialize-delimiter")) {
                    byId("deserialize-delimiter").value = selectedNode.config.delimiter || ",";
                }
                updateDeserializeFormatUI(selectedNode.config.format || "JSON");
                break;
            case "SERIALIZE":
                byId("serialize-config")?.classList.remove("hidden");
                if (byId("serialize-source-fields")) {
                    byId("serialize-source-fields").value = (selectedNode.config.sourceFields || []).join(",");
                }
                if (byId("serialize-target-field")) {
                    byId("serialize-target-field").value = selectedNode.config.targetField || "";
                }
                if (byId("serialize-format")) {
                    byId("serialize-format").value = selectedNode.config.format || "JSON";
                    refreshStudioSelectValue("serialize-format");
                }
                if (byId("serialize-delimiter")) {
                    byId("serialize-delimiter").value = selectedNode.config.delimiter || ",";
                }
                updateSerializeFormatUI(selectedNode.config.format || "JSON");
                break;
            case "FILTER":
                byId("filter-config")?.classList.remove("hidden");
                if (byId("filter-condition")) {
                    byId("filter-condition").value = selectedNode.config.condition || "";
                }
                break;
            case "GROK":
                byId("grok-config")?.classList.remove("hidden");
                if (byId("grok-input-field")) {
                    byId("grok-input-field").value = selectedNode.config.inputField || "";
                }
                if (byId("grok-output-field")) {
                    byId("grok-output-field").value = selectedNode.config.outputField || "";
                }
                if (byId("grok-pattern")) {
                    byId("grok-pattern").value = selectedNode.config.pattern || "";
                }
                break;
            case "CAST":
                byId("cast-config")?.classList.remove("hidden");
                if (byId("cast-input-field")) {
                    byId("cast-input-field").value = selectedNode.config.inputField || "";
                }
                const castOutputMode = selectedNode.config.outputMode || inferCastOutputMode(
                        selectedNode.config.inputField || "",
                        selectedNode.config.outputField || "");
                if (byId("cast-output-mode")) {
                    byId("cast-output-mode").value = castOutputMode;
                    refreshStudioSelectValue("cast-output-mode");
                }
                if (byId("cast-output-field")) {
                    byId("cast-output-field").value = castOutputMode === "NEW_FIELD" ? selectedNode.config.outputField || "" : "";
                }
                if (byId("cast-target-type")) {
                    byId("cast-target-type").value = selectedNode.config.targetType || "STRING";
                    refreshStudioSelectValue("cast-target-type");
                }
                updateCastOutputModeUI(castOutputMode);
                break;
            case "EVAL":
                byId("eval-config")?.classList.remove("hidden");
                if (byId("eval-target-field")) {
                    byId("eval-target-field").value = selectedNode.config.targetField || "";
                }
                if (byId("eval-expression")) {
                    byId("eval-expression").value = selectedNode.config.expression || "";
                }
                if (byId("eval-output-mode")) {
                    byId("eval-output-mode").value = selectedNode.config.outputMode || "OVERWRITE";
                    refreshStudioSelectValue("eval-output-mode");
                }
                if (byId("eval-error-strategy")) {
                    byId("eval-error-strategy").value = selectedNode.config.errorStrategy || "KEEP_ORIGINAL";
                    refreshStudioSelectValue("eval-error-strategy");
                }
                break;
            case "DEDUPLICATE":
                byId("deduplicate-config")?.classList.remove("hidden");
                if (byId("deduplicate-key-fields")) {
                    byId("deduplicate-key-fields").value = (selectedNode.config.keyFields || []).join(",");
                }
                if (byId("deduplicate-time-mode")) {
                    byId("deduplicate-time-mode").value = selectedNode.config.timeMode || "PROCESSING_TIME";
                    refreshStudioSelectValue("deduplicate-time-mode");
                }
                if (byId("deduplicate-ttl-seconds")) {
                    byId("deduplicate-ttl-seconds").value = selectedNode.config.ttlSeconds || 3600;
                }
                if (byId("deduplicate-event-time-field")) {
                    byId("deduplicate-event-time-field").value = selectedNode.config.eventTimeField || "";
                }
                if (byId("deduplicate-window-seconds")) {
                    byId("deduplicate-window-seconds").value = selectedNode.config.windowSeconds || 300;
                }
                if (byId("deduplicate-watermark-delay-seconds")) {
                    byId("deduplicate-watermark-delay-seconds").value = selectedNode.config.watermarkDelaySeconds ?? 30;
                }
                if (byId("deduplicate-keep-strategy")) {
                    byId("deduplicate-keep-strategy").value = selectedNode.config.keepStrategy || "FIRST";
                    refreshStudioSelectValue("deduplicate-keep-strategy");
                }
                if (byId("deduplicate-late-data-strategy")) {
                    byId("deduplicate-late-data-strategy").value = selectedNode.config.lateDataStrategy || "DISCARD";
                    refreshStudioSelectValue("deduplicate-late-data-strategy");
                }
                if (byId("deduplicate-duplicate-strategy")) {
                    byId("deduplicate-duplicate-strategy").value = selectedNode.config.duplicateStrategy || "DISCARD";
                    refreshStudioSelectValue("deduplicate-duplicate-strategy");
                }
                updateDeduplicateTimeModeUI(selectedNode.config.timeMode || "PROCESSING_TIME");
                break;
            case "LOOKUP_ENRICH":
                byId("lookup-enrich-config")?.classList.remove("hidden");
                if (byId("lookup-enrich-source-field")) {
                    byId("lookup-enrich-source-field").value = selectedNode.config.sourceField || "";
                }
                if (byId("lookup-enrich-target-field")) {
                    byId("lookup-enrich-target-field").value = selectedNode.config.targetField || "";
                }
                if (byId("lookup-enrich-missing-strategy")) {
                    byId("lookup-enrich-missing-strategy").value = selectedNode.config.missingStrategy || "KEEP_ORIGINAL";
                    refreshStudioSelectValue("lookup-enrich-missing-strategy");
                }
                if (byId("lookup-enrich-overwrite-target-field")) {
                    byId("lookup-enrich-overwrite-target-field").checked = Boolean(selectedNode.config.overwriteTargetField);
                }
                renderLookupEnrichEntries(selectedNode.config.entries || []);
                break;
            case "LOOKUP_JOIN":
                byId("lookup-join-config")?.classList.remove("hidden");
                if (byId("lookup-join-source-field")) {
                    byId("lookup-join-source-field").value = selectedNode.config.sourceField || "";
                }
                if (byId("lookup-join-target-field")) {
                    byId("lookup-join-target-field").value = selectedNode.config.targetField || "";
                }
                if (byId("lookup-join-type")) {
                    byId("lookup-join-type").value = selectedNode.config.joinType || "LEFT";
                    refreshStudioSelectValue("lookup-join-type");
                }
                if (byId("lookup-join-missing-strategy")) {
                    byId("lookup-join-missing-strategy").value = selectedNode.config.missingStrategy || "KEEP_ORIGINAL";
                    refreshStudioSelectValue("lookup-join-missing-strategy");
                }
                if (byId("lookup-join-overwrite-target-field")) {
                    byId("lookup-join-overwrite-target-field").checked = Boolean(selectedNode.config.overwriteTargetField);
                }
                renderLookupJoinEntries(selectedNode.config.entries || []);
                updateLookupJoinTypeUI(selectedNode.config.joinType || "LEFT");
                break;
            case "STREAM_JOIN":
                byId("stream-join-config")?.classList.remove("hidden");
                if (byId("stream-join-left-key-field")) {
                    byId("stream-join-left-key-field").value = selectedNode.config.leftKeyField || "";
                }
                if (byId("stream-join-right-key-field")) {
                    byId("stream-join-right-key-field").value = selectedNode.config.rightKeyField || "";
                }
                if (byId("stream-join-target-field")) {
                    byId("stream-join-target-field").value = selectedNode.config.targetField || "";
                }
                if (byId("stream-join-type")) {
                    byId("stream-join-type").value = selectedNode.config.joinType || "LEFT";
                    refreshStudioSelectValue("stream-join-type");
                }
                if (byId("stream-join-missing-strategy")) {
                    byId("stream-join-missing-strategy").value = selectedNode.config.missingStrategy || "KEEP_ORIGINAL";
                    refreshStudioSelectValue("stream-join-missing-strategy");
                }
                if (byId("stream-join-overwrite-target-field")) {
                    byId("stream-join-overwrite-target-field").checked = Boolean(selectedNode.config.overwriteTargetField);
                }
                if (byId("stream-join-time-mode")) {
                    byId("stream-join-time-mode").value = selectedNode.config.timeMode || "PROCESSING_TIME";
                    refreshStudioSelectValue("stream-join-time-mode");
                }
                if (byId("stream-join-time-unit")) {
                    byId("stream-join-time-unit").value = selectedNode.config.timeUnit || "SECONDS";
                    refreshStudioSelectValue("stream-join-time-unit");
                }
                if (byId("stream-join-window-before")) {
                    byId("stream-join-window-before").value = selectedNode.config.windowBefore ?? 60;
                }
                if (byId("stream-join-window-after")) {
                    byId("stream-join-window-after").value = selectedNode.config.windowAfter ?? 60;
                }
                if (byId("stream-join-watermark-delay")) {
                    byId("stream-join-watermark-delay").value = selectedNode.config.watermarkDelay ?? 30;
                }
                if (byId("stream-join-late-data-strategy")) {
                    byId("stream-join-late-data-strategy").value = selectedNode.config.lateDataStrategy || "DROP";
                }
                updateStreamJoinConfigUI(selectedNode.config.joinType || "LEFT", selectedNode.config.timeMode || "PROCESSING_TIME");
                break;
            case "FLATTEN":
                byId("flatten-config")?.classList.remove("hidden");
                if (byId("flatten-source-field")) {
                    byId("flatten-source-field").value = selectedNode.config.sourceField || "";
                }
                if (byId("flatten-target-prefix")) {
                    byId("flatten-target-prefix").value = selectedNode.config.targetPrefix || "";
                }
                if (byId("flatten-delimiter")) {
                    byId("flatten-delimiter").value = selectedNode.config.delimiter || "_";
                }
                if (byId("flatten-remove-source-field")) {
                    byId("flatten-remove-source-field").checked = Boolean(selectedNode.config.removeSourceField);
                }
                break;
            case "EXPLODE":
                byId("explode-config")?.classList.remove("hidden");
                if (byId("explode-source-field")) {
                    byId("explode-source-field").value = selectedNode.config.sourceField || "";
                }
                if (byId("explode-target-field")) {
                    byId("explode-target-field").value = selectedNode.config.targetField || "";
                }
                if (byId("explode-keep-empty")) {
                    byId("explode-keep-empty").checked = Boolean(selectedNode.config.keepEmpty);
                }
                break;
            case "DATA_QUALITY":
                byId("data-quality-config")?.classList.remove("hidden");
                if (byId("data-quality-mode")) {
                    byId("data-quality-mode").value = selectedNode.config.mode || "DIRTY_PORT";
                    refreshStudioSelectValue("data-quality-mode");
                }
                updateDataQualityModeUI(selectedNode.config.mode || "DIRTY_PORT");
                if (byId("data-quality-error-field")) {
                    byId("data-quality-error-field").value = selectedNode.config.errorField || "_streamcraft_quality_errors";
                }
                renderDataQualityRules(selectedNode.config.rules || []);
                break;
            case "TIME_DERIVE":
                byId("time-derive-config")?.classList.remove("hidden");
                if (byId("time-derive-source-field")) {
                    byId("time-derive-source-field").value = selectedNode.config.sourceField || "";
                }
                if (byId("time-derive-source-format")) {
                    byId("time-derive-source-format").value = selectedNode.config.sourceFormat || "AUTO";
                    refreshStudioSelectValue("time-derive-source-format");
                }
                if (byId("time-derive-source-pattern")) {
                    byId("time-derive-source-pattern").value = selectedNode.config.sourcePattern || "";
                }
                if (byId("time-derive-source-time-zone")) {
                    byId("time-derive-source-time-zone").value = selectedNode.config.sourceTimeZone || "UTC";
                }
                if (byId("time-derive-output-time-zone")) {
                    byId("time-derive-output-time-zone").value = selectedNode.config.outputTimeZone || "UTC";
                }
                if (byId("time-derive-parse-error-strategy")) {
                    byId("time-derive-parse-error-strategy").value = selectedNode.config.parseErrorStrategy || "KEEP_ORIGINAL";
                    refreshStudioSelectValue("time-derive-parse-error-strategy");
                }
                renderTimeDeriveItems(selectedNode.config.derivations || []);
                updateTimeDeriveSourceFormatUI(selectedNode.config.sourceFormat || "AUTO");
                break;
            case "MASK_HASH":
                byId("mask-hash-config")?.classList.remove("hidden");
                renderMaskHashRules(selectedNode.config.rules || []);
                break;
            case "CASE_WHEN":
                byId("case-when-config")?.classList.remove("hidden");
                if (byId("case-when-target-field")) {
                    byId("case-when-target-field").value = selectedNode.config.targetField || "";
                }
                renderCaseWhenItems(selectedNode.config.cases || []);
                if (byId("case-when-default-mode")) {
                    byId("case-when-default-mode").value = selectedNode.config.defaultMode || "NONE";
                    refreshStudioSelectValue("case-when-default-mode");
                }
                if (byId("case-when-default-value")) {
                    byId("case-when-default-value").value = selectedNode.config.defaultValue || "";
                }
                if (byId("case-when-default-expression")) {
                    byId("case-when-default-expression").value = selectedNode.config.defaultExpression || "";
                }
                updateCaseWhenDefaultModeUI(selectedNode.config.defaultMode || "NONE");
                break;
            case "ROUTE":
                byId("route-config")?.classList.remove("hidden");
                if (byId("route-match-mode")) {
                    byId("route-match-mode").value = selectedNode.config.matchMode || "FIRST_MATCH";
                    refreshStudioSelectValue("route-match-mode");
                }
                if (byId("route-include-unmatched")) {
                    byId("route-include-unmatched").checked = selectedNode.config.includeUnmatched !== false;
                }
                if (byId("route-unmatched-port")) {
                    byId("route-unmatched-port").value = selectedNode.config.unmatchedPort || "unmatched";
                }
                renderRouteItems(selectedNode.config.routes || []);
                updateRouteConfigUI(selectedNode.config.includeUnmatched !== false);
                break;
            case "AGGREGATE":
                byId("aggregate-config")?.classList.remove("hidden");
                if (byId("aggregate-mode")) {
                    byId("aggregate-mode").value = selectedNode.config.mode || "GLOBAL";
                    refreshStudioSelectValue("aggregate-mode");
                }
                if (byId("aggregate-group-by")) {
                    byId("aggregate-group-by").value = (selectedNode.config.groupBy || []).join(",");
                }
                if (byId("aggregate-window-type")) {
                    byId("aggregate-window-type").value = selectedNode.config.windowType || "TUMBLING_TIME";
                    refreshStudioSelectValue("aggregate-window-type");
                }
                if (byId("aggregate-time-mode")) {
                    byId("aggregate-time-mode").value = selectedNode.config.timeMode || "PROCESSING_TIME";
                    refreshStudioSelectValue("aggregate-time-mode");
                }
                if (byId("aggregate-time-unit")) {
                    byId("aggregate-time-unit").value = selectedNode.config.timeUnit || "SECONDS";
                    refreshStudioSelectValue("aggregate-time-unit");
                }
                if (byId("aggregate-window-size")) {
                    byId("aggregate-window-size").value = selectedNode.config.windowSize || 60;
                }
                if (byId("aggregate-window-slide")) {
                    byId("aggregate-window-slide").value = selectedNode.config.windowSlide || 10;
                }
                if (byId("aggregate-watermark-delay")) {
                    byId("aggregate-watermark-delay").value = selectedNode.config.watermarkDelay ?? 30;
                }
                if (byId("aggregate-event-time-field")) {
                    byId("aggregate-event-time-field").value = selectedNode.config.eventTimeField || "";
                }
                if (byId("aggregate-event-time-unit")) {
                    byId("aggregate-event-time-unit").value = selectedNode.config.eventTimeUnit || "MILLISECONDS";
                }
                if (byId("aggregate-output-mode")) {
                    byId("aggregate-output-mode").value = selectedNode.config.outputMode || "NESTED";
                    refreshStudioSelectValue("aggregate-output-mode");
                }
                if (byId("aggregate-window-start-field")) {
                    byId("aggregate-window-start-field").value = selectedNode.config.windowStartField || "windowStart";
                }
                if (byId("aggregate-window-end-field")) {
                    byId("aggregate-window-end-field").value = selectedNode.config.windowEndField || "windowEnd";
                }
                if (byId("aggregate-count-window-size")) {
                    byId("aggregate-count-window-size").value = selectedNode.config.countWindowSize || 100;
                }
                renderAggregateItems(selectedNode.config.aggregations || []);
                updateAggregateConfigUI(selectedNode.config.mode || "GLOBAL", selectedNode.config.windowType || "TUMBLING_TIME", selectedNode.config.timeMode || "PROCESSING_TIME");
                break;
            case "CUSTOM_CODE":
                byId("custom-code-config")?.classList.remove("hidden");
                if (byId("custom-code-class-name")) {
                    byId("custom-code-class-name").value = selectedNode.config.className || "";
                }
                if (byId("custom-code-source")) {
                    byId("custom-code-source").value = selectedNode.config.sourceCode || "";
                }
                if (byId("custom-code-error-strategy")) {
                    byId("custom-code-error-strategy").value = selectedNode.config.errorStrategy || "KEEP_ORIGINAL";
                    refreshStudioSelectValue("custom-code-error-strategy");
                }
                break;
            default:
                break;
        }

        if (byId("transform-note")) {
            byId("transform-note").value = selectedNode.config.note || "";
        }
    } else if (selectedNode.type === "SINK") {
        updateSinkConfigSections(selectedNode.operator);
        if (byId("sink-bootstrap-servers")) {
            byId("sink-bootstrap-servers").value = selectedNode.config.bootstrapServers || "";
        }
        if (byId("sink-topic")) {
            byId("sink-topic").value = selectedNode.config.topic || "";
        }
        if (byId("sink-delivery-guarantee")) {
            byId("sink-delivery-guarantee").value = selectedNode.config.deliveryGuarantee || "AT_LEAST_ONCE";
            refreshStudioSelectValue("sink-delivery-guarantee");
        }
        if (byId("sink-auth-type")) {
            byId("sink-auth-type").value = selectedNode.config.authType || "NONE";
            refreshStudioSelectValue("sink-auth-type");
        }
        if (byId("sink-auth-username")) {
            byId("sink-auth-username").value = selectedNode.config.username || "";
        }
        if (byId("sink-auth-password")) {
            byId("sink-auth-password").value = selectedNode.config.password || "";
        }
        if (byId("sink-scram-mechanism")) {
            byId("sink-scram-mechanism").value = selectedNode.config.scramMechanism || "SCRAM-SHA-512";
            refreshStudioSelectValue("sink-scram-mechanism");
        }
        if (byId("sink-format")) {
            byId("sink-format").value = selectedNode.config.format || "JSON";
            refreshStudioSelectValue("sink-format");
        }
        if (byId("sink-message-field")) {
            byId("sink-message-field").value = selectedNode.config.messageField || "_streamcraft_message";
        }
        if (byId("jdbc-sink-url")) {
            byId("jdbc-sink-url").value = selectedNode.config.url || "";
        }
        if (byId("jdbc-sink-driver")) {
            byId("jdbc-sink-driver").value = selectedNode.config.driver || "";
        }
        if (byId("jdbc-sink-username")) {
            byId("jdbc-sink-username").value = selectedNode.config.username || "";
        }
        if (byId("jdbc-sink-password")) {
            byId("jdbc-sink-password").value = selectedNode.config.password || "";
        }
        if (byId("jdbc-sink-table-path")) {
            byId("jdbc-sink-table-path").value = selectedNode.config.tablePath || "";
        }
        if (byId("jdbc-sink-write-mode")) {
            byId("jdbc-sink-write-mode").value = selectedNode.config.writeMode || "INSERT";
            refreshStudioSelectValue("jdbc-sink-write-mode");
        }
        if (byId("jdbc-sink-fields")) {
            byId("jdbc-sink-fields").value = (selectedNode.config.fields || []).join(",");
        }
        if (byId("jdbc-sink-key-fields")) {
            byId("jdbc-sink-key-fields").value = (selectedNode.config.keyFields || []).join(",");
        }
        if (byId("jdbc-sink-batch-size")) {
            byId("jdbc-sink-batch-size").value = selectedNode.config.batchSize || 500;
        }
        if (byId("jdbc-sink-flush-interval-millis")) {
            byId("jdbc-sink-flush-interval-millis").value = selectedNode.config.flushIntervalMillis || 5000;
        }
        if (byId("elasticsearch-sink-hosts")) {
            byId("elasticsearch-sink-hosts").value = (selectedNode.config.hosts || []).join(",");
        }
        if (byId("elasticsearch-sink-index")) {
            byId("elasticsearch-sink-index").value = selectedNode.config.index || "";
        }
        if (byId("elasticsearch-sink-index-type")) {
            byId("elasticsearch-sink-index-type").value = selectedNode.config.indexType || "";
        }
        if (byId("elasticsearch-sink-primary-keys")) {
            byId("elasticsearch-sink-primary-keys").value = (selectedNode.config.primaryKeys || []).join(",");
        }
        if (byId("elasticsearch-sink-key-delimiter")) {
            byId("elasticsearch-sink-key-delimiter").value = selectedNode.config.keyDelimiter || "_";
        }
        if (byId("elasticsearch-sink-fields")) {
            byId("elasticsearch-sink-fields").value = (selectedNode.config.fields || []).join(",");
        }
        if (byId("elasticsearch-sink-max-batch-size")) {
            byId("elasticsearch-sink-max-batch-size").value = selectedNode.config.maxBatchSize || 10;
        }
        if (byId("elasticsearch-sink-flush-interval-millis")) {
            byId("elasticsearch-sink-flush-interval-millis").value = selectedNode.config.flushIntervalMillis || 5000;
        }
        if (byId("elasticsearch-sink-max-retry-count")) {
            byId("elasticsearch-sink-max-retry-count").value = selectedNode.config.maxRetryCount ?? 3;
        }
        if (byId("elasticsearch-sink-auth-type")) {
            byId("elasticsearch-sink-auth-type").value = selectedNode.config.authType || "NONE";
            refreshStudioSelectValue("elasticsearch-sink-auth-type");
        }
        if (byId("elasticsearch-sink-username")) {
            byId("elasticsearch-sink-username").value = selectedNode.config.username || "";
        }
        if (byId("elasticsearch-sink-password")) {
            byId("elasticsearch-sink-password").value = selectedNode.config.password || "";
        }
        if (byId("elasticsearch-sink-api-key-id")) {
            byId("elasticsearch-sink-api-key-id").value = selectedNode.config.apiKeyId || "";
        }
        if (byId("elasticsearch-sink-api-key")) {
            byId("elasticsearch-sink-api-key").value = selectedNode.config.apiKey || "";
        }
        if (byId("elasticsearch-sink-api-key-encoded")) {
            byId("elasticsearch-sink-api-key-encoded").value = selectedNode.config.apiKeyEncoded || "";
        }
        if (byId("influxdb-sink-url")) {
            byId("influxdb-sink-url").value = selectedNode.config.url || "";
        }
        if (byId("influxdb-sink-database")) {
            byId("influxdb-sink-database").value = selectedNode.config.database || "";
        }
        if (byId("influxdb-sink-measurement")) {
            byId("influxdb-sink-measurement").value = selectedNode.config.measurement || "";
        }
        if (byId("influxdb-sink-key-time")) {
            byId("influxdb-sink-key-time").value = selectedNode.config.keyTime || "time";
        }
        if (byId("influxdb-sink-key-tags")) {
            byId("influxdb-sink-key-tags").value = (selectedNode.config.keyTags || []).join(",");
        }
        if (byId("influxdb-sink-fields")) {
            byId("influxdb-sink-fields").value = (selectedNode.config.fields || []).join(",");
        }
        if (byId("influxdb-sink-batch-size")) {
            byId("influxdb-sink-batch-size").value = selectedNode.config.batchSize || 100;
        }
        if (byId("influxdb-sink-max-retries")) {
            byId("influxdb-sink-max-retries").value = selectedNode.config.maxRetries ?? 3;
        }
        if (byId("influxdb-sink-retry-backoff-multiplier-millis")) {
            byId("influxdb-sink-retry-backoff-multiplier-millis").value = selectedNode.config.retryBackoffMultiplierMillis || 100;
        }
        if (byId("influxdb-sink-max-retry-backoff-millis")) {
            byId("influxdb-sink-max-retry-backoff-millis").value = selectedNode.config.maxRetryBackoffMillis || 1000;
        }
        if (byId("influxdb-sink-connect-timeout-millis")) {
            byId("influxdb-sink-connect-timeout-millis").value = selectedNode.config.connectTimeoutMillis || 10000;
        }
        if (byId("influxdb-sink-flush-interval-millis")) {
            byId("influxdb-sink-flush-interval-millis").value = selectedNode.config.flushIntervalMillis || 5000;
        }
        if (byId("influxdb-sink-precision")) {
            byId("influxdb-sink-precision").value = selectedNode.config.precision || "ms";
            refreshStudioSelectValue("influxdb-sink-precision");
        }
        if (byId("influxdb-sink-username")) {
            byId("influxdb-sink-username").value = selectedNode.config.username || "";
        }
        if (byId("influxdb-sink-password")) {
            byId("influxdb-sink-password").value = selectedNode.config.password || "";
        }
        if (byId("hdfs-file-sink-default-fs")) {
            byId("hdfs-file-sink-default-fs").value = selectedNode.config["fs.defaultFS"] || "";
        }
        if (byId("hdfs-file-sink-path")) {
            byId("hdfs-file-sink-path").value = selectedNode.config.path || "";
        }
        if (byId("hdfs-file-sink-tmp-path")) {
            byId("hdfs-file-sink-tmp-path").value = selectedNode.config.tmp_path || "/tmp/streamcraft/hdfs-file";
        }
        if (byId("hdfs-file-sink-format-type")) {
            byId("hdfs-file-sink-format-type").value = selectedNode.config.file_format_type || "JSON";
            refreshStudioSelectValue("hdfs-file-sink-format-type");
        }
        if (byId("hdfs-file-sink-columns")) {
            byId("hdfs-file-sink-columns").value = (selectedNode.config.sink_columns || []).join(",");
        }
        if (byId("hdfs-file-sink-partition-by")) {
            byId("hdfs-file-sink-partition-by").value = (selectedNode.config.partition_by || []).join(",");
        }
        if (byId("hdfs-file-sink-partition-dir-expression")) {
            byId("hdfs-file-sink-partition-dir-expression").value = selectedNode.config.partition_dir_expression || "";
        }
        if (byId("hdfs-file-sink-partition-field-write-in-file")) {
            byId("hdfs-file-sink-partition-field-write-in-file").checked = selectedNode.config.is_partition_field_write_in_file ?? true;
        }
        if (byId("hdfs-file-sink-custom-filename")) {
            byId("hdfs-file-sink-custom-filename").checked = Boolean(selectedNode.config.custom_filename);
        }
        if (byId("hdfs-file-sink-file-name-expression")) {
            byId("hdfs-file-sink-file-name-expression").value = selectedNode.config.file_name_expression || "part-${now}";
        }
        if (byId("hdfs-file-sink-filename-time-format")) {
            byId("hdfs-file-sink-filename-time-format").value = selectedNode.config.filename_time_format || "yyyyMMddHHmmss";
        }
        if (byId("hdfs-file-sink-batch-size")) {
            byId("hdfs-file-sink-batch-size").value = selectedNode.config.batch_size || 1000;
        }
        if (byId("hdfs-file-sink-flush-interval-millis")) {
            byId("hdfs-file-sink-flush-interval-millis").value = selectedNode.config.flushIntervalMillis || 5000;
        }
        if (byId("hdfs-file-sink-field-delimiter")) {
            byId("hdfs-file-sink-field-delimiter").value = selectedNode.config.field_delimiter || "\\001";
        }
        if (byId("hdfs-file-sink-row-delimiter")) {
            byId("hdfs-file-sink-row-delimiter").value = selectedNode.config.row_delimiter || "\\n";
        }
        if (byId("hdfs-file-sink-csv-use-header-line")) {
            byId("hdfs-file-sink-csv-use-header-line").checked = Boolean(selectedNode.config.csv_use_header_line);
        }
        if (byId("hdfs-file-sink-encoding")) {
            byId("hdfs-file-sink-encoding").value = selectedNode.config.encoding || "UTF-8";
        }
        if (byId("hdfs-file-sink-compress-codec")) {
            byId("hdfs-file-sink-compress-codec").value = selectedNode.config.compress_codec || "none";
        }
        if (byId("hdfs-file-sink-hdfs-site-path")) {
            byId("hdfs-file-sink-hdfs-site-path").value = selectedNode.config.hdfs_site_path || "";
        }
        if (byId("hdfs-file-sink-kerberos-principal")) {
            byId("hdfs-file-sink-kerberos-principal").value = selectedNode.config.kerberos_principal || "";
        }
        if (byId("hdfs-file-sink-kerberos-keytab-path")) {
            byId("hdfs-file-sink-kerberos-keytab-path").value = selectedNode.config.kerberos_keytab_path || "";
        }
        updateSinkAuthUI(selectedNode.config.authType || "NONE");
        updateSinkFormatUI(selectedNode.config.format || "JSON");
        updateJdbcSinkWriteModeUI(selectedNode.config.writeMode || "INSERT");
        updateElasticsearchSinkAuthUI(selectedNode.config.authType || "NONE");
        updateHdfsFileSinkFilenameUI(Boolean(selectedNode.config.custom_filename));
    }
}

function renderHeader() {
    const title = byId("studio-title");
    const status = byId("studio-status");
    const saveButton = byId("save-pipeline-button");
    const runButton = byId("run-pipeline-button");
    const stopButton = byId("stop-pipeline-button");
    const runStatus = String(state.lastRunStatus || "").toUpperCase();

    if (title) {
        title.textContent = state.pipelineMeta.name || (state.currentPipelineId ? t("studio.title.edit", "Edit pipeline") : t("studio.title.new", "New pipeline"));
    }

    let statusKey = state.currentPipelineId ? "SAVED" : "NEW";
    if (runStatus === "RUNNING") {
        statusKey = "RUNNING";
    } else if (runStatus === "FAILED") {
        statusKey = "FAILED";
    } else if (runStatus === "STOPPED") {
        statusKey = "STOPPED";
    }
    if (state.hasUnsavedChanges) {
        statusKey = "DIRTY";
    }

    const statusMeta = HEADER_STATUS_META[statusKey] || HEADER_STATUS_META.UNKNOWN;
    if (status) {
        status.textContent = statusMeta.label;
        status.className = statusMeta.className;
    }

    const isBusy = state.requestState.saving || state.requestState.running || state.requestState.stopping;
    const isRunning = runStatus === "RUNNING";
    if (saveButton) {
        saveButton.disabled = isBusy;
        saveButton.classList.toggle("opacity-60", isBusy);
        saveButton.classList.toggle("cursor-not-allowed", isBusy);
    }
    if (runButton) {
        runButton.disabled = isBusy;
        runButton.classList.toggle("hidden", isRunning);
        runButton.classList.toggle("opacity-60", isBusy);
        runButton.classList.toggle("cursor-not-allowed", isBusy);
    }
    if (stopButton) {
        stopButton.disabled = isBusy;
        stopButton.classList.toggle("hidden", !isRunning);
        stopButton.classList.toggle("opacity-60", isBusy);
        stopButton.classList.toggle("cursor-not-allowed", isBusy);
    }
}

function nodeTone(type) {
    if (type === "SOURCE") {
        return {
            badge: "S",
            badgeClass: "bg-blue-100 text-blue-700 dark:bg-blue-500/20 dark:text-blue-300",
            tagClass: "text-blue-700 dark:text-blue-300"
        };
    }
    if (type === "SINK") {
        return {
            badge: "K",
            badgeClass: "bg-emerald-100 text-emerald-700 dark:bg-emerald-500/20 dark:text-emerald-300",
            tagClass: "text-emerald-700 dark:text-emerald-300"
        };
    }
    return {
        badge: "T",
        badgeClass: "bg-amber-100 text-amber-700 dark:bg-amber-500/20 dark:text-amber-300",
        tagClass: "text-amber-700 dark:text-amber-300"
    };
}

function outputPortsForNode(node) {
    if (node.type === "SINK") {
        return [];
    }
    if (node.operator === "FILTER") {
        return FILTER_OUTPUT_PORT_IDS.map(portId => ({ id: portId }));
    }
    if (node.operator === "DATA_QUALITY") {
        return DATA_QUALITY_OUTPUT_PORT_IDS.map(portId => ({ id: portId }));
    }
    if (node.operator === "ROUTE") {
        const routePorts = Array.isArray(node.config?.routes)
            ? node.config.routes.map(route => String(route?.portId || "").trim()).filter(Boolean)
            : [];
        if (node.config?.includeUnmatched !== false) {
            routePorts.push(String(node.config?.unmatchedPort || "unmatched").trim() || "unmatched");
        }
        return [...new Set(routePorts)].map(portId => ({ id: portId }));
    }
    return [{ id: DEFAULT_SOURCE_PORT_ID, label: "" }];
}

function inputPortsForNode(node) {
    if (node.type === "SOURCE") {
        return [];
    }
    if (node.operator === "STREAM_JOIN") {
        return [
            { id: "left", label: "" },
            { id: "right", label: "" }
        ];
    }
    return [{ id: DEFAULT_TARGET_PORT_ID, label: "" }];
}

function sourcePortIdForEdge(edge, sourceNode) {
    if (sourceNode?.operator === "FILTER") {
        return edge.sourcePortId;
    }
    if (sourceNode?.operator === "ROUTE") {
        return edge.sourcePortId;
    }
    return edge.sourcePortId;
}

function outputPortIdForAnchor(node, portId) {
    if (node?.operator === "FILTER") {
        return portId;
    }
    if (node?.operator === "ROUTE") {
        return portId;
    }
    return portId;
}

function canvasWidthFootprintForNode(node) {
    return NODE_WIDTH + (node?.operator === "FILTER" || node?.operator === "ROUTE" ? FILTER_PORT_STACK_OUTSET_X : 0);
}

function clampCanvasNodePosition(node, x, y) {
    const rect = byId("canvas-drop-zone")?.getBoundingClientRect();
    if (!rect) {
        return {
            x: Math.max(CANVAS_NODE_PADDING, Math.round(x)),
            y: Math.max(CANVAS_NODE_PADDING, Math.round(y))
        };
    }

    const maxX = Math.max(CANVAS_NODE_PADDING, rect.width - canvasWidthFootprintForNode(node) - CANVAS_NODE_PADDING);
    const maxY = Math.max(CANVAS_NODE_PADDING, rect.height - NODE_HEIGHT - CANVAS_NODE_PADDING);

    return {
        x: Math.min(maxX, Math.max(CANVAS_NODE_PADDING, Math.round(x))),
        y: Math.min(maxY, Math.max(CANVAS_NODE_PADDING, Math.round(y)))
    };
}

function renderNodeHtml(node) {
    const tone = nodeTone(node.type);
    const inputPorts = inputPortsForNode(node);
    const outputPorts = outputPortsForNode(node);
    const isSelected = state.selectedNodeId === node.id;
    const left = Number(node.ui?.x ?? 120);
    const top = Number(node.ui?.y ?? 120);
    const metrics = isMonitorMode() ? monitorMetricsForNode(node.id) : null;
    const displayedOutputRecords = node.type === "SINK" ? metrics?.inputRecords : metrics?.outputRecords;
    const displayedOutputRate = node.type === "SINK" ? metrics?.inputRate : metrics?.outputRate;
    const inputCount = formatMetricCount(metrics?.inputRecords);
    const outputCount = formatMetricCount(displayedOutputRecords);
    const inputRate = formatMetricRate(metrics?.inputRate);
    const outputRate = formatMetricRate(displayedOutputRate);
    const monitorStatusText = metrics?.statusText || (metrics?.statusTone === "error" ? t("studio.monitor.metrics.error", "Metrics fetch failed") : t("studio.monitor.metrics.empty", "No realtime metrics"));
    const monitorStatusToneClass = metrics?.statusTone === "error"
        ? "text-red-600 dark:text-red-300"
        : "text-slate-500 dark:text-slate-400";
    const escapedMonitorStatusText = escapeHtml(monitorStatusText);
    const monitorBlock = isMonitorMode()
        ? `
            <div class="mt-3 pt-3 border-t border-slate-200 dark:border-slate-700">
                <div class="grid grid-cols-2 gap-2">
                    <div class="rounded border border-slate-200 dark:border-neutral-700 bg-slate-50 dark:bg-neutral-950/70 p-2">
                        <div class="text-[10px] text-slate-500 dark:text-slate-400">${t("studio.monitor.inputTotal", "Input total")}</div>
                        <div class="text-xs font-semibold text-slate-900 dark:text-neutral-100 mt-1">${inputCount}</div>
                    </div>
                    <div class="rounded border border-slate-200 dark:border-neutral-700 bg-slate-50 dark:bg-neutral-950/70 p-2">
                        <div class="text-[10px] text-slate-500 dark:text-slate-400">${t("studio.monitor.outputTotal", "Output total")}</div>
                        <div class="text-xs font-semibold text-slate-900 dark:text-neutral-100 mt-1">${outputCount}</div>
                    </div>
                    <div class="rounded border border-slate-200 dark:border-neutral-700 bg-slate-50 dark:bg-neutral-950/70 p-2">
                        <div class="text-[10px] text-slate-500 dark:text-slate-400">${t("studio.monitor.inputRate", "Input rate")}</div>
                        <div class="text-xs font-semibold text-slate-900 dark:text-neutral-100 mt-1">${inputRate}</div>
                    </div>
                    <div class="rounded border border-slate-200 dark:border-neutral-700 bg-slate-50 dark:bg-neutral-950/70 p-2">
                        <div class="text-[10px] text-slate-500 dark:text-slate-400">${t("studio.monitor.outputRate", "Output rate")}</div>
                        <div class="text-xs font-semibold text-slate-900 dark:text-neutral-100 mt-1">${outputRate}</div>
                    </div>
                </div>
                <div class="mt-2 text-[10px] ${monitorStatusToneClass}">${escapedMonitorStatusText}</div>
            </div>
        `
        : "";
    const inputPortsMarkup = inputPorts.length > 1
        ? `
            <div class="port-stack port-left-stack">
                ${inputPorts.map(port => {
                    const inputPortLabel = port.label ? `<span class="port-label port-label-left">${escapeHtml(port.label)}</span>` : "";
                    return `
                    <div class="port-stack-item port-stack-item-left">
                        ${inputPortLabel}
                        <div class="node-port port-left"
                             data-port-direction="input"
                             data-port-id="${port.id}"
                             data-node-id="${node.id}"></div>
                    </div>
                `;
                }).join("")}
            </div>
        `
        : inputPorts.map(port => `
            <div class="node-port port-left"
                 data-port-direction="input"
                 data-port-id="${port.id}"
                 data-node-id="${node.id}"></div>
        `).join("");
    const outputPortsMarkup = node.operator === "FILTER" || node.operator === "ROUTE" || node.operator === "DATA_QUALITY"
        ? `
            <div class="port-stack port-right-stack">
                ${outputPorts.map(port => `
                    <div class="port-stack-item">
                        <div class="node-port port-right"
                             data-port-direction="output"
                             data-port-id="${port.id}"
                             data-node-id="${node.id}"></div>
                    </div>
                `).join("")}
            </div>
        `
        : outputPorts.map(port => `
            <div class="node-port port-right"
                 data-port-direction="output"
                 data-port-id="${port.id}"
                 data-node-id="${node.id}"></div>
        `).join("");

    return `
        <div class="draggable-node ${isSelected ? "node-active" : ""} bg-white dark:bg-neutral-900/95 border border-slate-300 dark:border-neutral-700 rounded-lg shadow-xl"
             data-node-id="${node.id}"
             style="left:${left}px;top:${top}px;">
            ${inputPortsMarkup}
            ${outputPortsMarkup}
            <div class="drag-handle h-10 bg-slate-50 dark:bg-neutral-900 border-b border-slate-200 dark:border-neutral-700 rounded-t-lg flex items-center px-3">
                <span class="w-5 h-5 rounded flex items-center justify-center text-xs mr-2 font-bold ${tone.badgeClass}">${tone.badge}</span>
                <span class="text-sm font-medium text-slate-900 dark:text-neutral-100 flex-1 truncate">${escapeHtml(nodeDisplayTitle(node))}</span>
            </div>
            <div class="p-3">
                <div class="text-[10px] font-mono ${tone.tagClass}">${node.operator}</div>
                <div class="text-[11px] text-slate-500 dark:text-neutral-400 mt-2">${node.type}</div>
                ${monitorBlock}
            </div>
        </div>
    `;
}

function bindNodeInteractions() {
    if (!isEditorMode()) {
        return;
    }

    document.querySelectorAll(".draggable-node").forEach(nodeElement => {
        nodeElement.addEventListener("contextmenu", event => {
            showNodeContextMenu(nodeElement.dataset.nodeId, event);
        });

        nodeElement.addEventListener("click", event => {
            event.preventDefault();
            event.stopPropagation();
            selectNode(nodeElement.dataset.nodeId);
        });

        nodeElement.querySelector(".drag-handle")?.addEventListener("mousedown", event => {
            startNodeDrag(nodeElement.dataset.nodeId, event);
        });

        nodeElement.querySelectorAll('[data-port-direction="output"]').forEach(portElement => {
            portElement.addEventListener("mousedown", event => {
                startConnectionDrag(nodeElement.dataset.nodeId, event);
            });
        });

        nodeElement.querySelectorAll('[data-port-direction="input"]').forEach(portElement => {
            portElement.addEventListener("mouseup", event => {
                event.preventDefault();
                event.stopPropagation();
                completeConnectionDrag(
                    nodeElement.dataset.nodeId,
                    event.currentTarget.dataset.portId || DEFAULT_TARGET_PORT_ID
                );
            });
        });
    });
}

function renderNodes() {
    const layer = byId("nodes-layer");
    if (!layer) {
        return;
    }

    layer.innerHTML = state.nodes.map(renderNodeHtml).join("");
    bindNodeInteractions();
}

function renderNodeContextMenu() {
    const container = byId("canvas-container");
    const existingMenu = byId("node-context-menu");
    const targetNode = findNodeById(state.contextMenu.nodeId);

    if (!container || !isEditorMode() || !state.contextMenu.visible || !targetNode || state.selectedNodeId !== state.contextMenu.nodeId) {
        existingMenu?.remove();
        return;
    }

    const menu = existingMenu || document.createElement("div");
    menu.id = "node-context-menu";
    menu.className = [
        "absolute",
        "z-50",
        "min-w-[128px]",
        "rounded-xl",
        "border",
        "border-slate-700",
        "bg-slate-950/95",
        "p-1.5",
        "shadow-2xl",
        "shadow-slate-950/40",
        "backdrop-blur"
    ].join(" ");
    menu.innerHTML = `
        <button type="button"
                data-node-context-action="copy"
                class="flex w-full items-center rounded-lg px-3 py-2 text-left text-sm text-slate-200 transition-colors hover:bg-slate-800 hover:text-white">
            ${t("studio.action.copy", "Copy")}
        </button>
        <button type="button"
                data-node-context-action="delete"
                class="flex w-full items-center rounded-lg px-3 py-2 text-left text-sm text-red-300 transition-colors hover:bg-red-500/10 hover:text-red-200">
            ${t("studio.action.delete", "Delete")}
        </button>
    `;

    if (!existingMenu) {
        container.appendChild(menu);
    }

    const padding = 12;
    const maxX = Math.max(padding, container.clientWidth - menu.offsetWidth - padding);
    const maxY = Math.max(padding, container.clientHeight - menu.offsetHeight - padding);
    const left = Math.min(Math.max(padding, state.contextMenu.x), maxX);
    const top = Math.min(Math.max(padding, state.contextMenu.y), maxY);
    menu.style.left = `${left}px`;
    menu.style.top = `${top}px`;

    menu.querySelector('[data-node-context-action="copy"]')?.addEventListener("click", event => {
        event.preventDefault();
        event.stopPropagation();
        duplicateNode(state.contextMenu.nodeId);
    });

    menu.querySelector('[data-node-context-action="delete"]')?.addEventListener("click", event => {
        event.preventDefault();
        event.stopPropagation();
        deleteNode(state.contextMenu.nodeId);
    });
}

function fallbackPortAnchor(node, direction, portId) {
    const baseX = direction === "output"
        ? Number(node.ui?.x ?? 120) + NODE_WIDTH
        : Number(node.ui?.x ?? 120);
    const baseY = Number(node.ui?.y ?? 120) + NODE_HEIGHT / 2;
    if (direction === "output" && node.operator === "FILTER") {
        const portIndex = FILTER_OUTPUT_PORT_IDS.indexOf(portId);
        if (portIndex === 0) {
            return { x: baseX, y: baseY - 14 };
        }
        if (portIndex === 1) {
            return { x: baseX, y: baseY + 14 };
        }
        return null;
    }
    if (direction === "input" && node.operator === "STREAM_JOIN") {
        if (portId === "left") {
            return { x: baseX, y: baseY - 14 };
        }
        if (portId === "right") {
            return { x: baseX, y: baseY + 14 };
        }
    }
    return { x: baseX, y: baseY };
}

function resolvePortAnchor(nodeId, direction, portId) {
    const canvas = byId("canvas-drop-zone");
    const node = findNodeById(nodeId);
    if (!canvas || !node) {
        return null;
    }

    const resolvedPortId = direction === "input"
        ? portId || DEFAULT_TARGET_PORT_ID
        : outputPortIdForAnchor(node, portId);
    if (resolvedPortId == null || resolvedPortId === "") {
        return fallbackPortAnchor(node, direction, resolvedPortId);
    }
    const port = document.querySelector(
        `.draggable-node[data-node-id="${nodeId}"] [data-port-direction="${direction}"][data-port-id="${resolvedPortId}"]`
    );
    if (!port) {
        return fallbackPortAnchor(node, direction, resolvedPortId);
    }

    const canvasRect = canvas.getBoundingClientRect();
    const portRect = port.getBoundingClientRect();
    return {
        x: portRect.left - canvasRect.left + portRect.width / 2,
        y: portRect.top - canvasRect.top + portRect.height / 2
    };
}

function cubicBezierPoint(point0, point1, point2, point3, t) {
    const oneMinusT = 1 - t;
    return {
        x: oneMinusT ** 3 * point0.x
            + 3 * oneMinusT ** 2 * t * point1.x
            + 3 * oneMinusT * t ** 2 * point2.x
            + t ** 3 * point3.x,
        y: oneMinusT ** 3 * point0.y
            + 3 * oneMinusT ** 2 * t * point1.y
            + 3 * oneMinusT * t ** 2 * point2.y
            + t ** 3 * point3.y
    };
}

function resolveEdgeGeometry(edge) {
    const source = findNodeById(edge.sourceNodeId);
    const target = findNodeById(edge.targetNodeId);
    if (!source || !target) {
        return null;
    }

    const sourceAnchor = resolvePortAnchor(edge.sourceNodeId, "output", edge.sourcePortId);
    const targetAnchor = resolvePortAnchor(edge.targetNodeId, "input", edge.targetPortId);
    if (!sourceAnchor || !targetAnchor) {
        return null;
    }

    const x1 = sourceAnchor.x;
    const y1 = sourceAnchor.y;
    const x2 = targetAnchor.x;
    const y2 = targetAnchor.y;
    const curve = Math.max(120, Math.abs(x2 - x1) / 2);
    const control1 = { x: x1 + curve, y: y1 };
    const control2 = { x: x2 - curve, y: y2 };

    return {
        path: `M ${x1} ${y1} C ${control1.x} ${control1.y}, ${control2.x} ${control2.y}, ${x2} ${y2}`,
        midpoint: cubicBezierPoint({ x: x1, y: y1 }, control1, control2, { x: x2, y: y2 }, 0.5)
    };
}

function renderEdgePath(edge) {
    const geometry = resolveEdgeGeometry(edge);
    if (!geometry) {
        return "";
    }

    const stroke = state.selectedEdgeId === edge.id ? "#f59e0b" : "#3b82f6";
    return `<path data-edge-id="${edge.id}" d="${geometry.path}" fill="none" stroke="${stroke}" stroke-width="2.5" class="cursor-pointer" style="pointer-events: stroke;"></path>`;
}

function bindEdgeInteractions() {
    if (!isEditorMode()) {
        return;
    }

    document.querySelectorAll("[data-edge-id]").forEach(edgeElement => {
        edgeElement.addEventListener("click", event => {
            event.preventDefault();
            event.stopPropagation();
            selectEdge(edgeElement.dataset.edgeId);
        });
    });
}

function renderEdges() {
    const layer = byId("edges-layer");
    if (!layer) {
        return;
    }

    layer.classList.toggle("pointer-events-none", !isEditorMode());
    layer.innerHTML = `${state.edges.map(renderEdgePath).join("")}<path id="temp-connection-path" fill="none" stroke="#60a5fa" stroke-width="2.5" class="opacity-0"></path>`;
    bindEdgeInteractions();
}

function scheduleEdgeRefresh() {
    if (edgeRefreshFrameId != null) {
        window.cancelAnimationFrame(edgeRefreshFrameId);
    }

    edgeRefreshFrameId = window.requestAnimationFrame(() => {
        edgeRefreshFrameId = null;
        renderEdges();
    });
}

function renderInspector() {
    const selectedNode = findNodeById(state.selectedNodeId);
    const pipelineMetaPanel = byId("pipeline-meta-panel");
    const nodeDetailPanel = byId("node-detail-panel");
    const title = byId("selected-node-title");
    const tag = byId("selected-node-tag");
    const sourcePanel = byId("source-config-panel");
    const transformPanel = byId("transform-config-panel");
    const sinkPanel = byId("sink-config-panel");

    if (pipelineMetaPanel) {
        pipelineMetaPanel.classList.toggle("hidden", Boolean(selectedNode));
    }
    if (nodeDetailPanel) {
        nodeDetailPanel.classList.toggle("hidden", !selectedNode);
    }

    if (title) {
        title.textContent = selectedNode
            ? t("studio.node.configTitle", "{0} configuration", [nodeDisplayTitle(selectedNode)])
            : t("studio.node.selectPrompt", "Select a node");
    }
    if (tag) {
        tag.textContent = selectedNode ? selectedNode.type : "NONE";
        tag.className = selectedNode
            ? "sc-pill bg-blue-50 dark:bg-blue-500/10 text-blue-700 dark:text-blue-200 border border-blue-200 dark:border-blue-500/30"
            : "sc-pill bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border border-slate-300 dark:border-slate-600";
    }
    if (sourcePanel) {
        sourcePanel.classList.toggle("active", selectedNode?.type === "SOURCE");
    }
    if (transformPanel) {
        transformPanel.classList.toggle("active", selectedNode?.type === "TRANSFORMER");
    }
    if (sinkPanel) {
        sinkPanel.classList.toggle("active", selectedNode?.type === "SINK");
    }

    fillInspectorFromSelectedNode();
}

function buildDefinition() {
    syncPipelineMetaFromForm();
    syncSelectedNodeConfigFromInspector();

    return {
        pipelineId: state.currentPipelineId ? `pipeline-${state.currentPipelineId}` : `pipeline-${Date.now()}`,
        nodes: state.nodes.map(node => ({
            id: node.id,
            name: node.name,
            displayName: node.displayName || "",
            type: toDefinitionNodeType(node.type),
            operator: node.operator,
            config: cloneConfig(node.config),
            ui: {
                x: Math.round(Number(node.ui?.x ?? 120)),
                y: Math.round(Number(node.ui?.y ?? 120))
            }
        })),
        edges: state.edges.map(edge => ({
            id: edge.id,
            sourceNodeId: edge.sourceNodeId,
            sourcePortId: sourcePortIdForEdge(edge, findNodeById(edge.sourceNodeId)),
            targetNodeId: edge.targetNodeId,
            targetPortId: edge.targetPortId
        }))
    };
}

function renderStudio() {
    renderHeader();
    renderNodes();
    renderEdges();
    renderInspector();
    renderPreviewResults();
    renderNodeContextMenu();
    scheduleEdgeRefresh();
}

function selectNode(nodeId) {
    hideNodeContextMenu();
    state.selectedNodeId = nodeId;
    state.selectedEdgeId = null;
    renderStudio();
}

function closeNodeInspector() {
    syncSelectedNodeConfigFromInspector();
    hideNodeContextMenu();
    state.selectedNodeId = null;
    state.selectedEdgeId = null;
    renderStudio();
}

function selectEdge(edgeId) {
    syncSelectedNodeConfigFromInspector();
    hideNodeContextMenu();
    state.selectedEdgeId = edgeId;
    state.selectedNodeId = null;
    renderStudio();
}

function nextNodeId(prefix) {
    return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
}

function nextEdgeId(sourceNodeId, targetNodeId) {
    return `edge-${sourceNodeId}-${targetNodeId}-${Date.now()}`;
}

function canvasPointFromEvent(event, node) {
    const rect = byId("canvas-drop-zone")?.getBoundingClientRect();
    if (!rect) {
        const x = 120;
        const y = 120;
        return clampCanvasNodePosition(node, x, y);
    }

    const x = event.clientX - rect.left - NODE_WIDTH / 2;
    const y = event.clientY - rect.top - NODE_HEIGHT / 2;
    return clampCanvasNodePosition(node, x, y);
}

function createNodeFromPalette(operatorKey, x, y) {
    const schema = OPERATOR_CATALOG[operatorKey];
    if (!schema) {
        return;
    }

    const node = {
        id: nextNodeId(schema.type.toLowerCase()),
        name: schema.defaultName,
        displayName: "",
        type: schema.type,
        operator: schema.operator,
        config: cloneConfig(schema.defaultConfig),
        ui: { x, y }
    };
    const clampedPosition = clampCanvasNodePosition(node, x, y);
    node.ui = clampedPosition;

    state.nodes.push(node);
    hideNodeContextMenu();
    state.selectedNodeId = node.id;
    state.selectedEdgeId = null;
    markDirty();
    renderStudio();
}

function handlePaletteDragStart(event) {
    event.dataTransfer?.setData("text/operator-key", event.currentTarget.dataset.operatorKey);
}

function handleCanvasDrop(event) {
    event.preventDefault();
    const operatorKey = event.dataTransfer?.getData("text/operator-key");
    if (!operatorKey) {
        return;
    }

    const schema = OPERATOR_CATALOG[operatorKey];
    const nodeTemplate = schema ? { type: schema.type, operator: schema.operator } : null;
    const point = canvasPointFromEvent(event, nodeTemplate);
    const clampedPoint = clampCanvasNodePosition(nodeTemplate, point.x, point.y);
    createNodeFromPalette(operatorKey, clampedPoint.x, clampedPoint.y);
}

function startNodeDrag(nodeId, event) {
    if (event.button !== 0) {
        return;
    }

    event.preventDefault();
    event.stopPropagation();

    const node = findNodeById(nodeId);
    if (!node) {
        return;
    }

    hideNodeContextMenu();
    selectNode(nodeId);
    state.dragState = {
        nodeId,
        startMouseX: event.clientX,
        startMouseY: event.clientY,
        startX: node.ui.x,
        startY: node.ui.y
    };

    document.addEventListener("mousemove", onNodeDragMove);
    document.addEventListener("mouseup", stopNodeDrag, { once: true });
}

function onNodeDragMove(event) {
    if (!state.dragState) {
        return;
    }

    const node = findNodeById(state.dragState.nodeId);
    if (!node) {
        return;
    }

    const clampedPosition = clampCanvasNodePosition(
        node,
        state.dragState.startX + event.clientX - state.dragState.startMouseX,
        state.dragState.startY + event.clientY - state.dragState.startMouseY
    );
    node.ui.x = clampedPosition.x;
    node.ui.y = clampedPosition.y;
    markDirty();
    renderStudio();
}

function stopNodeDrag() {
    state.dragState = null;
    document.removeEventListener("mousemove", onNodeDragMove);
}

function startConnectionDrag(nodeId, event) {
    if (event.button !== 0) {
        return;
    }

    event.preventDefault();
    event.stopPropagation();

    const source = findNodeById(nodeId);
    if (!source || source.type === "SINK") {
        return;
    }

    hideNodeContextMenu();
    selectNode(nodeId);
    const sourcePortId = event.currentTarget?.dataset?.portId || DEFAULT_SOURCE_PORT_ID;
    state.connectState = {
        sourceNodeId: nodeId,
        sourcePortId,
        pointerX: event.clientX,
        pointerY: event.clientY
    };

    document.addEventListener("mousemove", onConnectionDragMove);
    document.addEventListener("mouseup", cancelConnectionDrag, { once: true });
    renderEdges();
    updateTemporaryConnectionPath();
}

function onConnectionDragMove(event) {
    if (!state.connectState) {
        return;
    }

    state.connectState.pointerX = event.clientX;
    state.connectState.pointerY = event.clientY;
    updateTemporaryConnectionPath();
}

function updateTemporaryConnectionPath() {
    if (!state.connectState) {
        return;
    }

    const source = findNodeById(state.connectState.sourceNodeId);
    const tempPath = byId("temp-connection-path");
    const rect = byId("canvas-drop-zone")?.getBoundingClientRect();
    if (!source || !tempPath || !rect) {
        return;
    }

    const sourceAnchor = resolvePortAnchor(
        state.connectState.sourceNodeId,
        "output",
        state.connectState.sourcePortId
    );
    if (!sourceAnchor) {
        return;
    }
    const x1 = sourceAnchor.x;
    const y1 = sourceAnchor.y;
    const x2 = state.connectState.pointerX - rect.left;
    const y2 = state.connectState.pointerY - rect.top;
    const curve = Math.max(120, Math.abs(x2 - x1) / 2);

    tempPath.setAttribute("d", `M ${x1} ${y1} C ${x1 + curve} ${y1}, ${x2 - curve} ${y2}, ${x2} ${y2}`);
    tempPath.classList.remove("opacity-0");
    tempPath.classList.add("opacity-100");
}

function completeConnectionDrag(targetNodeId, targetPortId = "input-0") {
    if (!state.connectState || state.connectState.sourceNodeId === targetNodeId) {
        cancelConnectionDrag();
        return;
    }

    const source = findNodeById(state.connectState.sourceNodeId);
    const target = findNodeById(targetNodeId);
    const duplicate = state.edges.find(edge =>
        edge.sourceNodeId === source?.id
        && edge.sourcePortId === (state.connectState.sourcePortId || DEFAULT_SOURCE_PORT_ID)
        && edge.targetNodeId === target?.id
        && edge.targetPortId === targetPortId
    );

    if (!source || !target || source.type === "SINK" || target.type === "SOURCE") {
        cancelConnectionDrag();
        return;
    }

    if (duplicate) {
        cancelConnectionDrag();
        deleteEdge(duplicate.id);
        return;
    }

    const edge = {
        id: nextEdgeId(source.id, target.id),
        sourceNodeId: source.id,
        sourcePortId: state.connectState.sourcePortId || DEFAULT_SOURCE_PORT_ID,
        targetNodeId: target.id,
        targetPortId
    };

    state.edges.push(edge);
    state.selectedEdgeId = edge.id;
    state.selectedNodeId = null;
    markDirty();
    cancelConnectionDrag();
    renderStudio();
}

function cancelConnectionDrag() {
    state.connectState = null;
    document.removeEventListener("mousemove", onConnectionDragMove);
    renderEdges();
}

function deleteEdge(edgeId) {
    state.edges = state.edges.filter(edge => edge.id !== edgeId);
    if (state.selectedEdgeId === edgeId) {
        state.selectedEdgeId = null;
    }
    hideNodeContextMenu();
    markDirty();
    renderStudio();
}

function deleteNode(nodeId) {
    if (!nodeId) {
        return;
    }

    state.edges = state.edges.filter(edge => edge.sourceNodeId !== nodeId && edge.targetNodeId !== nodeId);
    state.nodes = state.nodes.filter(node => node.id !== nodeId);
    state.selectedNodeId = state.selectedNodeId === nodeId ? state.nodes[0]?.id ?? null : state.selectedNodeId;
    state.selectedEdgeId = null;
    hideNodeContextMenu();
    markDirty();
    renderStudio();
}

function deleteSelectedSelection() {
    if (state.selectedEdgeId) {
        deleteEdge(state.selectedEdgeId);
        return;
    }

    if (state.selectedNodeId) {
        deleteNode(state.selectedNodeId);
    }
}

function duplicateNode(nodeId) {
    const sourceNode = findNodeById(nodeId);
    if (!sourceNode) {
        return;
    }

    const duplicate = normalizeNode({
        id: `${sourceNode.operator.toLowerCase()}-${Date.now()}`,
        name: sourceNode.name,
        displayName: sourceNode.displayName,
        type: sourceNode.type,
        operator: sourceNode.operator,
        config: cloneConfig(sourceNode.config),
        ui: {
            x: Number(sourceNode.ui?.x ?? 120) + 32,
            y: Number(sourceNode.ui?.y ?? 120) + 32
        }
    });

    state.nodes = [...state.nodes, duplicate];
    state.edges = [...state.edges];
    state.selectedNodeId = duplicate.id;
    state.selectedEdgeId = null;
    hideNodeContextMenu();
    markDirty();
    renderStudio();
}

function normalizeNode(node) {
    const schema = catalogEntryForOperator(node?.operator);
    const normalizedNode = {
        id: node.id,
        name: node.name || schema?.defaultName || node.operator || t("studio.node.unnamed", "Unnamed node"),
        displayName: String(node.displayName || "").trim(),
        type: toStudioNodeType(node.type),
        operator: node.operator,
        config: {
            ...cloneConfig(schema?.defaultConfig),
            ...cloneConfig(node.config)
        },
        ui: {
            x: Number(node.ui?.x ?? 120),
            y: Number(node.ui?.y ?? 120)
        }
    };
    const clampedPosition = clampCanvasNodePosition(
        normalizedNode,
        Number(node.ui?.x ?? 120),
        Number(node.ui?.y ?? 120)
    );
    normalizedNode.ui = clampedPosition;
    return normalizedNode;
}

function normalizeEdge(edge, nodes = state.nodes) {
    const sourceNode = nodes.find(node => node.id === edge.sourceNodeId);
    return {
        id: edge.id || `edge-${edge.sourceNodeId}-${edge.targetNodeId}`,
        sourceNodeId: edge.sourceNodeId,
        sourcePortId: sourcePortIdForEdge(edge, sourceNode),
        targetNodeId: edge.targetNodeId,
        targetPortId: edge.targetPortId
    };
}

function loadDefinitionIntoState(definition) {
    const nodes = Array.isArray(definition?.nodes) ? definition.nodes.map(normalizeNode) : [];
    state.nodes = nodes;
    state.edges = Array.isArray(definition?.edges) ? definition.edges.map(edge => normalizeEdge(edge, nodes)) : [];
    hideNodeContextMenu();
    state.selectedNodeId = null;
    state.selectedEdgeId = null;
    clearDirty();
    renderStudio();
}

function isPositiveNumber(value) {
    const numericValue = Number(value);
    return Number.isFinite(numericValue) && numericValue > 0;
}

function isNonNegativeNumber(value) {
    const numericValue = Number(value);
    return Number.isFinite(numericValue) && numericValue >= 0;
}

function isIntegerNumber(value) {
    const numericValue = Number(value);
    return Number.isInteger(numericValue);
}

function validateAggregateConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (config.mode === "GROUPED" && (!Array.isArray(config.groupBy) || config.groupBy.length === 0)) {
        return { ok: false, message: t("studio.validation.aggregate.groupByRequired", "{0}: grouped aggregates require at least one group-by field.", [nodeName]) };
    }
    if (!Array.isArray(config.aggregations) || config.aggregations.length === 0) {
        return { ok: false, message: t("studio.validation.aggregate.aggregationsRequired", "{0}: add at least one aggregation.", [nodeName]) };
    }

    for (const aggregation of config.aggregations) {
        if (!aggregation.outputField) {
            return { ok: false, message: t("studio.validation.aggregate.outputFieldRequired", "{0}: every aggregation needs an output field.", [nodeName]) };
        }
        if (aggregation.function !== "COUNT" && !aggregation.field) {
            return { ok: false, message: t("studio.validation.aggregate.aggregationFieldRequired", "{0}: {1} requires an input field.", [nodeName, aggregation.function || "AGGREGATION"]) };
        }
        if (aggregation.function === "TOP_N" && !isPositiveNumber(aggregation.limit || 10)) {
            return { ok: false, message: t("studio.validation.aggregate.limitPositive", "{0}: TOP_N limit must be greater than 0.", [nodeName]) };
        }
    }

    if (config.windowType === "COUNT" && !isPositiveNumber(config.countWindowSize)) {
        return { ok: false, message: t("studio.validation.aggregate.countWindowSizePositive", "{0}: count window size must be greater than 0.", [nodeName]) };
    }
    if (config.windowType === "COUNT" && !isIntegerNumber(config.countWindowSize)) {
        return { ok: false, message: t("studio.validation.aggregate.countWindowSizeInteger", "{0}: count window size must be a valid integer.", [nodeName]) };
    }
    if (config.windowType !== "COUNT" && !isPositiveNumber(config.windowSize)) {
        return { ok: false, message: t("studio.validation.aggregate.windowSizePositive", "{0}: time window size must be greater than 0.", [nodeName]) };
    }
    if (config.windowType !== "COUNT" && !isIntegerNumber(config.windowSize)) {
        return { ok: false, message: t("studio.validation.aggregate.windowSizeInteger", "{0}: time window size must be a valid integer.", [nodeName]) };
    }
    if (config.windowType === "SLIDING_TIME" && !isPositiveNumber(config.windowSlide)) {
        return { ok: false, message: t("studio.validation.aggregate.windowSlidePositive", "{0}: sliding window interval must be greater than 0.", [nodeName]) };
    }
    if (config.windowType === "SLIDING_TIME" && !isIntegerNumber(config.windowSlide)) {
        return { ok: false, message: t("studio.validation.aggregate.windowSlideInteger", "{0}: sliding window interval must be a valid integer.", [nodeName]) };
    }
    if (config.windowType === "SLIDING_TIME" && Number(config.windowSlide) > Number(config.windowSize)) {
        return { ok: false, message: t("studio.validation.aggregate.windowSlideMaxWindowSize", "{0}: sliding window interval must be less than or equal to the window size.", [nodeName]) };
    }
    if (config.timeMode === "EVENT_TIME" && config.windowType !== "COUNT" && !isNonNegativeNumber(config.watermarkDelay)) {
        return { ok: false, message: t("studio.validation.aggregate.watermarkDelayNonNegative", "{0}: watermark delay must be greater than or equal to 0.", [nodeName]) };
    }
    if (config.timeMode === "EVENT_TIME" && config.windowType !== "COUNT" && !isIntegerNumber(config.watermarkDelay)) {
        return { ok: false, message: t("studio.validation.aggregate.watermarkDelayInteger", "{0}: watermark delay must be a valid integer.", [nodeName]) };
    }

    return { ok: true };
}

function validateDeduplicateConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (!Array.isArray(config.keyFields) || config.keyFields.length === 0) {
        return { ok: false, message: t("studio.validation.deduplicate.keyFieldsRequired", "{0}: add at least one deduplicate key field.", [nodeName]) };
    }
    const timeMode = config.timeMode || "PROCESSING_TIME";
    if (!["PROCESSING_TIME", "EVENT_TIME"].includes(timeMode)) {
        return { ok: false, message: t("studio.validation.deduplicate.timeModeUnsupported", "{0}: time mode must be PROCESSING_TIME or EVENT_TIME.", [nodeName]) };
    }
    if (timeMode === "PROCESSING_TIME") {
        if (!isPositiveNumber(config.ttlSeconds)) {
            return { ok: false, message: t("studio.validation.deduplicate.ttlSecondsPositive", "{0}: TTL seconds must be greater than 0.", [nodeName]) };
        }
        if (!isIntegerNumber(config.ttlSeconds)) {
            return { ok: false, message: t("studio.validation.deduplicate.ttlSecondsInteger", "{0}: TTL seconds must be a valid integer.", [nodeName]) };
        }
        if (!["FIRST", "LAST"].includes(config.keepStrategy || "FIRST")) {
            return { ok: false, message: t("studio.validation.deduplicate.keepStrategyUnsupported", "{0}: processing-time keep strategy must be FIRST or LAST.", [nodeName]) };
        }
    } else {
        if (!String(config.eventTimeField || "").trim()) {
            return { ok: false, message: t("studio.validation.deduplicate.eventTimeFieldRequired", "{0}: event time field is required.", [nodeName]) };
        }
        if (!isPositiveNumber(config.windowSeconds)) {
            return { ok: false, message: t("studio.validation.deduplicate.windowSecondsPositive", "{0}: window seconds must be greater than 0.", [nodeName]) };
        }
        if (!isIntegerNumber(config.windowSeconds)) {
            return { ok: false, message: t("studio.validation.deduplicate.windowSecondsInteger", "{0}: window seconds must be a valid integer.", [nodeName]) };
        }
        if (!isNonNegativeNumber(config.watermarkDelaySeconds)) {
            return { ok: false, message: t("studio.validation.deduplicate.watermarkDelaySecondsNonNegative", "{0}: watermark delay seconds must be greater than or equal to 0.", [nodeName]) };
        }
        if (!isIntegerNumber(config.watermarkDelaySeconds)) {
            return { ok: false, message: t("studio.validation.deduplicate.watermarkDelaySecondsInteger", "{0}: watermark delay seconds must be a valid integer.", [nodeName]) };
        }
        if (!["FIRST", "LAST", "EVENT_TIME_LATEST"].includes(config.keepStrategy || "EVENT_TIME_LATEST")) {
            return { ok: false, message: t("studio.validation.deduplicate.keepStrategyUnsupported", "{0}: event-time keep strategy must be FIRST, LAST, or EVENT_TIME_LATEST.", [nodeName]) };
        }
        if ((config.lateDataStrategy || "DISCARD") !== "DISCARD") {
            return { ok: false, message: t("studio.validation.deduplicate.lateDataStrategyUnsupported", "{0}: late data strategy must be DISCARD.", [nodeName]) };
        }
    }
    if ((config.duplicateStrategy || "DISCARD") !== "DISCARD") {
        return { ok: false, message: t("studio.validation.deduplicate.duplicateStrategyUnsupported", "{0}: duplicate strategy must be DISCARD.", [nodeName]) };
    }
    return { ok: true };
}

function validateTimeDeriveConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (!String(config.sourceField || "").trim()) {
        return { ok: false, message: t("studio.validation.timeDerive.sourceFieldRequired", "{0}: source field is required.", [nodeName]) };
    }
    if ((config.sourceFormat || "AUTO") === "PATTERN" && !String(config.sourcePattern || "").trim()) {
        return { ok: false, message: t("studio.validation.timeDerive.patternRequired", "{0}: pattern is required for PATTERN source format and FORMAT derivations.", [nodeName]) };
    }
    if (!Array.isArray(config.derivations) || config.derivations.length === 0) {
        return { ok: false, message: t("studio.validation.timeDerive.derivationsRequired", "{0}: add at least one derivation.", [nodeName]) };
    }
    const seenFields = new Set();
    for (const derivation of config.derivations) {
        const outputField = String(derivation?.outputField || "").trim();
        if (!outputField) {
            return { ok: false, message: t("studio.validation.timeDerive.outputFieldRequired", "{0}: every derivation needs an output field.", [nodeName]) };
        }
        if (seenFields.has(outputField)) {
            return { ok: false, message: t("studio.validation.timeDerive.duplicateOutputField", "{0}: derived output fields must be unique.", [nodeName]) };
        }
        seenFields.add(outputField);
        if (timeDeriveRequiresPattern(derivation?.type || "DATE") && !String(derivation?.pattern || "").trim()) {
            return { ok: false, message: t("studio.validation.timeDerive.patternRequired", "{0}: pattern is required for PATTERN source format and FORMAT derivations.", [nodeName]) };
        }
    }
    return { ok: true };
}

function validateMaskHashConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (!Array.isArray(config.rules) || config.rules.length === 0) {
        return { ok: false, message: t("studio.validation.maskHash.rulesRequired", "{0}: add at least one mask/hash rule.", [nodeName]) };
    }
    for (const rule of config.rules) {
        if (!String(rule?.sourceField || "").trim()) {
            return { ok: false, message: t("studio.validation.maskHash.sourceFieldRequired", "{0}: every rule needs a source field.", [nodeName]) };
        }
        if (!String(rule?.targetField || "").trim()) {
            return { ok: false, message: t("studio.validation.maskHash.targetFieldRequired", "{0}: every rule needs a target field.", [nodeName]) };
        }
        if (rule.action === "HASH") {
            if (!String(rule?.algorithm || "").trim()) {
                return { ok: false, message: t("studio.validation.maskHash.algorithmRequired", "{0}: hash rules need an algorithm.", [nodeName]) };
            }
            continue;
        }
        if (!String(rule?.maskChar || "").trim()) {
            return { ok: false, message: t("studio.validation.maskHash.maskCharRequired", "{0}: mask rules need a mask character.", [nodeName]) };
        }
        if (!isNonNegativeNumber(rule.keepFirst) || !isIntegerNumber(rule.keepFirst) || String(rule.keepFirst ?? "").trim() === "") {
            return { ok: false, message: t("studio.validation.maskHash.keepFirstNonNegative", "{0}: keep first must be a non-negative integer.", [nodeName]) };
        }
        if (!isNonNegativeNumber(rule.keepLast) || !isIntegerNumber(rule.keepLast) || String(rule.keepLast ?? "").trim() === "") {
            return { ok: false, message: t("studio.validation.maskHash.keepLastNonNegative", "{0}: keep last must be a non-negative integer.", [nodeName]) };
        }
    }
    return { ok: true };
}

function validateCaseWhenConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (!String(config.targetField || "").trim()) {
        return { ok: false, message: t("studio.validation.caseWhen.targetFieldRequired", "{0}: target field is required.", [nodeName]) };
    }
    if (!Array.isArray(config.cases) || config.cases.length === 0) {
        return { ok: false, message: t("studio.validation.caseWhen.casesRequired", "{0}: add at least one case.", [nodeName]) };
    }
    if (config.cases.some(item => !String(item?.condition || "").trim())) {
        return { ok: false, message: t("studio.validation.caseWhen.conditionRequired", "{0}: every case needs a condition.", [nodeName]) };
    }
    for (const item of config.cases) {
        if (caseValueMode(item) === "EXPRESSION") {
            if (!String(item?.expression || "").trim()) {
                return { ok: false, message: t("studio.validation.caseWhen.valueRequired", "{0}: every case needs an output value or expression.", [nodeName]) };
            }
        } else if (!String(item?.value || "").trim()) {
            return { ok: false, message: t("studio.validation.caseWhen.valueRequired", "{0}: every case needs an output value or expression.", [nodeName]) };
        }
    }
    if ((config.defaultMode || "NONE") === "VALUE" && !String(config.defaultValue || "").trim()) {
        return { ok: false, message: t("studio.validation.caseWhen.defaultValueRequired", "{0}: default output value is required.", [nodeName]) };
    }
    if ((config.defaultMode || "NONE") === "EXPRESSION" && !String(config.defaultExpression || "").trim()) {
        return { ok: false, message: t("studio.validation.caseWhen.defaultValueRequired", "{0}: default output expression is required.", [nodeName]) };
    }
    return { ok: true };
}

function validateRouteConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    const routePortPattern = /^[A-Za-z0-9_-]+$/;
    if (!Array.isArray(config.routes) || config.routes.length === 0) {
        return { ok: false, message: t("studio.validation.route.routesRequired", "{0}: add at least one route.", [nodeName]) };
    }
    const seenPorts = new Set();
    for (const route of config.routes) {
        const portId = String(route?.portId || "").trim();
        const condition = String(route?.condition || "").trim();
        if (!portId || !condition) {
            return { ok: false, message: t("studio.validation.route.routeRequired", "{0}: every route needs a port and condition.", [nodeName]) };
        }
        if (!routePortPattern.test(portId)) {
            return { ok: false, message: t("studio.validation.route.invalidPortId", "{0}: route port can contain only letters, numbers, underscores, and hyphens.", [nodeName]) };
        }
        if (seenPorts.has(portId)) {
            return { ok: false, message: t("studio.validation.route.duplicatePortId", "{0}: route output ports must be unique.", [nodeName]) };
        }
        seenPorts.add(portId);
    }
    if (config.includeUnmatched !== false) {
        const unmatchedPort = String(config.unmatchedPort || "unmatched").trim();
        if (!routePortPattern.test(unmatchedPort)) {
            return { ok: false, message: t("studio.validation.route.invalidPortId", "{0}: route port can contain only letters, numbers, underscores, and hyphens.", [nodeName]) };
        }
        if (seenPorts.has(unmatchedPort)) {
            return { ok: false, message: t("studio.validation.route.duplicatePortId", "{0}: route output ports must be unique.", [nodeName]) };
        }
    }
    return { ok: true };
}

function validateLookupEnrichConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (!String(config.sourceField || "").trim()) {
        return { ok: false, message: t("studio.validation.lookupEnrich.sourceFieldRequired", "{0}: source field is required.", [nodeName]) };
    }
    if (!String(config.targetField || "").trim()) {
        return { ok: false, message: t("studio.validation.lookupEnrich.targetFieldRequired", "{0}: target field is required.", [nodeName]) };
    }
    if (!Array.isArray(config.entries) || config.entries.length === 0) {
        return { ok: false, message: t("studio.validation.lookupEnrich.entriesRequired", "{0}: add at least one lookup entry.", [nodeName]) };
    }

    const seenKeys = new Set();
    for (const entry of config.entries) {
        const key = String(entry?.key || "").trim();
        if (!key || !lookupEnrichEntryHasValue(entry)) {
            return { ok: false, message: t("studio.validation.lookupEnrich.entryRequired", "{0}: each lookup entry needs both key and value.", [nodeName]) };
        }
        if (seenKeys.has(key)) {
            return { ok: false, message: t("studio.validation.lookupEnrich.duplicateKey", "{0}: lookup keys must be unique.", [nodeName]) };
        }
        seenKeys.add(key);
    }
    return { ok: true };
}

function validateLookupJoinConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (!String(config.sourceField || "").trim()) {
        return { ok: false, message: t("studio.validation.lookupJoin.sourceFieldRequired", "{0}: source field is required.", [nodeName]) };
    }
    if (!String(config.targetField || "").trim()) {
        return { ok: false, message: t("studio.validation.lookupJoin.targetFieldRequired", "{0}: target field is required.", [nodeName]) };
    }
    if (!Array.isArray(config.entries) || config.entries.length === 0) {
        return { ok: false, message: t("studio.validation.lookupJoin.entriesRequired", "{0}: add at least one lookup join entry.", [nodeName]) };
    }
    const keys = new Set();
    for (const entry of config.entries) {
        const key = String(entry?.key || "").trim();
        const fields = entry?.fields;
        if (!key || !fields || typeof fields !== "object" || Array.isArray(fields) || Object.keys(fields).length === 0) {
            return { ok: false, message: t("studio.validation.lookupJoin.entryRequired", "{0}: each lookup join entry needs key and fields.", [nodeName]) };
        }
        if (keys.has(key)) {
            return { ok: false, message: t("studio.validation.lookupJoin.duplicateKey", "{0}: lookup join keys must be unique.", [nodeName]) };
        }
        keys.add(key);
    }
    return { ok: true };
}

function validateStreamJoinConfig(node, definition = null) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (!String(config.leftKeyField || "").trim()) {
        return { ok: false, message: t("studio.validation.streamJoin.leftKeyFieldRequired", "{0}: left key field is required.", [nodeName]) };
    }
    if (!String(config.rightKeyField || "").trim()) {
        return { ok: false, message: t("studio.validation.streamJoin.rightKeyFieldRequired", "{0}: right key field is required.", [nodeName]) };
    }
    if (!String(config.targetField || "").trim()) {
        return { ok: false, message: t("studio.validation.streamJoin.targetFieldRequired", "{0}: target field is required.", [nodeName]) };
    }
    if (!isNonNegativeNumber(config.windowBefore) || !isIntegerNumber(config.windowBefore)) {
        return { ok: false, message: t("studio.validation.streamJoin.windowBeforeNonNegative", "{0}: left join window must be a non-negative integer.", [nodeName]) };
    }
    if (!isNonNegativeNumber(config.windowAfter) || !isIntegerNumber(config.windowAfter)) {
        return { ok: false, message: t("studio.validation.streamJoin.windowAfterNonNegative", "{0}: right join window must be a non-negative integer.", [nodeName]) };
    }
    if (Number(config.windowBefore || 0) === 0 && Number(config.windowAfter || 0) === 0) {
        return { ok: false, message: t("studio.validation.streamJoin.windowRangeRequired", "{0}: configure a non-zero join window.", [nodeName]) };
    }
    if (!isNonNegativeNumber(config.watermarkDelay) || !isIntegerNumber(config.watermarkDelay)) {
        return { ok: false, message: t("studio.validation.streamJoin.watermarkDelayNonNegative", "{0}: watermark delay must be a non-negative integer.", [nodeName]) };
    }
    if (definition) {
        const incomingPorts = new Set((definition.edges || [])
            .filter(edge => edge.targetNodeId === node.id)
            .map(edge => edge.targetPortId || DEFAULT_TARGET_PORT_ID));
        if (!incomingPorts.has("left") || !incomingPorts.has("right")) {
            return { ok: false, message: t("studio.validation.streamJoin.inputPortsRequired", "{0}: connect both left and right inputs.", [nodeName]) };
        }
    }
    return { ok: true };
}

function validateFlattenConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (!String(config.sourceField || "").trim()) {
        return { ok: false, message: t("studio.validation.flatten.sourceFieldRequired", "{0}: source field is required.", [nodeName]) };
    }
    if (!String(config.delimiter || "").trim()) {
        return { ok: false, message: t("studio.validation.flatten.delimiterRequired", "{0}: delimiter is required.", [nodeName]) };
    }
    return { ok: true };
}

function validateExplodeConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (!String(config.sourceField || "").trim()) {
        return { ok: false, message: t("studio.validation.explode.sourceFieldRequired", "{0}: source field is required.", [nodeName]) };
    }
    if (!String(config.targetField || "").trim()) {
        return { ok: false, message: t("studio.validation.explode.targetFieldRequired", "{0}: target field is required.", [nodeName]) };
    }
    return { ok: true };
}

function validateHdfsFileSourceConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (!String(config["fs.defaultFS"] || "").trim()) {
        return { ok: false, message: t("studio.validation.hdfsFileSource.defaultFsRequired", "{0}: default FS is required.", [nodeName]) };
    }
    if (!String(config.path || "").trim()) {
        return { ok: false, message: t("studio.validation.hdfsFileSource.pathRequired", "{0}: path is required.", [nodeName]) };
    }
    if (!String(config.file_format_type || "").trim()) {
        return { ok: false, message: t("studio.validation.hdfsFileSource.fileFormatRequired", "{0}: file format is required.", [nodeName]) };
    }
    if (config.readMode === "INCREMENTAL" && !isPositiveNumber(config.pollIntervalMillis)) {
        return { ok: false, message: t("studio.validation.hdfsFileSource.pollIntervalPositive", "{0}: poll interval must be greater than 0.", [nodeName]) };
    }
    if (!isNonNegativeNumber(config.maxPolls ?? 0) || !isIntegerNumber(config.maxPolls ?? 0)) {
        return { ok: false, message: t("studio.validation.hdfsFileSource.maxPollsNonNegative", "{0}: max polls must be a non-negative integer.", [nodeName]) };
    }
    if (!isNonNegativeNumber(config.skip_header_row_number ?? 0)
            || !isIntegerNumber(config.skip_header_row_number ?? 0)) {
        return { ok: false, message: t("studio.validation.hdfsFileSource.skipHeaderNonNegative", "{0}: skipped header rows must be a non-negative integer.", [nodeName]) };
    }
    if (config.schema && (typeof config.schema !== "object" || Array.isArray(config.schema))) {
        return { ok: false, message: t("studio.validation.hdfsFileSource.schemaObject", "{0}: schema must be a JSON object.", [nodeName]) };
    }
    if (config.file_filter_pattern) {
        try {
            new RegExp(config.file_filter_pattern);
        } catch (error) {
            return { ok: false, message: t("studio.validation.hdfsFileSource.fileFilterPattern", "{0}: file filter pattern is invalid.", [nodeName]) };
        }
    }
    return { ok: true };
}

function validateHdfsFileSinkConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (!String(config["fs.defaultFS"] || "").trim()) {
        return { ok: false, message: t("studio.validation.hdfsFileSink.defaultFsRequired", "{0}: default FS is required.", [nodeName]) };
    }
    if (!String(config.path || "").trim()) {
        return { ok: false, message: t("studio.validation.hdfsFileSink.pathRequired", "{0}: path is required.", [nodeName]) };
    }
    if (!String(config.file_format_type || "").trim()) {
        return { ok: false, message: t("studio.validation.hdfsFileSink.fileFormatRequired", "{0}: file format is required.", [nodeName]) };
    }
    if (!isPositiveNumber(config.batch_size ?? 1000)
            || !isIntegerNumber(config.batch_size ?? 1000)) {
        return { ok: false, message: t("studio.validation.hdfsFileSink.batchSizePositive", "{0}: batch size must be a positive integer.", [nodeName]) };
    }
    if (!isPositiveNumber(config.flushIntervalMillis ?? 5000)
            || !isIntegerNumber(config.flushIntervalMillis ?? 5000)) {
        return { ok: false, message: t("studio.validation.hdfsFileSink.flushIntervalPositive", "{0}: flush interval must be a positive integer.", [nodeName]) };
    }
    return { ok: true };
}

function validateAggregateConfigs(definition) {
    const aggregateNodes = (definition.nodes || []).filter(node => node.operator === "AGGREGATE");
    for (const node of aggregateNodes) {
        const validation = validateAggregateConfig(node);
        if (!validation.ok) {
            return validation;
        }
    }
    return { ok: true };
}

function validateDeduplicateConfigs(definition) {
    const deduplicateNodes = (definition.nodes || []).filter(node => node.operator === "DEDUPLICATE");
    for (const node of deduplicateNodes) {
        const validation = validateDeduplicateConfig(node);
        if (!validation.ok) {
            return validation;
        }
    }
    return { ok: true };
}

function validateRenameConfig(node) {
    const config = node.config || {};
    const mapping = config.mapping;
    const nodeName = nodeDisplayTitle(node);
    if (!mapping || typeof mapping !== "object" || Array.isArray(mapping) || Object.keys(mapping).length === 0) {
        return { ok: false, message: t("studio.validation.rename.mappingRequired", "{0}: add at least one field mapping.", [nodeName]) };
    }

    for (const [sourceField, targetField] of Object.entries(mapping)) {
        if (!String(sourceField || "").trim()) {
            return { ok: false, message: t("studio.validation.rename.sourceFieldRequired", "{0}: every mapping needs an input field.", [nodeName]) };
        }
        if (!String(targetField || "").trim()) {
            return { ok: false, message: t("studio.validation.rename.targetFieldRequired", "{0}: every mapping needs an output field.", [nodeName]) };
        }
    }
    return { ok: true };
}

function validateRenameConfigs(definition) {
    const validators = {
        RENAME: validateRenameConfig
    };
    for (const node of definition.nodes || []) {
        const validator = validators[node.operator];
        if (!validator) {
            continue;
        }
        const validation = validator(node);
        if (!validation.ok) {
            return validation;
        }
    }
    return { ok: true };
}

function validateDataQualityConfig(node) {
    const config = node.config || {};
    const nodeName = nodeDisplayTitle(node);
    if (!Array.isArray(config.rules) || config.rules.length === 0) {
        return { ok: false, message: t("studio.validation.dataQuality.rulesRequired", "{0}: add at least one validation rule.", [nodeName]) };
    }

    for (const rule of config.rules) {
        const field = String(rule?.field || "").trim();
        if (!field) {
            return { ok: false, message: t("studio.validation.dataQuality.ruleFieldRequired", "{0}: each rule needs a field name.", [nodeName]) };
        }
        const ruleType = String(rule?.ruleType || "").trim();
        if (!ruleType) {
            return { ok: false, message: t("studio.validation.dataQuality.ruleTypeRequired", "{0}: each rule needs a rule type.", [nodeName]) };
        }

        if (ruleType === "TYPE" && !String(rule?.valueType || "").trim()) {
            return { ok: false, message: t("studio.validation.dataQuality.valueTypeRequired", "{0}: type rules need a value type.", [nodeName]) };
        }
        if (ruleType === "RANGE" && rule?.min === undefined && rule?.max === undefined) {
            return { ok: false, message: t("studio.validation.dataQuality.rangeRequired", "{0}: range rules need min or max.", [nodeName]) };
        }
        if (ruleType === "LENGTH" && rule?.minLength === undefined && rule?.maxLength === undefined) {
            return { ok: false, message: t("studio.validation.dataQuality.lengthRequired", "{0}: length rules need min length or max length.", [nodeName]) };
        }
        if (ruleType === "ENUM" && (!Array.isArray(rule?.enumValues) || rule.enumValues.length === 0)) {
            return { ok: false, message: t("studio.validation.dataQuality.enumRequired", "{0}: enum rules need allowed values.", [nodeName]) };
        }
        if (ruleType === "REGEX" && !String(rule?.pattern || "").trim()) {
            return { ok: false, message: t("studio.validation.dataQuality.patternRequired", "{0}: regex rules need a pattern.", [nodeName]) };
        }
        if (ruleType === "REGEX" && rule?.pattern) {
            try {
                new RegExp(rule.pattern);
            } catch (error) {
                return { ok: false, message: t("studio.validation.dataQuality.invalidPattern", "{0}: rule pattern is invalid.", [nodeName]) };
            }
        }
    }
    return { ok: true };
}

function validateLookupEnrichConfigs(definition) {
    const lookupEnrichNodes = (definition.nodes || []).filter(node => node.operator === "LOOKUP_ENRICH");
    for (const node of lookupEnrichNodes) {
        const validation = validateLookupEnrichConfig(node);
        if (!validation.ok) {
            return validation;
        }
    }
    return { ok: true };
}

function validateLookupJoinConfigs(definition) {
    const lookupJoinNodes = (definition.nodes || []).filter(node => node.operator === "LOOKUP_JOIN");
    for (const node of lookupJoinNodes) {
        const validation = validateLookupJoinConfig(node);
        if (!validation.ok) {
            return validation;
        }
    }
    return { ok: true };
}

function validateStreamJoinConfigs(definition) {
    const streamJoinNodes = (definition.nodes || []).filter(node => node.operator === "STREAM_JOIN");
    for (const node of streamJoinNodes) {
        const validation = validateStreamJoinConfig(node, definition);
        if (!validation.ok) {
            return validation;
        }
    }
    return { ok: true };
}

function validateFlattenConfigs(definition) {
    const flattenNodes = (definition.nodes || []).filter(node => node.operator === "FLATTEN");
    for (const node of flattenNodes) {
        const validation = validateFlattenConfig(node);
        if (!validation.ok) {
            return validation;
        }
    }
    return { ok: true };
}

function validateExplodeConfigs(definition) {
    const explodeNodes = (definition.nodes || []).filter(node => node.operator === "EXPLODE");
    for (const node of explodeNodes) {
        const validation = validateExplodeConfig(node);
        if (!validation.ok) {
            return validation;
        }
    }
    return { ok: true };
}

function validateDataQualityConfigs(definition) {
    const dataQualityNodes = (definition.nodes || []).filter(node => node.operator === "DATA_QUALITY");
    for (const node of dataQualityNodes) {
        const validation = validateDataQualityConfig(node);
        if (!validation.ok) {
            return validation;
        }
    }
    return { ok: true };
}

function validateSecondBatchTransformConfigs(definition) {
    const validators = {
        TIME_DERIVE: validateTimeDeriveConfig,
        MASK_HASH: validateMaskHashConfig,
        CASE_WHEN: validateCaseWhenConfig,
        ROUTE: validateRouteConfig
    };
    for (const node of definition.nodes || []) {
        const validator = validators[node.operator];
        if (!validator) {
            continue;
        }
        const validation = validator(node);
        if (!validation.ok) {
            return validation;
        }
    }
    return { ok: true };
}

function validateHdfsFileSourceConfigs(definition) {
    const nodes = (definition.nodes || []).filter(node => node.operator === "HDFS_FILE_SOURCE");
    for (const node of nodes) {
        const validation = validateHdfsFileSourceConfig(node);
        if (!validation.ok) {
            return validation;
        }
    }
    return { ok: true };
}

function validateHdfsFileSinkConfigs(definition) {
    const nodes = (definition.nodes || []).filter(node => node.operator === "HDFS_FILE_SINK");
    for (const node of nodes) {
        const validation = validateHdfsFileSinkConfig(node);
        if (!validation.ok) {
            return validation;
        }
    }
    return { ok: true };
}

function validateSaveDefinition(definition) {
    if (!state.pipelineMeta.name) {
        return { ok: false, message: t("studio.validation.nameRequired", "Enter a pipeline name first.") };
    }
    if (!Array.isArray(definition.nodes) || definition.nodes.length === 0) {
        return { ok: false, message: t("studio.validation.nodeRequired", "Add at least one node before saving.") };
    }

    const hdfsFileSourceValidation = validateHdfsFileSourceConfigs(definition);
    if (!hdfsFileSourceValidation.ok) {
        return hdfsFileSourceValidation;
    }

    const hdfsFileSinkValidation = validateHdfsFileSinkConfigs(definition);
    if (!hdfsFileSinkValidation.ok) {
        return hdfsFileSinkValidation;
    }

    const aggregateValidation = validateAggregateConfigs(definition);
    if (!aggregateValidation.ok) {
        return aggregateValidation;
    }

    const deduplicateValidation = validateDeduplicateConfigs(definition);
    if (!deduplicateValidation.ok) {
        return deduplicateValidation;
    }

    const renameValidation = validateRenameConfigs(definition);
    if (!renameValidation.ok) {
        return renameValidation;
    }

    const lookupEnrichValidation = validateLookupEnrichConfigs(definition);
    if (!lookupEnrichValidation.ok) {
        return lookupEnrichValidation;
    }

    const lookupJoinValidation = validateLookupJoinConfigs(definition);
    if (!lookupJoinValidation.ok) {
        return lookupJoinValidation;
    }

    const streamJoinValidation = validateStreamJoinConfigs(definition);
    if (!streamJoinValidation.ok) {
        return streamJoinValidation;
    }

    const flattenValidation = validateFlattenConfigs(definition);
    if (!flattenValidation.ok) {
        return flattenValidation;
    }

    const explodeValidation = validateExplodeConfigs(definition);
    if (!explodeValidation.ok) {
        return explodeValidation;
    }

    const dataQualityValidation = validateDataQualityConfigs(definition);
    if (!dataQualityValidation.ok) {
        return dataQualityValidation;
    }

    const secondBatchValidation = validateSecondBatchTransformConfigs(definition);
    if (!secondBatchValidation.ok) {
        return secondBatchValidation;
    }

    return { ok: true };
}

function validateRunnableGraph(definition) {
    if (!Array.isArray(definition.edges) || definition.edges.length === 0) {
        return { ok: false, message: t("studio.validation.edgeRequired", "Create at least one connection before running.") };
    }

    const nodeById = new Map(definition.nodes.map(node => [node.id, node]));
    const incoming = new Map(definition.nodes.map(node => [node.id, 0]));
    const outgoing = new Map(definition.nodes.map(node => [node.id, 0]));
    const adjacency = new Map(definition.nodes.map(node => [node.id, []]));

    for (const edge of definition.edges) {
        if (!nodeById.has(edge.sourceNodeId) || !nodeById.has(edge.targetNodeId)) {
            return { ok: false, message: t("studio.validation.unknownEdgeNode", "A connection references an unknown node.") };
        }
        if (edge.sourceNodeId === edge.targetNodeId) {
            return { ok: false, message: t("studio.validation.selfEdge", "A node cannot connect to itself.") };
        }

        incoming.set(edge.targetNodeId, (incoming.get(edge.targetNodeId) || 0) + 1);
        outgoing.set(edge.sourceNodeId, (outgoing.get(edge.sourceNodeId) || 0) + 1);
        adjacency.get(edge.sourceNodeId).push(edge.targetNodeId);
    }

    const isolatedNode = definition.nodes.find(node => (incoming.get(node.id) || 0) === 0 && (outgoing.get(node.id) || 0) === 0);
    if (isolatedNode) {
        return { ok: false, message: t("studio.validation.isolatedNode", "Disconnected node exists: {0}", [isolatedNode.name]) };
    }

    const hasSource = definition.nodes.some(node => node.type === "SOURCE");
    const hasSink = definition.nodes.some(node => node.type === "SINK");
    if (!hasSource || !hasSink) {
        return { ok: false, message: t("studio.validation.sourceSinkRequired", "At least one Source and one Sink are required before running.") };
    }

    const invalidSourceInput = definition.nodes.find(node => node.type === "SOURCE" && (incoming.get(node.id) || 0) > 0);
    if (invalidSourceInput) {
        return { ok: false, message: t("studio.validation.sourceHasInput", "Source nodes cannot have upstream input: {0}", [invalidSourceInput.name]) };
    }

    const invalidNodeInput = definition.nodes.find(node => node.type !== "SOURCE" && (incoming.get(node.id) || 0) === 0);
    if (invalidNodeInput) {
        return { ok: false, message: t("studio.validation.nonSourceMissingInput", "Non-Source nodes must have upstream input: {0}", [invalidNodeInput.name]) };
    }

    const invalidSinkOutput = definition.nodes.find(node => node.type === "SINK" && (outgoing.get(node.id) || 0) > 0);
    if (invalidSinkOutput) {
        return { ok: false, message: t("studio.validation.sinkHasOutput", "Sink nodes cannot continue output: {0}", [invalidSinkOutput.name]) };
    }

    const invalidTerminal = definition.nodes.find(node => (outgoing.get(node.id) || 0) === 0 && node.type !== "SINK");
    if (invalidTerminal) {
        return { ok: false, message: t("studio.validation.terminalMustBeSink", "Terminal nodes must be Sinks: {0}", [invalidTerminal.name]) };
    }

    const indegree = new Map(definition.nodes.map(node => [node.id, incoming.get(node.id) || 0]));
    const queue = [...indegree.entries()].filter(([, value]) => value === 0).map(([nodeId]) => nodeId);
    let visited = 0;

    while (queue.length > 0) {
        const current = queue.shift();
        visited += 1;
        for (const next of adjacency.get(current)) {
            const nextIndegree = indegree.get(next) - 1;
            indegree.set(next, nextIndegree);
            if (nextIndegree === 0) {
                queue.push(next);
            }
        }
    }

    if (visited !== definition.nodes.length) {
        return { ok: false, message: t("studio.validation.acyclicDag", "The pipeline topology must be an acyclic DAG.") };
    }

    return { ok: true };
}

function canRunDefinition(definition) {
    const saveValidation = validateSaveDefinition(definition);
    if (!saveValidation.ok) {
        return { ok: false, reason: saveValidation.message };
    }

    if (!state.runtimeTarget) {
        return { ok: false, reason: t("studio.validation.runtimeTargetRequired", "Configure the Flink runtime target before running.") };
    }

    const graphValidation = validateRunnableGraph(definition);
    if (!graphValidation.ok) {
        return { ok: false, reason: graphValidation.message };
    }

    const unsupported = definition.nodes.filter(node => !catalogEntryForOperator(node.operator)?.runnableInRuntime);
    if (unsupported.length > 0) {
        return {
            ok: false,
            reason: t("studio.validation.unsupportedRuntimeOperators", "The current topology contains operators not wired into runtime yet: {0}", [unsupported.map(node => node.name).join(", ")])
        };
    }

    return { ok: true };
}

function canPreviewDefinition(definition) {
    const saveValidation = validateSaveDefinition(definition);
    if (!saveValidation.ok) {
        return { ok: false, reason: saveValidation.message };
    }

    const graphValidation = validateRunnableGraph(definition);
    if (!graphValidation.ok) {
        return { ok: false, reason: graphValidation.message };
    }

    const unsupportedSource = definition.nodes.find(node =>
        node.type === "SOURCE" && false);
    if (unsupportedSource) {
        return { ok: false, reason: t("studio.validation.previewSampleRequired", "Preview requires the input operator to enable sample data: {0}", [nodeDisplayTitle(unsupportedSource)]) };
    }

    return { ok: true };
}

function renderPreviewResults() {
    const panel = byId("pipeline-preview-results-panel");
    const list = byId("pipeline-preview-results-list");
    if (!panel || !list) {
        return;
    }

    const hasContent = state.preview.running
        || state.preview.error
        || state.preview.outputs.length > 0
        || Boolean(state.preview.touched);
    panel.classList.toggle("hidden", !hasContent);

    if (state.preview.running) {
        list.innerHTML = `<div class="rounded-xl border border-blue-200 dark:border-blue-500/30 bg-blue-50 dark:bg-blue-500/10 px-4 py-3 text-xs text-blue-700 dark:text-blue-200">${t("studio.preview.running", "Testing")}</div>`;
        return;
    }

    if (state.preview.error) {
        list.innerHTML = `<div class="rounded-xl border border-red-200 dark:border-red-500/30 bg-red-50 dark:bg-red-500/10 px-4 py-3 text-xs text-red-700 dark:text-red-200">${escapeHtml(state.preview.error)}</div>`;
        return;
    }

    if (state.preview.outputs.length === 0) {
        list.innerHTML = `<div class="rounded-xl border border-slate-200 dark:border-neutral-700 bg-slate-50 dark:bg-neutral-900/60 px-4 py-3 text-xs text-slate-600 dark:text-slate-300">${t("studio.preview.empty", "No output data captured.")}</div>`;
        return;
    }

    list.innerHTML = state.preview.outputs.map(output => `
        <section class="rounded-xl border border-slate-200 dark:border-neutral-700 bg-white dark:bg-neutral-950">
            <header class="px-4 py-3 border-b border-slate-200 dark:border-neutral-700 text-xs font-semibold text-slate-900 dark:text-neutral-100">
                ${escapeHtml(output.nodeName)}
            </header>
            <div class="max-h-72 overflow-y-auto px-4 py-3">
                ${renderPreviewOutputRecords(output.records)}
            </div>
        </section>
    `).join("");
}

function clearPreviewResults() {
    state.preview = { outputs: [], error: "", running: false };
}

async function previewPipeline() {
    const definition = buildDefinition();
    const previewCheck = canPreviewDefinition(definition);
    if (!previewCheck.ok) {
        showMessage("info", previewCheck.reason);
        return;
    }

    clearPreviewResults();
    state.preview = { outputs: [], error: "", running: true, touched: true };
    renderStudio();

    try {
        const response = await fetch("/api/pipelines/preview", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                [STUDIO_BOOTSTRAP.dataset.csrfHeader]: STUDIO_BOOTSTRAP.dataset.csrfToken
            },
            body: JSON.stringify({
                name: state.pipelineMeta.name,
                description: state.pipelineMeta.description,
                definitionJson: JSON.stringify(definition)
            })
        });

        if (!response.ok) {
            throw new Error(await parseError(response, t("studio.preview.error.retry", "Test execution failed. Please try again later.")));
        }

        const payload = await response.json();
        state.preview = {
            outputs: Array.isArray(payload.outputs) ? payload.outputs : [],
            error: "",
            running: false,
            touched: true
        };
        showMessage("success", t("studio.preview.success", "Test execution completed."));
    } catch (error) {
        state.preview = {
            outputs: [],
            error: error.message || t("studio.preview.error", "Test execution failed."),
            running: false,
            touched: true
        };
        showMessage("error", state.preview.error);
    } finally {
        renderStudio();
    }
}

async function parseError(response, fallbackMessage) {
    const payload = await response.json().catch(() => ({}));
    return payload.message || fallbackMessage;
}

function applyPipelineSummary(payload) {
    if (!payload) {
        return;
    }

    if (payload.id !== undefined && payload.id !== null) {
        state.currentPipelineId = String(payload.id);
        if (STUDIO_BOOTSTRAP) {
            STUDIO_BOOTSTRAP.dataset.pipelineId = String(payload.id);
        }
        window.history.replaceState({}, "", `/studio/${payload.id}`);
    }

    if (typeof payload.name === "string") {
        state.pipelineMeta.name = payload.name;
    }
    if (typeof payload.description === "string") {
        state.pipelineMeta.description = payload.description;
    }
    state.lastRunStatus = payload.lastRunStatus || state.lastRunStatus;
    state.lastRunMessage = payload.lastRunMessage || state.lastRunMessage || "";
    fillMetaForm();
}

function renderRuntimeTargetSummary() {
    const summary = byId("runtime-target-summary-copy");
    if (!summary) {
        return;
    }
    if (!state.runtimeTarget) {
        summary.textContent = t("studio.runtime.target.unconfigured", "No runtime target configured.");
        return;
    }
    const typeLabel = "Flink Standalone";
    const status = state.runtimeTarget.status || "UNKNOWN";
    const endpoint = state.runtimeTarget.jobManagerUrl || "";
    summary.textContent = t("studio.runtime.target.summary", "{0} / {1} / {2}", [typeLabel, status, endpoint]);
}

async function loadRuntimeTarget() {
    try {
        const response = await fetch("/api/runtime-target");
        if (!response.ok) {
            throw new Error(t("studio.runtime.target.load.error", "Loading the Flink runtime target failed."));
        }

        const target = await response.json();
        state.runtimeTarget = target.configured ? target : null;
        renderRuntimeTargetSummary();
        updateRuntimeResourceControls();
    } catch (error) {
        renderRuntimeTargetSummary();
        showMessage("info", error.message || t("studio.runtime.target.load.error", "Loading the Flink runtime target failed."));
    }
}

async function loadExistingPipeline() {
    if (!state.currentPipelineId) {
        renderStudio();
        return false;
    }

    try {
        const response = await fetch(`/api/pipelines/${state.currentPipelineId}`);
        if (!response.ok) {
            throw new Error(t("studio.pipeline.load.error", "Loading pipeline details failed."));
        }

        const detail = await response.json();
        state.currentPipelineId = detail.id == null ? state.currentPipelineId : String(detail.id);
        state.lastRunStatus = detail.lastRunStatus || null;
        state.lastRunMessage = detail.lastRunMessage || "";
        state.pipelineMeta.name = detail.name || "";
        state.pipelineMeta.description = detail.description || "";
        fillMetaForm();
        loadDefinitionIntoState(parseJsonValue(detail.definitionJson || "{}", {}));
        return true;
    } catch (error) {
        showMessage("error", error.message || t("studio.pipeline.load.error", "Loading pipeline details failed."));
        renderStudio();
        return false;
    }
}

async function savePipeline() {
    const definition = buildDefinition();
    const validation = validateSaveDefinition(definition);
    if (!validation.ok) {
        showMessage("error", validation.message);
        return null;
    }

    state.requestState.saving = true;
    renderHeader();

    try {
        const response = await fetch("/api/pipelines", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                [STUDIO_BOOTSTRAP.dataset.csrfHeader]: STUDIO_BOOTSTRAP.dataset.csrfToken
            },
            body: JSON.stringify({
                id: state.currentPipelineId ? Number(state.currentPipelineId) : null,
                name: state.pipelineMeta.name,
                description: state.pipelineMeta.description,
                definitionJson: JSON.stringify(definition)
            })
        });

        if (!response.ok) {
            throw new Error(await parseError(response, t("studio.save.error.retry", "Saving the pipeline failed. Please try again later.")));
        }

        const payload = await response.json();
        applyPipelineSummary(payload);
        clearDirty();
        renderStudio();
        showMessage("success", t("studio.save.success", "Pipeline {0} saved.", [payload.name || state.pipelineMeta.name]));
        return payload;
    } catch (error) {
        showMessage("error", error.message || t("studio.save.error.retry", "Saving the pipeline failed. Please try again later."));
        return null;
    } finally {
        state.requestState.saving = false;
        renderHeader();
    }
}

async function runPipeline() {
    const definition = buildDefinition();
    const runCheck = canRunDefinition(definition);
    if (!runCheck.ok) {
        showMessage("info", runCheck.reason);
        return;
    }

    const needsSave = state.hasUnsavedChanges || !state.currentPipelineId;
    const saved = needsSave ? await savePipeline() : true;
    if (!saved || !state.currentPipelineId) {
        return;
    }

    state.requestState.running = true;
    renderHeader();

    try {
        const response = await fetch(`/api/pipelines/${state.currentPipelineId}/run`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                [STUDIO_BOOTSTRAP.dataset.csrfHeader]: STUDIO_BOOTSTRAP.dataset.csrfToken
            },
            body: JSON.stringify({
                ...runResourcePayload()
            })
        });

        if (!response.ok) {
            throw new Error(await parseError(response, t("studio.run.error.retry", "Running the pipeline failed. Please try again later.")));
        }

        const payload = await response.json();
        applyPipelineSummary(payload);
        clearDirty();
        renderStudio();
        showMessage("success", t("studio.run.success", "Pipeline {0} submitted.", [payload.name || state.pipelineMeta.name]));
    } catch (error) {
        showMessage("error", error.message || t("studio.run.error.retry", "Running the pipeline failed. Please try again later."));
    } finally {
        state.requestState.running = false;
        renderHeader();
    }
}

async function stopPipeline() {
    if (!state.currentPipelineId) {
        showMessage("info", t("studio.stop.saveFirst", "Save the pipeline first."));
        return;
    }

    if (String(state.lastRunStatus || "").toUpperCase() !== "RUNNING") {
        showMessage("info", t("studio.stop.notRunning", "The current pipeline is not running."));
        return;
    }

    state.requestState.stopping = true;
    renderHeader();

    try {
        const response = await fetch(`/api/pipelines/${state.currentPipelineId}/stop`, {
            method: "POST",
            headers: {
                [STUDIO_BOOTSTRAP.dataset.csrfHeader]: STUDIO_BOOTSTRAP.dataset.csrfToken
            }
        });

        if (!response.ok) {
            throw new Error(await parseError(response, t("studio.stop.error.retry", "Stopping the pipeline failed. Please try again later.")));
        }

        const payload = await response.json();
        applyPipelineSummary(payload);
        renderStudio();
        showMessage("success", t("studio.stop.success", "Pipeline {0} stopped.", [payload.name || state.pipelineMeta.name]));
    } catch (error) {
        showMessage("error", error.message || t("studio.stop.error.retry", "Stopping the pipeline failed. Please try again later."));
    } finally {
        state.requestState.stopping = false;
        renderHeader();
    }
}

function bindMetaInputs() {
    if (!isEditorMode()) {
        return;
    }

    ["pipeline-name", "pipeline-description"].forEach(id => {
        const element = byId(id);
        if (!element) {
            return;
        }

        const handler = () => {
            syncPipelineMetaFromForm();
            updateRuntimeResourceControls();
            markDirty();
            renderHeader();
        };

        element.addEventListener("input", handler);
        element.addEventListener("change", handler);
    });
}

function bindInspectorInputs() {
    if (!isEditorMode()) {
        return;
    }

    [
        "node-display-name",
        "source-bootstrap-servers",
        "source-topics",
        "source-group-id",
        "source-consume-mode",
        "source-auth-type",
        "source-auth-username",
        "source-auth-password",
        "source-scram-mechanism",
        "source-format",
        "jdbc-url",
        "jdbc-driver",
        "jdbc-username",
        "jdbc-password",
        "jdbc-query",
        "jdbc-table-path",
        "jdbc-read-mode",
        "jdbc-cursor-field",
        "jdbc-cursor-type",
        "jdbc-initial-cursor-value",
        "jdbc-poll-interval-millis",
        "jdbc-fetch-size",
        "jdbc-max-polls",
        "jdbc-id-field",
        "jdbc-timestamp-field",
        "elasticsearch-hosts",
        "elasticsearch-index",
        "elasticsearch-source-fields",
        "elasticsearch-query",
        "elasticsearch-read-mode",
        "elasticsearch-cursor-field",
        "elasticsearch-cursor-type",
        "elasticsearch-initial-cursor-value",
        "elasticsearch-poll-interval-millis",
        "elasticsearch-scroll-size",
        "elasticsearch-scroll-time",
        "elasticsearch-max-polls",
        "elasticsearch-id-field",
        "elasticsearch-timestamp-field",
        "elasticsearch-auth-type",
        "elasticsearch-username",
        "elasticsearch-password",
        "elasticsearch-api-key",
        "influxdb-url",
        "influxdb-database",
        "influxdb-sql",
        "influxdb-schema",
        "influxdb-epoch",
        "influxdb-query-timeout-seconds",
        "influxdb-connect-timeout-millis",
        "influxdb-read-mode",
        "influxdb-cursor-field",
        "influxdb-cursor-type",
        "influxdb-initial-cursor-value",
        "influxdb-poll-interval-millis",
        "influxdb-max-polls",
        "influxdb-id-field",
        "influxdb-timestamp-field",
        "influxdb-username",
        "influxdb-password",
        "hdfs-file-default-fs",
        "hdfs-file-path",
        "hdfs-file-format-type",
        "hdfs-file-schema",
        "hdfs-file-read-columns",
        "hdfs-file-read-mode",
        "hdfs-file-poll-interval-millis",
        "hdfs-file-max-polls",
        "hdfs-file-filter-pattern",
        "hdfs-file-parse-partition-from-path",
        "hdfs-file-field-delimiter",
        "hdfs-file-row-delimiter",
        "hdfs-file-skip-header-row-number",
        "hdfs-file-csv-use-header-line",
        "hdfs-file-encoding",
        "hdfs-file-compress-codec",
        "hdfs-file-id-field",
        "hdfs-file-timestamp-field",
        "hdfs-file-hdfs-site-path",
        "hdfs-file-kerberos-principal",
        "hdfs-file-kerberos-keytab-path",
        "put-field",
        "put-value-mode",
        "put-literal-value",
        "put-reference-field",
        "put-template-value",
        "deserialize-field",
        "deserialize-target-field",
        "deserialize-format",
        "deserialize-field-names",
        "deserialize-delimiter",
        "serialize-source-fields",
        "serialize-target-field",
        "serialize-format",
        "serialize-delimiter",
        "filter-condition",
        "grok-input-field",
        "grok-output-field",
        "grok-pattern",
        "cast-input-field",
        "cast-output-mode",
        "cast-output-field",
        "cast-target-type",
        "eval-target-field",
        "eval-expression",
        "eval-output-mode",
        "eval-error-strategy",
        "deduplicate-key-fields",
        "deduplicate-time-mode",
        "deduplicate-ttl-seconds",
        "deduplicate-event-time-field",
        "deduplicate-window-seconds",
        "deduplicate-watermark-delay-seconds",
        "deduplicate-keep-strategy",
        "deduplicate-late-data-strategy",
        "deduplicate-duplicate-strategy",
        "lookup-enrich-source-field",
        "lookup-enrich-target-field",
        "lookup-enrich-missing-strategy",
        "lookup-enrich-overwrite-target-field",
        "lookup-enrich-entry-list",
        "lookup-join-source-field",
        "lookup-join-target-field",
        "lookup-join-type",
        "lookup-join-missing-strategy",
        "lookup-join-overwrite-target-field",
        "lookup-join-entry-list",
        "stream-join-left-key-field",
        "stream-join-right-key-field",
        "stream-join-target-field",
        "stream-join-type",
        "stream-join-missing-strategy",
        "stream-join-overwrite-target-field",
        "stream-join-time-mode",
        "stream-join-time-unit",
        "stream-join-window-before",
        "stream-join-window-after",
        "stream-join-watermark-delay",
        "stream-join-late-data-strategy",
        "flatten-source-field",
        "flatten-target-prefix",
        "flatten-delimiter",
        "flatten-remove-source-field",
        "explode-source-field",
        "explode-target-field",
        "explode-keep-empty",
        "data-quality-mode",
        "data-quality-error-field",
        "data-quality-rule-list",
        "time-derive-source-field",
        "time-derive-source-format",
        "time-derive-source-pattern",
        "time-derive-source-time-zone",
        "time-derive-output-time-zone",
        "time-derive-parse-error-strategy",
        "case-when-target-field",
        "case-when-default-mode",
        "case-when-default-value",
        "case-when-default-expression",
        "route-match-mode",
        "route-include-unmatched",
        "route-unmatched-port",
        "aggregate-mode",
        "aggregate-group-by",
        "aggregate-window-type",
        "aggregate-time-mode",
        "aggregate-time-unit",
        "aggregate-window-size",
        "aggregate-window-slide",
        "aggregate-watermark-delay",
        "aggregate-event-time-field",
        "aggregate-event-time-unit",
        "aggregate-output-mode",
        "aggregate-window-start-field",
        "aggregate-window-end-field",
        "aggregate-count-window-size",
        "custom-code-class-name",
        "custom-code-source",
        "custom-code-error-strategy",
        "transform-note",
        "sink-bootstrap-servers",
        "sink-topic",
        "sink-delivery-guarantee",
        "sink-auth-type",
        "sink-auth-username",
        "sink-auth-password",
        "sink-scram-mechanism",
        "sink-format",
        "sink-message-field",
        "jdbc-sink-url",
        "jdbc-sink-driver",
        "jdbc-sink-username",
        "jdbc-sink-password",
        "jdbc-sink-table-path",
        "jdbc-sink-write-mode",
        "jdbc-sink-fields",
        "jdbc-sink-key-fields",
        "jdbc-sink-batch-size",
        "jdbc-sink-flush-interval-millis",
        "elasticsearch-sink-hosts",
        "elasticsearch-sink-index",
        "elasticsearch-sink-index-type",
        "elasticsearch-sink-primary-keys",
        "elasticsearch-sink-key-delimiter",
        "elasticsearch-sink-fields",
        "elasticsearch-sink-max-batch-size",
        "elasticsearch-sink-flush-interval-millis",
        "elasticsearch-sink-max-retry-count",
        "elasticsearch-sink-auth-type",
        "elasticsearch-sink-username",
        "elasticsearch-sink-password",
        "elasticsearch-sink-api-key-id",
        "elasticsearch-sink-api-key",
        "elasticsearch-sink-api-key-encoded",
        "influxdb-sink-url",
        "influxdb-sink-database",
        "influxdb-sink-measurement",
        "influxdb-sink-key-time",
        "influxdb-sink-key-tags",
        "influxdb-sink-fields",
        "influxdb-sink-batch-size",
        "influxdb-sink-max-retries",
        "influxdb-sink-retry-backoff-multiplier-millis",
        "influxdb-sink-max-retry-backoff-millis",
        "influxdb-sink-connect-timeout-millis",
        "influxdb-sink-flush-interval-millis",
        "influxdb-sink-precision",
        "influxdb-sink-username",
        "influxdb-sink-password",
        "hdfs-file-sink-default-fs",
        "hdfs-file-sink-path",
        "hdfs-file-sink-tmp-path",
        "hdfs-file-sink-format-type",
        "hdfs-file-sink-columns",
        "hdfs-file-sink-partition-by",
        "hdfs-file-sink-partition-dir-expression",
        "hdfs-file-sink-partition-field-write-in-file",
        "hdfs-file-sink-custom-filename",
        "hdfs-file-sink-file-name-expression",
        "hdfs-file-sink-filename-time-format",
        "hdfs-file-sink-batch-size",
        "hdfs-file-sink-flush-interval-millis",
        "hdfs-file-sink-field-delimiter",
        "hdfs-file-sink-row-delimiter",
        "hdfs-file-sink-csv-use-header-line",
        "hdfs-file-sink-encoding",
        "hdfs-file-sink-compress-codec",
        "hdfs-file-sink-hdfs-site-path",
        "hdfs-file-sink-kerberos-principal",
        "hdfs-file-sink-kerberos-keytab-path"
    ].forEach(id => {
        const element = byId(id);
        if (!element) {
            return;
        }

        const handler = () => {
            syncSelectedNodeConfigFromInspector();
            markDirty();
            renderHeader();
        };

        element.addEventListener("input", handler);
        element.addEventListener("change", handler);
    });

    byId("add-source-sample-button")?.addEventListener("click", () => {
        appendSourceSampleInput("");
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("source-sample-list")?.addEventListener("input", event => {
        if (!event.target.matches("[data-role='source-sample-input']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("source-sample-list")?.addEventListener("click", event => {
        const removeButton = event.target.closest("[data-role='remove-source-sample']");
        if (!removeButton) {
            return;
        }
        const item = removeButton.closest("[data-role='source-sample-item']");
        if (!item) {
            return;
        }
        const items = Array.from(byId("source-sample-list")?.children || []);
        const index = items.indexOf(item);
        if (index < 0) {
            return;
        }
        removeSourceSampleInput(index);
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("add-rename-mapping-button")?.addEventListener("click", () => {
        appendRenameMappingInput("", "");
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("add-prune-field-button")?.addEventListener("click", () => {
        appendPruneFieldInput("");
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("prune-field-list")?.addEventListener("input", event => {
        if (!event.target.matches("[data-role='prune-field-name']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("prune-field-list")?.addEventListener("click", event => {
        const removeButton = event.target.closest("[data-role='remove-prune-field']");
        if (!removeButton) {
            return;
        }
        const item = removeButton.closest("[data-role='prune-field-item']");
        if (!item) {
            return;
        }
        const items = Array.from(byId("prune-field-list")?.children || []);
        const index = items.indexOf(item);
        if (index < 0) {
            return;
        }
        removePruneFieldInput(index);
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("rename-mapping-list")?.addEventListener("input", event => {
        if (!event.target.matches("[data-role='rename-source-field']") && !event.target.matches("[data-role='rename-target-field']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("rename-mapping-list")?.addEventListener("click", event => {
        const removeButton = event.target.closest("[data-role='remove-rename-mapping']");
        if (!removeButton) {
            return;
        }
        const item = removeButton.closest("[data-role='rename-mapping-item']");
        if (!item) {
            return;
        }
        const items = Array.from(byId("rename-mapping-list")?.children || []);
        const index = items.indexOf(item);
        if (index < 0) {
            return;
        }
        removeRenameMappingInput(index);
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("add-lookup-enrich-entry-button")?.addEventListener("click", () => {
        appendLookupEnrichEntry("", "");
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("lookup-enrich-entry-list")?.addEventListener("input", event => {
        if (!event.target.matches("[data-role='lookup-enrich-entry-key']") && !event.target.matches("[data-role='lookup-enrich-entry-value']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("lookup-enrich-entry-list")?.addEventListener("click", event => {
        const removeButton = event.target.closest("[data-role='remove-lookup-enrich-entry']");
        if (!removeButton) {
            return;
        }
        const item = removeButton.closest("[data-role='lookup-enrich-entry-item']");
        if (!item) {
            return;
        }
        const items = Array.from(byId("lookup-enrich-entry-list")?.children || []);
        const index = items.indexOf(item);
        if (index < 0) {
            return;
        }
        removeLookupEnrichEntry(index);
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("lookup-enrich-entry-list")?.addEventListener("change", event => {
        if (!event.target.matches("[data-role='lookup-enrich-entry-value-type']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("add-lookup-join-entry-button")?.addEventListener("click", () => {
        appendLookupJoinEntry();
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("lookup-join-entry-list")?.addEventListener("input", event => {
        if (!event.target.matches("[data-role='lookup-join-entry-key']")
                && !event.target.matches("[data-role='lookup-join-field-name']")
                && !event.target.matches("[data-role='lookup-join-field-value']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("lookup-join-entry-list")?.addEventListener("change", event => {
        if (!event.target.matches("[data-role='lookup-join-field-type']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("lookup-join-entry-list")?.addEventListener("click", event => {
        const addFieldButton = event.target.closest("[data-role='add-lookup-join-field']");
        const removeFieldButton = event.target.closest("[data-role='remove-lookup-join-field']");
        const removeEntryButton = event.target.closest("[data-role='remove-lookup-join-entry']");
        if (!addFieldButton && !removeFieldButton && !removeEntryButton) {
            return;
        }

        const entryItems = Array.from(byId("lookup-join-entry-list")?.children || []);
        if (addFieldButton) {
            const entryItem = addFieldButton.closest("[data-role='lookup-join-entry-item']");
            const entryIndex = entryItems.indexOf(entryItem);
            if (entryIndex >= 0) {
                appendLookupJoinField(entryIndex);
            }
        } else if (removeFieldButton) {
            const entryItem = removeFieldButton.closest("[data-role='lookup-join-entry-item']");
            const fieldItems = Array.from(entryItem?.querySelectorAll("[data-role='lookup-join-field-item']") || []);
            const entryIndex = entryItems.indexOf(entryItem);
            const fieldIndex = fieldItems.indexOf(removeFieldButton.closest("[data-role='lookup-join-field-item']"));
            if (entryIndex >= 0 && fieldIndex >= 0) {
                removeLookupJoinField(entryIndex, fieldIndex);
            }
        } else if (removeEntryButton) {
            const entryIndex = entryItems.indexOf(removeEntryButton.closest("[data-role='lookup-join-entry-item']"));
            if (entryIndex >= 0) {
                removeLookupJoinEntry(entryIndex);
            }
        }

        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("lookup-join-type")?.addEventListener("change", event => {
        updateLookupJoinTypeUI(event.target.value || "LEFT");
    });

    byId("add-data-quality-rule-button")?.addEventListener("click", () => {
        appendDataQualityRule();
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("data-quality-rule-list")?.addEventListener("input", event => {
        if (!event.target.matches("[data-role='data-quality-rule-field']")
                && !event.target.matches("[data-role='data-quality-rule-min']")
                && !event.target.matches("[data-role='data-quality-rule-max']")
                && !event.target.matches("[data-role='data-quality-rule-min-length']")
                && !event.target.matches("[data-role='data-quality-rule-max-length']")
                && !event.target.matches("[data-role='data-quality-rule-enum-values']")
                && !event.target.matches("[data-role='data-quality-rule-pattern']")
                && !event.target.matches("[data-role='data-quality-rule-message']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("data-quality-rule-list")?.addEventListener("change", event => {
        if (!event.target.matches("[data-role='data-quality-rule-kind']")
                && !event.target.matches("[data-role='data-quality-rule-value-type']")) {
            return;
        }
        if (event.target.matches("[data-role='data-quality-rule-kind']")) {
            updateDataQualityRuleUI(event.target.closest("[data-role='data-quality-rule-item']"));
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("data-quality-rule-list")?.addEventListener("click", event => {
        const removeButton = event.target.closest("[data-role='remove-data-quality-rule']");
        if (!removeButton) {
            return;
        }
        const item = removeButton.closest("[data-role='data-quality-rule-item']");
        if (!item) {
            return;
        }
        const items = Array.from(byId("data-quality-rule-list")?.children || []);
        const index = items.indexOf(item);
        if (index < 0) {
            return;
        }
        removeDataQualityRule(index);
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("data-quality-mode")?.addEventListener("change", () => {
        updateDataQualityModeUI(byId("data-quality-mode")?.value || "DIRTY_PORT");
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("time-derive-source-format")?.addEventListener("change", () => {
        updateTimeDeriveSourceFormatUI(byId("time-derive-source-format")?.value || "AUTO");
    });

    byId("add-time-derive-item-button")?.addEventListener("click", () => {
        appendTimeDeriveItem();
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("time-derive-derivation-list")?.addEventListener("input", event => {
        if (!event.target.matches("[data-role='time-derive-output-field']")
                && !event.target.matches("[data-role='time-derive-pattern']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("time-derive-derivation-list")?.addEventListener("change", event => {
        if (!event.target.matches("[data-role='time-derive-type']")) {
            return;
        }
        const item = event.target.closest("[data-role='time-derive-derivation-item']");
        if (item) {
            updateTimeDeriveDerivationUI(item);
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("time-derive-derivation-list")?.addEventListener("click", event => {
        const item = event.target.closest("[data-role='time-derive-derivation-item']");
        if (!item) {
            return;
        }
        const items = Array.from(byId("time-derive-derivation-list")?.children || []);
        const index = items.indexOf(item);
        if (index < 0) {
            return;
        }
        if (event.target.closest("[data-role='move-time-derive-up']")) {
            moveTimeDeriveItem(index, -1);
        } else if (event.target.closest("[data-role='move-time-derive-down']")) {
            moveTimeDeriveItem(index, 1);
        } else if (event.target.closest("[data-role='remove-time-derive']")) {
            removeTimeDeriveItem(index);
        } else {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("route-include-unmatched")?.addEventListener("change", () => {
        updateRouteConfigUI(Boolean(byId("route-include-unmatched")?.checked));
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
        renderNodes();
        scheduleEdgeRefresh();
    });

    byId("add-route-item-button")?.addEventListener("click", () => {
        appendRouteItem();
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
        renderNodes();
        scheduleEdgeRefresh();
    });

    byId("route-rule-list")?.addEventListener("input", event => {
        if (!event.target.matches("[data-role='route-port-id']")
                && !event.target.matches("[data-role='route-condition']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
        renderNodes();
        scheduleEdgeRefresh();
    });

    byId("route-rule-list")?.addEventListener("click", event => {
        const item = event.target.closest("[data-role='route-rule-item']");
        if (!item) {
            return;
        }
        const items = Array.from(byId("route-rule-list")?.children || []);
        const index = items.indexOf(item);
        if (index < 0) {
            return;
        }
        if (event.target.closest("[data-role='move-route-up']")) {
            moveRouteItem(index, -1);
        } else if (event.target.closest("[data-role='move-route-down']")) {
            moveRouteItem(index, 1);
        } else if (event.target.closest("[data-role='remove-route']")) {
            removeRouteItem(index);
        } else {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
        renderNodes();
        scheduleEdgeRefresh();
    });

    byId("add-mask-hash-rule-button")?.addEventListener("click", () => {
        appendMaskHashRule();
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("mask-hash-rule-list")?.addEventListener("input", event => {
        if (!event.target.matches("[data-role='mask-hash-source-field']")
                && !event.target.matches("[data-role='mask-hash-target-field']")
                && !event.target.matches("[data-role='mask-hash-mask-char']")
                && !event.target.matches("[data-role='mask-hash-keep-first']")
                && !event.target.matches("[data-role='mask-hash-keep-last']")
                && !event.target.matches("[data-role='mask-hash-salt']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("mask-hash-rule-list")?.addEventListener("change", event => {
        if (!event.target.matches("[data-role='mask-hash-action']")
                && !event.target.matches("[data-role='mask-hash-algorithm']")) {
            return;
        }
        const item = event.target.closest("[data-role='mask-hash-rule-item']");
        if (item) {
            updateMaskHashActionUI(item);
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("mask-hash-rule-list")?.addEventListener("click", event => {
        const item = event.target.closest("[data-role='mask-hash-rule-item']");
        if (!item) {
            return;
        }
        const items = Array.from(byId("mask-hash-rule-list")?.children || []);
        const index = items.indexOf(item);
        if (index < 0) {
            return;
        }
        if (event.target.closest("[data-role='move-mask-hash-up']")) {
            moveMaskHashRule(index, -1);
        } else if (event.target.closest("[data-role='move-mask-hash-down']")) {
            moveMaskHashRule(index, 1);
        } else if (event.target.closest("[data-role='remove-mask-hash']")) {
            removeMaskHashRule(index);
        } else {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("add-case-when-item-button")?.addEventListener("click", () => {
        appendCaseWhenItem();
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("case-when-rule-list")?.addEventListener("input", event => {
        if (!event.target.matches("[data-role='case-when-condition']")
                && !event.target.matches("[data-role='case-when-value']")
                && !event.target.matches("[data-role='case-when-expression']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("case-when-rule-list")?.addEventListener("change", event => {
        if (!event.target.matches("[data-role='case-when-value-mode']")) {
            return;
        }
        const item = event.target.closest("[data-role='case-when-rule-item']");
        if (item) {
            updateCaseWhenValueModeUI(item);
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("case-when-rule-list")?.addEventListener("click", event => {
        const item = event.target.closest("[data-role='case-when-rule-item']");
        if (!item) {
            return;
        }
        const items = Array.from(byId("case-when-rule-list")?.children || []);
        const index = items.indexOf(item);
        if (index < 0) {
            return;
        }
        if (event.target.closest("[data-role='move-case-when-up']")) {
            moveCaseWhenItem(index, -1);
        } else if (event.target.closest("[data-role='move-case-when-down']")) {
            moveCaseWhenItem(index, 1);
        } else if (event.target.closest("[data-role='remove-case-when']")) {
            removeCaseWhenItem(index);
        } else {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("case-when-default-mode")?.addEventListener("change", () => {
        updateCaseWhenDefaultModeUI(byId("case-when-default-mode")?.value || "NONE");
    });

    byId("put-value-mode")?.addEventListener("change", () => {
        updatePutValueModeUI(byId("put-value-mode")?.value || "LITERAL");
    });

    byId("cast-output-mode")?.addEventListener("change", () => {
        updateCastOutputModeUI(byId("cast-output-mode")?.value || "OVERWRITE");
    });

    byId("deduplicate-time-mode")?.addEventListener("change", () => {
        updateDeduplicateTimeModeUI(byId("deduplicate-time-mode")?.value || "PROCESSING_TIME");
    });

    ["aggregate-mode", "aggregate-window-type", "aggregate-time-mode"].forEach(id => {
        byId(id)?.addEventListener("change", () => {
            updateAggregateConfigUI(
                byId("aggregate-mode")?.value || "GLOBAL",
                byId("aggregate-window-type")?.value || "TUMBLING_TIME",
                byId("aggregate-time-mode")?.value || "PROCESSING_TIME"
            );
        });
    });

    byId("aggregate-output-mode")?.addEventListener("change", () => {
        updateAggregateOutputModeUI(byId("aggregate-output-mode")?.value || "NESTED");
    });

    ["stream-join-type", "stream-join-time-mode"].forEach(id => {
        byId(id)?.addEventListener("change", () => {
            updateStreamJoinConfigUI(
                byId("stream-join-type")?.value || "LEFT",
                byId("stream-join-time-mode")?.value || "PROCESSING_TIME"
            );
        });
    });

    byId("add-aggregate-item-button")?.addEventListener("click", () => {
        appendAggregateItem();
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("aggregate-item-list")?.addEventListener("input", event => {
        if (!event.target.matches("[data-role='aggregate-field']")
                && !event.target.matches("[data-role='aggregate-sort-field']")
                && !event.target.matches("[data-role='aggregate-output-field']")
                && !event.target.matches("[data-role='aggregate-limit']")) {
            return;
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("aggregate-item-list")?.addEventListener("change", event => {
        if (!event.target.matches("[data-role='aggregate-function']")
                && !event.target.matches("[data-role='aggregate-sort-direction']")) {
            return;
        }
        const item = event.target.closest("[data-role='aggregate-item']");
        if (item) {
            updateAggregateItemVisibility(item);
        }
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("aggregate-item-list")?.addEventListener("click", event => {
        const removeButton = event.target.closest("[data-role='remove-aggregate-item']");
        if (!removeButton) {
            return;
        }
        const item = removeButton.closest("[data-role='aggregate-item']");
        if (!item) {
            return;
        }
        const items = Array.from(byId("aggregate-item-list")?.children || []);
        const index = items.indexOf(item);
        if (index < 0) {
            return;
        }
        removeAggregateItem(index);
        syncSelectedNodeConfigFromInspector();
        markDirty();
        renderHeader();
    });

    byId("sink-format")?.addEventListener("change", () => {
        updateSinkFormatUI(byId("sink-format")?.value || "JSON");
    });

    byId("deserialize-format")?.addEventListener("change", () => {
        updateDeserializeFormatUI(byId("deserialize-format")?.value || "JSON");
    });

    byId("serialize-format")?.addEventListener("change", () => {
        updateSerializeFormatUI(byId("serialize-format")?.value || "JSON");
    });

    byId("source-auth-type")?.addEventListener("change", () => {
        updateSourceAuthUI(byId("source-auth-type")?.value || "NONE");
    });

    byId("jdbc-read-mode")?.addEventListener("change", () => {
        updateJdbcReadModeUI(byId("jdbc-read-mode")?.value || "FULL");
    });

    byId("elasticsearch-read-mode")?.addEventListener("change", () => {
        updateElasticsearchReadModeUI(byId("elasticsearch-read-mode")?.value || "FULL");
    });

    byId("influxdb-read-mode")?.addEventListener("change", () => {
        updateInfluxDbReadModeUI(byId("influxdb-read-mode")?.value || "FULL");
    });

    byId("hdfs-file-read-mode")?.addEventListener("change", () => {
        updateHdfsFileReadModeUI(byId("hdfs-file-read-mode")?.value || "FULL");
    });

    byId("elasticsearch-auth-type")?.addEventListener("change", () => {
        updateElasticsearchAuthUI(byId("elasticsearch-auth-type")?.value || "NONE");
    });

    byId("sink-auth-type")?.addEventListener("change", () => {
        updateSinkAuthUI(byId("sink-auth-type")?.value || "NONE");
    });

    byId("jdbc-sink-write-mode")?.addEventListener("change", () => {
        updateJdbcSinkWriteModeUI(byId("jdbc-sink-write-mode")?.value || "INSERT");
    });

    byId("elasticsearch-sink-auth-type")?.addEventListener("change", () => {
        updateElasticsearchSinkAuthUI(byId("elasticsearch-sink-auth-type")?.value || "NONE");
    });

    byId("hdfs-file-sink-custom-filename")?.addEventListener("change", () => {
        updateHdfsFileSinkFilenameUI(Boolean(byId("hdfs-file-sink-custom-filename")?.checked));
    });
}

function updateStreamJoinConfigUI(joinType, timeMode) {
    byId("stream-join-missing-strategy-wrapper")?.classList.toggle("hidden", joinType === "INNER");
    byId("stream-join-watermark-delay-wrapper")?.classList.toggle("hidden", timeMode !== "EVENT_TIME");
}

function bindPaletteInteractions() {
    if (!isEditorMode()) {
        return;
    }

    document.querySelectorAll(".operator-palette-item").forEach(button => {
        button.addEventListener("dragstart", handlePaletteDragStart);
    });

    const dropZone = byId("canvas-drop-zone");
    if (dropZone) {
        dropZone.addEventListener("dragover", event => event.preventDefault());
        dropZone.addEventListener("drop", handleCanvasDrop);
        dropZone.addEventListener("click", event => {
            if (!(event.target instanceof Element)) {
                closeNodeInspector();
                return;
            }
            if (event.target.closest(".draggable-node")) {
                return;
            }
            closeNodeInspector();
        });
    }

    document.addEventListener("pointerdown", event => {
        if (!state.contextMenu.visible) {
            return;
        }
        if (event.target.closest("#node-context-menu")) {
            return;
        }
        hideNodeContextMenu();
        renderNodeContextMenu();
    });

    document.addEventListener("keydown", event => {
        const activeTag = document.activeElement?.tagName;
        if (["INPUT", "TEXTAREA", "SELECT", "BUTTON"].includes(activeTag) || event.target?.closest?.(".studio-select-shell, .studio-segmented-control")) {
            return;
        }
        if (event.key === "Escape") {
            hideNodeContextMenu();
            renderNodeContextMenu();
            return;
        }
        if (event.key === "Delete") {
            deleteSelectedSelection();
        }
    });
}

function bindToolbarActions() {
    if (!isEditorMode()) {
        return;
    }

    byId("save-pipeline-button")?.addEventListener("click", savePipeline);
    byId("run-pipeline-button")?.addEventListener("click", runPipeline);
    byId("preview-pipeline-button")?.addEventListener("click", previewPipeline);
    byId("stop-pipeline-button")?.addEventListener("click", stopPipeline);
    byId("close-node-inspector-button")?.addEventListener("click", closeNodeInspector);
}

function bindMonitorActions() {
    byId("refresh-button")?.addEventListener("click", refreshMonitorMetrics);
    window.addEventListener("beforeunload", stopMonitorRefresh);
}

function updateMonitorLastRefresh(timestamp) {
    const element = byId("monitor-last-refresh");
    if (!element) {
        return;
    }
    const displayValue = timestamp ? new Date(timestamp).toLocaleString() : "--";
    element.textContent = t("monitorDetail.lastRefresh.value", "Last refresh: {0}", [displayValue]);
}

function applyMonitorMetrics(metrics) {
    const sampledAt = Date.now();
    const previousTotals = state.previousMonitorTotalsByNodeId;
    const nextTotals = new Map();
    const nextMetrics = new Map();
    const intervalSeconds = state.monitorLastSampleAt ? (sampledAt - state.monitorLastSampleAt) / 1000 : 0;

    (metrics?.nodeMetrics || []).forEach(entry => {
        const nodeId = String(entry?.nodeId || "");
        if (!nodeId) {
            return;
        }

        const inputRecords = Number(entry?.inputRecords);
        const outputRecords = Number(entry?.outputRecords);
        const previous = previousTotals.get(nodeId);
        const inputRate = calculateRate(inputRecords, previous?.inputRecords, intervalSeconds);
        const outputRate = calculateRate(outputRecords, previous?.outputRecords, intervalSeconds);
        const statusText = metrics?.status
            ? t("studio.monitor.runStatus", "Run status: {0}", [metrics.status])
            : t("studio.monitor.metrics.updated", "Metrics updated");

        nextTotals.set(nodeId, { inputRecords, outputRecords });
        nextMetrics.set(nodeId, {
            inputRecords,
            outputRecords,
            inputRate,
            outputRate,
            statusTone: "info",
            statusText
        });
    });

    state.monitorMetricsByNodeId = nextMetrics;
    state.previousMonitorTotalsByNodeId = nextTotals;
    state.lastRunStatus = metrics?.status || state.lastRunStatus;
    state.lastMonitorRefreshAt = sampledAt;
    state.monitorLastSampleAt = sampledAt;
    updateMonitorLastRefresh(state.lastMonitorRefreshAt);
    renderStudio();
}

async function refreshMonitorMetrics() {
    if (!state.currentPipelineId) {
        return;
    }

    if (state.monitorRefreshInFlight) {
        return;
    }

    state.monitorRefreshInFlight = true;
    const requestId = state.monitorRefreshRequestId + 1;
    state.monitorRefreshRequestId = requestId;
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => {
        controller.abort();
    }, MONITOR_REFRESH_TIMEOUT_MS);

    try {
        const response = await fetch(`/api/pipelines/${state.currentPipelineId}/metrics`, {
            signal: controller.signal
        });
        if (!response.ok) {
            throw new Error(t("studio.monitor.metrics.error", "Metrics fetch failed"));
        }

        const metrics = await response.json();
        if (requestId !== state.monitorRefreshRequestId) {
            return;
        }
        applyMonitorMetrics(metrics);
    } catch (error) {
        if (requestId !== state.monitorRefreshRequestId) {
            return;
        }

        const failedMetricsByNodeId = new Map();
        state.nodes.forEach(node => {
            const previous = state.monitorMetricsByNodeId.get(node.id) || {};
            failedMetricsByNodeId.set(node.id, {
                ...previous,
                statusTone: "error",
                statusText: t("studio.monitor.metrics.error", "Metrics fetch failed")
            });
        });

        state.monitorMetricsByNodeId = failedMetricsByNodeId;
        state.previousMonitorTotalsByNodeId = new Map();
        state.monitorLastSampleAt = null;
        updateMonitorLastRefresh(state.lastMonitorRefreshAt);
        renderStudio();
        showMessage("error", t("studio.monitor.metrics.errorPreserved", "Metrics fetch failed. The current DAG view is preserved."));
    } finally {
        window.clearTimeout(timeoutId);
        if (requestId === state.monitorRefreshRequestId) {
            state.monitorRefreshInFlight = false;
        }
    }
}

function startMonitorRefresh() {
    stopMonitorRefresh();
    state.monitorRefreshTimer = window.setInterval(refreshMonitorMetrics, MONITOR_REFRESH_INTERVAL_MS);
}

function stopMonitorRefresh() {
    if (!state.monitorRefreshTimer) {
        return;
    }

    window.clearInterval(state.monitorRefreshTimer);
    state.monitorRefreshTimer = null;
}

function bootstrapStudioEditor() {
    window.StudioSelectEnhancer?.init(document.querySelector("[data-studio-select-root]") || document);
    bindSegmentedControls();
    document.fonts?.ready?.then(() => {
        scheduleEdgeRefresh();
    });
    window.addEventListener("resize", scheduleEdgeRefresh);

    if (isEditorMode()) {
        bindPaletteInteractions();
        bindMetaInputs();
        bindInspectorInputs();
        bindToolbarActions();
        loadRuntimeTarget().then(loadExistingPipeline);
        return;
    }

    if (isMonitorMode()) {
        bindMonitorActions();
        loadExistingPipeline().then(loaded => {
            if (!loaded) {
                return;
            }
            refreshMonitorMetrics();
            startMonitorRefresh();
        });
    }
}

bootstrapStudioEditor();
