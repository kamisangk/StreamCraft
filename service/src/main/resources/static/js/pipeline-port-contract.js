window.StreamCraftPipelinePortContract = (() => {
    const RECORDS_PORT = "records";
    const MATCHED_PORT = "matched";
    const REJECTED_PORT = "rejected";
    const CLEAN_PORT = "clean";
    const DIRTY_PORT = "dirty";
    const LEFT_PORT = "left";
    const RIGHT_PORT = "right";

    const sourceOperators = new Set([
        "KAFKA_SOURCE",
        "JDBC_SOURCE",
        "ELASTICSEARCH_SOURCE",
        "INFLUXDB_SOURCE",
        "HDFS_FILE_SOURCE"
    ]);
    const sinkOperators = new Set([
        "KAFKA_SINK",
        "JDBC_SINK",
        "ELASTICSEARCH_SINK",
        "INFLUXDB_SINK",
        "HDFS_FILE_SINK"
    ]);
    const singleRecordTransformOperators = new Set([
        "PUT",
        "PRUNE",
        "RENAME",
        "DESERIALIZE",
        "SERIALIZE",
        "GROK",
        "CAST",
        "EVAL",
        "CUSTOM_CODE",
        "AGGREGATE",
        "DEDUPLICATE",
        "LOOKUP_ENRICH",
        "LOOKUP_JOIN",
        "FLATTEN",
        "EXPLODE",
        "TIME_DERIVE",
        "MASK_HASH",
        "CASE_WHEN"
    ]);

    function contractForOperator(operator) {
        if (sourceOperators.has(operator)) {
            return { inputPorts: [], outputPorts: [RECORDS_PORT], outputPortsDynamic: false };
        }
        if (sinkOperators.has(operator)) {
            return { inputPorts: [RECORDS_PORT], outputPorts: [], outputPortsDynamic: false };
        }
        if (singleRecordTransformOperators.has(operator)) {
            return { inputPorts: [RECORDS_PORT], outputPorts: [RECORDS_PORT], outputPortsDynamic: false };
        }
        if (operator === "FILTER") {
            return { inputPorts: [RECORDS_PORT], outputPorts: [MATCHED_PORT, REJECTED_PORT], outputPortsDynamic: false };
        }
        if (operator === "DATA_QUALITY") {
            return { inputPorts: [RECORDS_PORT], outputPorts: [CLEAN_PORT, DIRTY_PORT], outputPortsDynamic: false };
        }
        if (operator === "STREAM_JOIN") {
            return { inputPorts: [LEFT_PORT, RIGHT_PORT], outputPorts: [RECORDS_PORT], outputPortsDynamic: false };
        }
        if (operator === "ROUTE") {
            return { inputPorts: [RECORDS_PORT], outputPorts: [], outputPortsDynamic: true };
        }
        return { inputPorts: [], outputPorts: [], outputPortsDynamic: false };
    }

    function outputPortsForNode(node) {
        const contract = contractForOperator(node?.operator);
        if (!contract.outputPortsDynamic) {
            return [...contract.outputPorts];
        }

        const routePorts = Array.isArray(node?.config?.routes)
            ? node.config.routes.map(route => String(route?.portId || "").trim()).filter(Boolean)
            : [];
        if (node?.config?.includeUnmatched !== false) {
            routePorts.push(String(node?.config?.unmatchedPort || "unmatched").trim() || "unmatched");
        }
        return [...new Set(routePorts)];
    }

    function inputPortsForNode(node) {
        return [...contractForOperator(node?.operator).inputPorts];
    }

    function isInputPortDeclared(node, portId) {
        return inputPortsForNode(node).includes(portId);
    }

    function isOutputPortDeclared(node, portId) {
        return outputPortsForNode(node).includes(portId);
    }

    return Object.freeze({
        RECORDS_PORT,
        MATCHED_PORT,
        REJECTED_PORT,
        CLEAN_PORT,
        DIRTY_PORT,
        LEFT_PORT,
        RIGHT_PORT,
        contractForOperator,
        outputPortsForNode,
        inputPortsForNode,
        isInputPortDeclared,
        isOutputPortDeclared
    });
})();
