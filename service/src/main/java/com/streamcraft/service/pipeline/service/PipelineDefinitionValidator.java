package com.streamcraft.service.pipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.streamcraft.service.config.UiMessageService;
import com.streamcraft.shared.aggregation.AggregateConfigParser;
import com.streamcraft.shared.casewhen.CaseWhenConfigParser;
import com.streamcraft.shared.dataquality.DataQualityConfigParser;
import com.streamcraft.shared.deduplication.DeduplicateConfigParser;
import com.streamcraft.shared.eval.EvalConfigParser;
import com.streamcraft.shared.explode.ExplodeConfigParser;
import com.streamcraft.shared.elasticsearch.ElasticsearchSinkConfigParser;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfigParser;
import com.streamcraft.shared.expression.SafeExpressionSupport;
import com.streamcraft.shared.file.HdfsFileSinkConfigParser;
import com.streamcraft.shared.file.HdfsFileSourceConfigParser;
import com.streamcraft.shared.flatten.FlattenConfigParser;
import com.streamcraft.shared.influxdb.InfluxDbSinkConfigParser;
import com.streamcraft.shared.influxdb.InfluxDbSourceConfigParser;
import com.streamcraft.shared.jdbc.JdbcSinkConfigParser;
import com.streamcraft.shared.jdbc.JdbcSourceConfigParser;
import com.streamcraft.shared.lookup.LookupEnrichConfigParser;
import com.streamcraft.shared.lookupjoin.LookupJoinConfigParser;
import com.streamcraft.shared.maskhash.MaskHashConfigParser;
import com.streamcraft.shared.pattern.GrokPatternSupport;
import com.streamcraft.shared.route.RouteConfigParser;
import com.streamcraft.shared.streamjoin.StreamJoinConfigParser;
import com.streamcraft.shared.timederive.TimeDeriveConfigParser;
import com.streamcraft.shared.validation.RuntimePipelineValidationSupport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PipelineDefinitionValidator {

    private static final String SOURCE_TYPE = RuntimePipelineValidationSupport.SOURCE_TYPE;
    private static final String TRANSFORM_TYPE = RuntimePipelineValidationSupport.TRANSFORM_TYPE;
    private static final String SINK_TYPE = RuntimePipelineValidationSupport.SINK_TYPE;
    private static final String KAFKA_SOURCE = RuntimePipelineValidationSupport.KAFKA_SOURCE_OPERATOR;
    private static final String JDBC_SOURCE = RuntimePipelineValidationSupport.JDBC_SOURCE_OPERATOR;
    private static final String ELASTICSEARCH_SOURCE = RuntimePipelineValidationSupport.ELASTICSEARCH_SOURCE_OPERATOR;
    private static final String INFLUXDB_SOURCE = RuntimePipelineValidationSupport.INFLUXDB_SOURCE_OPERATOR;
    private static final String HDFS_FILE_SOURCE = RuntimePipelineValidationSupport.HDFS_FILE_SOURCE_OPERATOR;
    private static final String KAFKA_SINK = RuntimePipelineValidationSupport.KAFKA_SINK_OPERATOR;
    private static final String JDBC_SINK = RuntimePipelineValidationSupport.JDBC_SINK_OPERATOR;
    private static final String ELASTICSEARCH_SINK = RuntimePipelineValidationSupport.ELASTICSEARCH_SINK_OPERATOR;
    private static final String INFLUXDB_SINK = RuntimePipelineValidationSupport.INFLUXDB_SINK_OPERATOR;
    private static final String HDFS_FILE_SINK = RuntimePipelineValidationSupport.HDFS_FILE_SINK_OPERATOR;
    private static final Set<String> SUPPORTED_CONSUME_MODES = Set.of("earliest", "latest", "committed");
    private static final Set<String> SUPPORTED_FORMATS = Set.of("JSON", "TEXT");
    private static final Set<String> SUPPORTED_TRANSFORM_SERDE_FORMATS = Set.of("JSON", "KV", "CSV", "XML");
    private static final Set<String> SUPPORTED_AUTH_TYPES = Set.of("NONE", "SASL_PLAIN", "SASL_SCRAM");
    private static final Set<String> SUPPORTED_SCRAM_MECHANISMS = Set.of("SCRAM-SHA-256", "SCRAM-SHA-512");
    private static final Set<String> SUPPORTED_TRANSFORM_OPERATORS = Set.of(
            "PUT",
            "PRUNE",
            "RENAME",
            "DESERIALIZE",
            "SERIALIZE",
            "FILTER",
            "GROK",
            "CAST",
            "EVAL",
            "AGGREGATE",
            "DEDUPLICATE",
            "LOOKUP_ENRICH",
            "LOOKUP_JOIN",
            "STREAM_JOIN",
            "FLATTEN",
            "EXPLODE",
            "DATA_QUALITY",
            "TIME_DERIVE",
            "MASK_HASH",
            "CASE_WHEN",
            "ROUTE",
            "CUSTOM_CODE");

    private final ObjectMapper objectMapper;
    private final UiMessageService messages;

    @Autowired
    public PipelineDefinitionValidator(ObjectMapper objectMapper, UiMessageService messages) {
        this.objectMapper = objectMapper;
        this.messages = messages == null ? UiMessageService.englishFallback() : messages;
    }

    public PipelineDefinitionValidator(ObjectMapper objectMapper) {
        this(objectMapper, UiMessageService.englishFallback());
    }

    public void validateForSave(String definitionJson) {
        JsonNode root = parse(definitionJson);
        ArrayNode nodes = requiredArray(root, "nodes");
        ArrayNode edges = requiredArray(root, "edges");
        Map<String, JsonNode> nodeById = indexNodes(nodes);
        validateSaveTransformConfigs(nodeById);
        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> runtimeEdges = toRuntimeEdges(edges);
        Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById = toRuntimeNodes(nodeById);
        validateEdges(runtimeEdges, nodeById.keySet(), runtimeNodeById, true);
        RuntimePipelineValidationSupport.ensureRequiredInputPorts(runtimeNodeById, runtimeEdges);
    }

    public void validateForRun(String definitionJson) {
        JsonNode root = parse(definitionJson);
        ArrayNode nodes = requiredArray(root, "nodes");
        ArrayNode edges = requiredArray(root, "edges");

        if (nodes.isEmpty()) {
            throw fail("pipeline.validation.sourceSink.execution");
        }

        Map<String, JsonNode> nodeById = indexNodes(nodes);
        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> runtimeEdges = toRuntimeEdges(edges);
        Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById =
                toRuntimeNodes(nodeById);
        List<String> sourceNodeIds = new ArrayList<>();
        Set<String> sinkNodeIds = new HashSet<>();
        for (RuntimePipelineValidationSupport.RuntimeNodeDescriptor node : runtimeNodeById.values()) {
            validateRuntimeNode(node, nodeById.get(node.id()).path("config"));
            String nodeType = node.type();
            if (SOURCE_TYPE.equals(nodeType)) {
                sourceNodeIds.add(node.id());
                validateSource(node, nodeById.get(node.id()), false);
            }
            if (SINK_TYPE.equals(nodeType)) {
                sinkNodeIds.add(node.id());
                validateSink(node, nodeById.get(node.id()), false);
            }
        }

        if (sourceNodeIds.isEmpty() || sinkNodeIds.isEmpty()) {
            throw fail("pipeline.validation.sourceSink.execution");
        }
        validateEdges(runtimeEdges, nodeById.keySet(), runtimeNodeById, true);
        RuntimePipelineValidationSupport.ensureRequiredInputPorts(runtimeNodeById, runtimeEdges);
        RuntimePipelineValidationSupport.ensureAcyclic(nodeById.keySet(), runtimeEdges);
        RuntimePipelineValidationSupport.ensureExecutablePaths(
                new HashSet<>(sourceNodeIds),
                sinkNodeIds,
                nodeById.keySet(),
                runtimeEdges);
        RuntimePipelineValidationSupport.ensureNoOutgoingEdgesFromSinks(sinkNodeIds, runtimeEdges);
    }

    public void validateForPreview(String definitionJson) {
        JsonNode root = parse(definitionJson);
        ArrayNode nodes = requiredArray(root, "nodes");
        ArrayNode edges = requiredArray(root, "edges");

        if (nodes.isEmpty()) {
            throw fail("pipeline.validation.sourceSink.preview");
        }

        Map<String, JsonNode> nodeById = indexNodes(nodes);
        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> runtimeEdges = toRuntimeEdges(edges);
        Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById =
                toRuntimeNodes(nodeById);
        List<String> sourceNodeIds = new ArrayList<>();
        Set<String> sinkNodeIds = new HashSet<>();
        for (RuntimePipelineValidationSupport.RuntimeNodeDescriptor node : runtimeNodeById.values()) {
            validateRuntimeNode(node, nodeById.get(node.id()).path("config"));
            String nodeType = node.type();
            if (SOURCE_TYPE.equals(nodeType)) {
                sourceNodeIds.add(node.id());
                validatePreviewSource(nodeById.get(node.id()));
            }
            if (SINK_TYPE.equals(nodeType)) {
                sinkNodeIds.add(node.id());
                validateSink(node, nodeById.get(node.id()), true);
            }
        }

        if (sourceNodeIds.isEmpty() || sinkNodeIds.isEmpty()) {
            throw fail("pipeline.validation.sourceSink.preview");
        }

        validateEdges(runtimeEdges, nodeById.keySet(), runtimeNodeById, true);
        RuntimePipelineValidationSupport.ensureRequiredInputPorts(runtimeNodeById, runtimeEdges);
        RuntimePipelineValidationSupport.ensureAcyclic(nodeById.keySet(), runtimeEdges);
        RuntimePipelineValidationSupport.ensureExecutablePaths(
                new HashSet<>(sourceNodeIds),
                sinkNodeIds,
                nodeById.keySet(),
                runtimeEdges);
        RuntimePipelineValidationSupport.ensureNoOutgoingEdgesFromSinks(sinkNodeIds, runtimeEdges);
    }

    private JsonNode parse(String definitionJson) {
        try {
            return objectMapper.readTree(definitionJson);
        } catch (IOException ex) {
            throw fail("pipeline.validation.definition.invalidJson", ex);
        }
    }

    private ArrayNode requiredArray(JsonNode node, String fieldName) {
        JsonNode value = node.path(fieldName);
        if (!(value instanceof ArrayNode arrayNode)) {
            throw fail("pipeline.validation.definition.missingArray", fieldName);
        }
        return arrayNode;
    }

    private String requiredText(JsonNode node, String fieldName) {
        String value = node.path(fieldName).asText(null);
        if (value == null || value.isBlank()) {
            throw fail("pipeline.validation.definition.missingField", fieldName);
        }
        return value;
    }

    private void validateRuntimeNode(RuntimePipelineValidationSupport.RuntimeNodeDescriptor node, JsonNode config) {
        RuntimePipelineValidationSupport.validateRuntimeNode(node, SUPPORTED_TRANSFORM_OPERATORS);
        validateRuntimeTransformConfig(node.operator(), config);
    }

    private Map<String, JsonNode> indexNodes(ArrayNode nodes) {
        Map<String, JsonNode> nodeById = new HashMap<>();
        for (JsonNode node : nodes) {
            String nodeId = requiredText(node, "id");
            if (nodeById.putIfAbsent(nodeId, node) != null) {
                throw fail("pipeline.validation.definition.duplicateNode", nodeId);
            }
        }
        return nodeById;
    }

    private Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> toRuntimeNodes(
            Map<String, JsonNode> nodeById) {
        Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById = new HashMap<>();
        for (Map.Entry<String, JsonNode> entry : nodeById.entrySet()) {
            String nodeId = entry.getKey();
            JsonNode node = entry.getValue();
            runtimeNodeById.put(
                    nodeId,
                    new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                            nodeId,
                            requiredText(node, "type"),
                            requiredText(node, "operator")));
        }
        return runtimeNodeById;
    }

    private void validateRuntimeTransformConfig(String operator, JsonNode config) {
        if (config == null || config.isMissingNode() || config.isNull()) {
            config = objectMapper.createObjectNode();
        }
        switch (operator) {
            case "DESERIALIZE" -> {
                requireNonBlank(config, "field");
                requireNonBlank(config, "targetField");
                String format = validateTransformSerdeFormat(config);
                if ("CSV".equals(format)) {
                    requireNonEmptyArray(config, "fieldNames");
                }
            }
            case "SERIALIZE" -> {
                requireNonEmptyArray(config, "sourceFields");
                requireNonBlank(config, "targetField");
                validateTransformSerdeFormat(config);
            }
            case "FILTER" -> SafeExpressionSupport.validate(config.path("condition").asText(null), "Filter condition");
            case "GROK" -> {
                requireNonBlank(config, "inputField");
                requireNonBlank(config, "outputField");
                GrokPatternSupport.validate(config.path("pattern").asText(null), "Grok pattern");
            }
            case "CAST" -> {
                requireNonBlank(config, "inputField");
                requireNonBlank(config, "outputField");
                validateCastTargetType(config.path("targetType").asText(null));
            }
            case "EVAL" -> EvalConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "AGGREGATE" -> AggregateConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "DEDUPLICATE" -> DeduplicateConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "LOOKUP_ENRICH" -> LookupEnrichConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "LOOKUP_JOIN" -> LookupJoinConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "STREAM_JOIN" -> StreamJoinConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "FLATTEN" -> FlattenConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "EXPLODE" -> ExplodeConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "DATA_QUALITY" -> DataQualityConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "TIME_DERIVE" -> TimeDeriveConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "MASK_HASH" -> MaskHashConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "CASE_WHEN" -> CaseWhenConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "ROUTE" -> RouteConfigParser.parseValidated(
                    config, error -> fail(error.messageKey(), error.args()));
            case "CUSTOM_CODE" -> {
                validateCustomCodeLanguage(config.path("language").asText("JAVA"));
                validateCustomCodeCompilePattern(config.path("compilePattern").asText("SOURCE_CODE"));
                requireNonBlank(config, "className");
                requireNonBlank(config, "sourceCode");
                validateCustomCodeErrorStrategy(config.path("errorStrategy").asText("KEEP_ORIGINAL"));
            }
            default -> {
            }
        }
    }

    private void validateSaveTransformConfigs(Map<String, JsonNode> nodeById) {
        for (JsonNode node : nodeById.values()) {
            if (TRANSFORM_TYPE.equals(node.path("type").asText(null))) {
                String operator = node.path("operator").asText(null);
                if ("AGGREGATE".equals(operator)
                        || "DEDUPLICATE".equals(operator)
                        || "LOOKUP_ENRICH".equals(operator)
                        || "LOOKUP_JOIN".equals(operator)
                        || "STREAM_JOIN".equals(operator)
                        || "FLATTEN".equals(operator)
                        || "EXPLODE".equals(operator)
                        || "DATA_QUALITY".equals(operator)
                        || "TIME_DERIVE".equals(operator)
                        || "MASK_HASH".equals(operator)
                        || "CASE_WHEN".equals(operator)
                        || "ROUTE".equals(operator)) {
                    validateRuntimeTransformConfig(operator, node.path("config"));
                }
            }
        }
    }

    private void validateSource(
            RuntimePipelineValidationSupport.RuntimeNodeDescriptor runtimeNode,
            JsonNode node,
            boolean preview) {
        if (preview) {
            validatePreviewSource(node);
            return;
        }
        if (KAFKA_SOURCE.equals(runtimeNode.operator())) {
            validateKafkaSource(node);
            return;
        }
        if (JDBC_SOURCE.equals(runtimeNode.operator())) {
            validateJdbcSource(node);
            return;
        }
        if (ELASTICSEARCH_SOURCE.equals(runtimeNode.operator())) {
            validateElasticsearchSource(node);
            return;
        }
        if (INFLUXDB_SOURCE.equals(runtimeNode.operator())) {
            validateInfluxDbSource(node);
            return;
        }
        if (HDFS_FILE_SOURCE.equals(runtimeNode.operator())) {
            validateHdfsFileSource(node);
            return;
        }
        throw fail("pipeline.validation.source.unsupported", runtimeNode.operator());
    }

    private void validateKafkaSource(JsonNode node) {
        JsonNode config = node.path("config");
        requireNonBlank(config, "bootstrapServers");
        JsonNode topics = config.path("topics");
        if (!topics.isArray() || topics.isEmpty()) {
            throw fail("pipeline.validation.kafkaSource.topicRequired");
        }
        requireNonBlank(config, "groupId");
        String consumeMode = requireNonBlank(config, "consumeMode");
        validateConsumeMode(consumeMode);
        validateKafkaAuth(config, "Kafka Source");
        validateFormat(requireNonBlank(config, "format"));
    }

    private void validateJdbcSource(JsonNode node) {
        JdbcSourceConfigParser.parseValidated(
                node.path("config"),
                error -> fail(error.messageKey(), error.args()));
    }

    private void validateElasticsearchSource(JsonNode node) {
        ElasticsearchSourceConfigParser.parseValidated(
                node.path("config"),
                error -> fail(error.messageKey(), error.args()));
    }

    private void validateInfluxDbSource(JsonNode node) {
        InfluxDbSourceConfigParser.parseValidated(
                node.path("config"),
                error -> fail(error.messageKey(), error.args()));
    }

    private void validateHdfsFileSource(JsonNode node) {
        HdfsFileSourceConfigParser.parseValidated(
                node.path("config"),
                error -> fail(error.messageKey(), error.args()));
    }

    private void validateSink(
            RuntimePipelineValidationSupport.RuntimeNodeDescriptor runtimeNode,
            JsonNode node,
            boolean preview) {
        if (preview) {
            validatePreviewSink(runtimeNode, node);
            return;
        }
        if (KAFKA_SINK.equals(runtimeNode.operator())) {
            validateKafkaSink(node);
            return;
        }
        if (JDBC_SINK.equals(runtimeNode.operator())) {
            validateJdbcSink(node);
            return;
        }
        if (ELASTICSEARCH_SINK.equals(runtimeNode.operator())) {
            validateElasticsearchSink(node);
            return;
        }
        if (INFLUXDB_SINK.equals(runtimeNode.operator())) {
            validateInfluxDbSink(node);
            return;
        }
        if (HDFS_FILE_SINK.equals(runtimeNode.operator())) {
            validateHdfsFileSink(node);
            return;
        }
        throw fail("pipeline.validation.sink.unsupported", runtimeNode.operator());
    }

    private void validateKafkaSink(JsonNode node) {
        JsonNode config = node.path("config");
        requireNonBlank(config, "bootstrapServers");
        requireNonBlank(config, "topic");
        requireNonBlank(config, "deliveryGuarantee");
        validateKafkaAuth(config, "Kafka Sink");
        String format = validateFormat(requireNonBlank(config, "format"));
        if ("TEXT".equals(format)) {
            requireNonBlank(config, "messageField");
        }
    }

    private void validateJdbcSink(JsonNode node) {
        JdbcSinkConfigParser.parseValidated(
                node.path("config"),
                error -> fail(error.messageKey(), error.args()));
    }

    private void validateElasticsearchSink(JsonNode node) {
        ElasticsearchSinkConfigParser.parseValidated(
                node.path("config"),
                error -> fail(error.messageKey(), error.args()));
    }

    private void validateInfluxDbSink(JsonNode node) {
        InfluxDbSinkConfigParser.parseValidated(
                node.path("config"),
                error -> fail(error.messageKey(), error.args()));
    }

    private void validateHdfsFileSink(JsonNode node) {
        HdfsFileSinkConfigParser.parseValidated(
                node.path("config"),
                error -> fail(error.messageKey(), error.args()));
    }

    private void validatePreviewSource(JsonNode node) {
        JsonNode config = node.path("config");
        String format = validateFormat(config.path("format").asText("JSON"));
        JsonNode sampleData = config.path("sampleData");
        if (!sampleData.isArray()) {
            throw fail("pipeline.validation.preview.sampleDataStringArray");
        }
        for (JsonNode item : sampleData) {
            if (!item.isTextual()) {
                throw fail("pipeline.validation.preview.sampleDataStringArray");
            }
            if ("JSON".equals(format)) {
                validatePreviewJsonSample(item.asText());
            }
        }
    }

    private void validatePreviewSink(RuntimePipelineValidationSupport.RuntimeNodeDescriptor runtimeNode, JsonNode node) {
        if (!KAFKA_SINK.equals(runtimeNode.operator())) {
            return;
        }
        JsonNode config = node.path("config");
        if (config != null && !config.isMissingNode() && !config.isNull() && config.hasNonNull("format")) {
            String format = validateFormat(config.path("format").asText());
            if ("TEXT".equals(format)) {
                requireNonBlank(config, "messageField");
            }
        }
    }

    private void validatePreviewJsonSample(String sample) {
        try {
            JsonNode json = objectMapper.readTree(sample);
            if (!json.isObject()) {
                throw fail("pipeline.validation.preview.jsonObject");
            }
        } catch (IOException exception) {
            throw fail("pipeline.validation.preview.jsonObject", exception);
        }
    }

    private String requireNonBlank(JsonNode config, String fieldName) {
        String value = config.path(fieldName).asText(null);
        if (value == null || value.isBlank()) {
            throw fail("pipeline.validation.config.missingField", fieldName);
        }
        return value;
    }

    private void requireNonEmptyArray(JsonNode config, String fieldName) {
        JsonNode value = config.path(fieldName);
        if (!value.isArray() || value.isEmpty()) {
            throw fail("pipeline.validation.config.missingField", fieldName);
        }
    }

    private void validateKafkaAuth(JsonNode config, String nodeLabel) {
        String authType = config.path("authType").asText(null);
        if (authType == null || authType.isBlank()) {
            throw fail("pipeline.validation.auth.required", nodeLabel);
        }
        if (!SUPPORTED_AUTH_TYPES.contains(authType)) {
            throw fail("pipeline.validation.auth.oneOf", nodeLabel, joinValues(SUPPORTED_AUTH_TYPES));
        }
        switch (authType) {
            case "NONE" -> {
                return;
            }
            case "SASL_PLAIN" -> {
                requireNodeField(config, nodeLabel, "username", authType);
                requireNodeField(config, nodeLabel, "password", authType);
            }
            case "SASL_SCRAM" -> {
                requireNodeField(config, nodeLabel, "username", authType);
                requireNodeField(config, nodeLabel, "password", authType);
                String scramMechanism = requireNodeField(config, nodeLabel, "scramMechanism", authType);
                if (!SUPPORTED_SCRAM_MECHANISMS.contains(scramMechanism)) {
                    throw fail("pipeline.validation.scram.oneOf", nodeLabel, joinValues(SUPPORTED_SCRAM_MECHANISMS));
                }
            }
            default -> throw fail("pipeline.validation.auth.oneOf", nodeLabel, joinValues(SUPPORTED_AUTH_TYPES));
        }
    }

    private String requireNodeField(JsonNode config, String nodeLabel, String fieldName, String authType) {
        String value = config.path(fieldName).asText(null);
        if (value == null || value.isBlank()) {
            throw fail("pipeline.validation.auth.fieldRequired", nodeLabel, fieldName, authType);
        }
        return value;
    }

    private void validateConsumeMode(String consumeMode) {
        if (!SUPPORTED_CONSUME_MODES.contains(consumeMode)) {
            throw fail("pipeline.validation.consumeMode.oneOf", joinValues(SUPPORTED_CONSUME_MODES));
        }
    }

    private String validateFormat(String format) {
        if (!SUPPORTED_FORMATS.contains(format)) {
            throw fail("pipeline.validation.format.oneOf", "JSON, TEXT");
        }
        return format;
    }

    private String validateTransformSerdeFormat(JsonNode config) {
        String format = requireNonBlank(config, "format");
        if (!SUPPORTED_TRANSFORM_SERDE_FORMATS.contains(format)) {
            throw fail("pipeline.validation.format.oneOf", joinValues(SUPPORTED_TRANSFORM_SERDE_FORMATS));
        }
        return format;
    }

    private void validateCastTargetType(String targetType) {
        String normalized = targetType == null ? "" : targetType.trim().toUpperCase();
        if (!Set.of("STRING", "INT", "INTEGER", "LONG", "DOUBLE", "FLOAT", "BOOLEAN").contains(normalized)) {
            throw fail("pipeline.validation.targetType.oneOf");
        }
    }

    private void validateCustomCodeLanguage(String language) {
        if (!"JAVA".equals(normalize(language))) {
            throw fail("pipeline.validation.language.javaOnly");
        }
    }

    private void validateCustomCodeCompilePattern(String compilePattern) {
        if (!"SOURCE_CODE".equals(normalize(compilePattern))) {
            throw fail("pipeline.validation.compilePattern.sourceCodeOnly");
        }
    }

    private void validateCustomCodeErrorStrategy(String errorStrategy) {
        if (!Set.of("KEEP_ORIGINAL", "SKIP", "FAIL").contains(normalize(errorStrategy))) {
            throw fail("pipeline.validation.errorStrategy.oneOf");
        }
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase();
    }

    private String joinValues(Set<String> values) {
        return String.join(", ", values.stream().sorted().toList());
    }

    private IllegalArgumentException fail(String key, Object... args) {
        return new IllegalArgumentException(messages.get(key, args));
    }

    private IllegalArgumentException fail(String key, Throwable cause, Object... args) {
        return new IllegalArgumentException(messages.get(key, args), cause);
    }

    private void validateEdges(
            List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> edges,
            Set<String> nodeIds,
            Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById,
            boolean enforceRuntimePorts) {
        for (RuntimePipelineValidationSupport.RuntimeEdgeDescriptor edge : edges) {
            RuntimePipelineValidationSupport.validateEdgeEndpoints(edge, nodeIds);
            if (enforceRuntimePorts) {
                RuntimePipelineValidationSupport.validateRuntimePorts(
                        edge, runtimeNodeById.get(edge.sourceNodeId()), runtimeNodeById.get(edge.targetNodeId()));
            }
        }
    }

    private List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> toRuntimeEdges(ArrayNode edges) {
        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> runtimeEdges = new ArrayList<>();
        for (JsonNode edge : edges) {
            runtimeEdges.add(new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor(
                    edge.path("id").asText(null),
                    requiredText(edge, "sourceNodeId"),
                    requiredText(edge, "sourcePortId"),
                    requiredText(edge, "targetNodeId"),
                    requiredText(edge, "targetPortId")));
        }
        return runtimeEdges;
    }
}

