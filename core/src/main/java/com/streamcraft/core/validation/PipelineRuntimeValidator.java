package com.streamcraft.core.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.PipelineDefinition;
import com.streamcraft.core.model.PipelineEdge;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.model.PipelineNodeType;
import com.streamcraft.core.model.PipelineOperator;
import com.streamcraft.core.runtime.ExecutionMode;
import com.streamcraft.core.runtime.transform.TransformOperatorFactory;
import com.streamcraft.shared.aggregation.AggregateConfigParser;
import com.streamcraft.shared.casewhen.CaseWhenConfigParser;
import com.streamcraft.shared.dataquality.DataQualityConfigParser;
import com.streamcraft.shared.deduplication.DeduplicateConfigParser;
import com.streamcraft.shared.eval.EvalConfigParser;
import com.streamcraft.shared.explode.ExplodeConfigParser;
import com.streamcraft.shared.flatten.FlattenConfigParser;
import com.streamcraft.shared.expression.SafeExpressionSupport;
import com.streamcraft.shared.elasticsearch.ElasticsearchSinkConfigParser;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfigParser;
import com.streamcraft.shared.file.HdfsFileSinkConfigParser;
import com.streamcraft.shared.file.HdfsFileSourceConfigParser;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PipelineRuntimeValidator {

    private static final Set<PipelineOperator> SUPPORTED_TRANSFORM_OPERATORS =
            TransformOperatorFactory.supportedOperators();
    private static final Set<String> SUPPORTED_TRANSFORM_OPERATOR_NAMES = supportedTransformOperatorNames();
    private static final Set<String> SUPPORTED_CONSUME_MODES = Set.of("earliest", "latest", "committed");
    private static final Set<String> SUPPORTED_FORMATS = Set.of("JSON", "TEXT");
    private static final Set<String> SUPPORTED_TRANSFORM_SERDE_FORMATS = Set.of("JSON", "KV", "CSV", "XML");
    private static final Set<String> SUPPORTED_AUTH_TYPES = Set.of("NONE", "SASL_PLAIN", "SASL_SCRAM");
    private static final Set<String> SUPPORTED_SCRAM_MECHANISMS = Set.of("SCRAM-SHA-256", "SCRAM-SHA-512");

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static Set<String> supportedTransformOperatorNames() {
        Set<String> names = SUPPORTED_TRANSFORM_OPERATORS.stream()
                .map(Enum::name)
                .collect(Collectors.toCollection(HashSet::new));
        names.add(PipelineOperator.AGGREGATE.name());
        return Set.copyOf(names);
    }

    public void validate(PipelineDefinition definition) {
        validate(definition, ExecutionMode.RUN);
    }

    public void validate(PipelineDefinition definition, ExecutionMode executionMode) {
        if (definition == null) {
            throw new IllegalArgumentException("Pipeline definition is required.");
        }
        if (definition.nodes() == null || definition.nodes().isEmpty()) {
            throw new IllegalArgumentException("Pipeline must contain at least one node.");
        }

        ExecutionMode normalizedExecutionMode = executionMode == null ? ExecutionMode.RUN : executionMode;
        Map<String, PipelineNode> nodeById = new HashMap<>();
        Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById = new HashMap<>();
        Set<String> sourceIds = new HashSet<>();
        Set<String> sinkIds = new HashSet<>();

        for (PipelineNode node : definition.nodes()) {
            requireText(node.id(), "Node id is required.");
            if (nodeById.putIfAbsent(node.id(), node) != null) {
                throw new IllegalArgumentException("Duplicate node id is not allowed: " + node.id());
            }

            validateSupportedNode(node);
            runtimeNodeById.put(
                    node.id(),
                    new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                            node.id(),
                            node.type().name(),
                            node.operator().name()));

            if (node.type() == PipelineNodeType.SOURCE) {
                sourceIds.add(node.id());
                validateSource(node, normalizedExecutionMode);
            } else if (node.type() == PipelineNodeType.SINK) {
                sinkIds.add(node.id());
                validateSink(node, normalizedExecutionMode);
            } else if (node.type() == PipelineNodeType.TRANSFORM) {
                validateTransformNode(node);
            }
        }

        if (sourceIds.isEmpty() || sinkIds.isEmpty()) {
            throw new IllegalArgumentException("Pipeline must contain at least one Source and one Sink.");
        }
        if (definition.edges() == null || definition.edges().isEmpty()) {
            throw new IllegalArgumentException("Pipeline must contain at least one edge.");
        }

        List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> runtimeEdges = definition.edges().stream()
                .map(this::toRuntimeEdge)
                .toList();
        validateEdges(runtimeEdges, nodeById.keySet(), runtimeNodeById);
        RuntimePipelineValidationSupport.ensureRequiredInputPorts(runtimeNodeById, runtimeEdges);
        RuntimePipelineValidationSupport.ensureNoOutgoingEdgesFromSinks(sinkIds, runtimeEdges);
        RuntimePipelineValidationSupport.ensureAcyclic(nodeById.keySet(), runtimeEdges);
        RuntimePipelineValidationSupport.ensureExecutablePaths(sourceIds, sinkIds, nodeById.keySet(), runtimeEdges);
    }

    private void validateSupportedNode(PipelineNode node) {
        PipelineNodeType nodeType = node.type();
        if (nodeType == null || nodeType == PipelineNodeType.UNKNOWN) {
            throw new IllegalArgumentException("Node " + node.id() + " must declare a supported type.");
        }

        PipelineOperator operator = node.operator();
        if (operator == null || operator == PipelineOperator.UNKNOWN) {
            throw new IllegalArgumentException("Node " + node.id() + " must declare a supported operator.");
        }

        RuntimePipelineValidationSupport.validateRuntimeNode(
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor(
                        node.id(),
                        nodeType.name(),
                        operator.name()),
                SUPPORTED_TRANSFORM_OPERATOR_NAMES);
    }

    private void validateSource(PipelineNode node, ExecutionMode executionMode) {
        if (executionMode.forceMockSources()) {
            validatePreviewSource(node.config());
            return;
        }
        switch (node.operator()) {
            case KAFKA_SOURCE -> validateKafkaSource(node);
            case JDBC_SOURCE -> validateJdbcSource(node);
            case ELASTICSEARCH_SOURCE -> validateElasticsearchSource(node);
            case INFLUXDB_SOURCE -> validateInfluxDbSource(node);
            case HDFS_FILE_SOURCE -> validateHdfsFileSource(node);
            default -> throw new IllegalArgumentException("Unsupported source operator: " + node.operator());
        }
    }

    private void validateKafkaSource(PipelineNode node) {
        JsonNode config = node.config();
        requireJsonText(config, "bootstrapServers");
        JsonNode topics = config.path("topics");
        if (!topics.isArray() || topics.isEmpty()) {
            throw new IllegalArgumentException("Kafka Source must contain at least one topic.");
        }
        requireJsonText(config, "groupId");
        validateConsumeMode(requireJsonText(config, "consumeMode"));
        validateKafkaAuth(config, "Kafka Source");
        validateFormat(requireJsonText(config, "format"));
    }

    private void validateJdbcSource(PipelineNode node) {
        JdbcSourceConfigParser.parseValidated(
                node.config(),
                error -> new IllegalArgumentException(error.defaultMessage()));
    }

    private void validateElasticsearchSource(PipelineNode node) {
        ElasticsearchSourceConfigParser.parseValidated(
                node.config(),
                error -> new IllegalArgumentException(error.defaultMessage()));
    }

    private void validateInfluxDbSource(PipelineNode node) {
        InfluxDbSourceConfigParser.parseValidated(
                node.config(),
                error -> new IllegalArgumentException(error.defaultMessage()));
    }

    private void validateHdfsFileSource(PipelineNode node) {
        HdfsFileSourceConfigParser.parseValidated(
                node.config(),
                error -> new IllegalArgumentException(error.defaultMessage()));
    }

    private void validateSink(PipelineNode node, ExecutionMode executionMode) {
        if (executionMode.interceptSinks()) {
            validatePreviewSink(node);
            return;
        }
        switch (node.operator()) {
            case KAFKA_SINK -> validateKafkaSink(node);
            case JDBC_SINK -> validateJdbcSink(node);
            case ELASTICSEARCH_SINK -> validateElasticsearchSink(node);
            case INFLUXDB_SINK -> validateInfluxDbSink(node);
            case HDFS_FILE_SINK -> validateHdfsFileSink(node);
            default -> throw new IllegalArgumentException("Unsupported sink operator: " + node.operator());
        }
    }

    private void validatePreviewSink(PipelineNode node) {
        if (node.operator() != PipelineOperator.KAFKA_SINK) {
            return;
        }
        JsonNode config = node.config();
        if (config != null && config.hasNonNull("format")) {
            String format = validateFormat(config.path("format").asText());
            if ("TEXT".equals(format)) {
                requireJsonText(config, "messageField");
            }
        }
    }

    private void validateKafkaSink(PipelineNode node) {
        JsonNode config = node.config();
        requireJsonText(config, "bootstrapServers");
        requireJsonText(config, "topic");
        requireJsonText(config, "deliveryGuarantee");
        validateKafkaAuth(config, "Kafka Sink");
        String format = validateFormat(requireJsonText(config, "format"));
        if ("TEXT".equals(format)) {
            requireJsonText(config, "messageField");
        }
    }

    private void validateJdbcSink(PipelineNode node) {
        JdbcSinkConfigParser.parseValidated(
                node.config(),
                error -> new IllegalArgumentException(error.defaultMessage()));
    }

    private void validateElasticsearchSink(PipelineNode node) {
        ElasticsearchSinkConfigParser.parseValidated(
                node.config(),
                error -> new IllegalArgumentException(error.defaultMessage()));
    }

    private void validateInfluxDbSink(PipelineNode node) {
        InfluxDbSinkConfigParser.parseValidated(
                node.config(),
                error -> new IllegalArgumentException(error.defaultMessage()));
    }

    private void validateHdfsFileSink(PipelineNode node) {
        HdfsFileSinkConfigParser.parseValidated(
                node.config(),
                error -> new IllegalArgumentException(error.defaultMessage()));
    }

    private void validateTransformNode(PipelineNode node) {
        JsonNode config = node.config();
        switch (node.operator()) {
            case DESERIALIZE -> {
                requireJsonText(config, "field");
                requireJsonText(config, "targetField");
                String format = validateTransformSerdeFormat(config);
                if ("CSV".equals(format)) {
                    requireNonEmptyArray(config, "fieldNames");
                }
            }
            case SERIALIZE -> {
                requireNonEmptyArray(config, "sourceFields");
                requireJsonText(config, "targetField");
                validateTransformSerdeFormat(config);
            }
            case FILTER -> SafeExpressionSupport.validate(config.path("condition").asText(null), "Filter condition");
            case GROK -> {
                requireJsonText(config, "inputField");
                requireJsonText(config, "outputField");
                GrokPatternSupport.validate(config.path("pattern").asText(null), "Grok pattern");
            }
            case CAST -> {
                requireJsonText(config, "inputField");
                requireJsonText(config, "outputField");
                validateCastTargetType(config.path("targetType").asText(null));
            }
            case RENAME -> validateRenameMapping(config);
            case EVAL -> EvalConfigParser.parseValidated(
                    config, error -> new IllegalArgumentException(error.defaultMessage()));
            case CUSTOM_CODE -> {
                validateCustomCodeLanguage(config.path("language").asText("JAVA"));
                validateCustomCodeCompilePattern(config.path("compilePattern").asText("SOURCE_CODE"));
                requireJsonText(config, "className");
                requireJsonText(config, "sourceCode");
                validateCustomCodeErrorStrategy(config.path("errorStrategy").asText("KEEP_ORIGINAL"));
            }
            case AGGREGATE -> AggregateConfigParser.parse(config, IllegalArgumentException::new);
            case DEDUPLICATE -> DeduplicateConfigParser.parse(config, IllegalArgumentException::new);
            case LOOKUP_ENRICH -> LookupEnrichConfigParser.parseValidated(
                    config, error -> new IllegalArgumentException(error.defaultMessage()));
            case LOOKUP_JOIN -> LookupJoinConfigParser.parseValidated(
                    config, error -> new IllegalArgumentException(error.defaultMessage()));
            case STREAM_JOIN -> StreamJoinConfigParser.parseValidated(
                    config, error -> new IllegalArgumentException(error.defaultMessage()));
            case FLATTEN -> FlattenConfigParser.parseValidated(config, error -> new IllegalArgumentException(error.defaultMessage()));
            case EXPLODE -> ExplodeConfigParser.parseValidated(config, error -> new IllegalArgumentException(error.defaultMessage()));
            case DATA_QUALITY -> DataQualityConfigParser.parseValidated(
                    config, error -> new IllegalArgumentException(error.defaultMessage()));
            case TIME_DERIVE -> TimeDeriveConfigParser.parseValidated(
                    config, error -> new IllegalArgumentException(error.defaultMessage()));
            case MASK_HASH -> MaskHashConfigParser.parseValidated(
                    config, error -> new IllegalArgumentException(error.defaultMessage()));
            case CASE_WHEN -> CaseWhenConfigParser.parseValidated(
                    config, error -> new IllegalArgumentException(error.defaultMessage()));
            case ROUTE -> RouteConfigParser.parseValidated(
                    config, error -> new IllegalArgumentException(error.defaultMessage()));
            default -> {
            }
        }
    }

    private void validateEdges(
            List<RuntimePipelineValidationSupport.RuntimeEdgeDescriptor> edges,
            Set<String> nodeIds,
            Map<String, RuntimePipelineValidationSupport.RuntimeNodeDescriptor> runtimeNodeById) {
        for (RuntimePipelineValidationSupport.RuntimeEdgeDescriptor edge : edges) {
            RuntimePipelineValidationSupport.validateEdgeEndpoints(edge, nodeIds);
            RuntimePipelineValidationSupport.validateRuntimePorts(
                    edge,
                    runtimeNodeById.get(edge.sourceNodeId()),
                    runtimeNodeById.get(edge.targetNodeId()));
        }
    }

    private void requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
    }

    private String requireJsonText(JsonNode node, String fieldName) {
        if (node == null || node.path(fieldName).asText().isBlank()) {
            throw new IllegalArgumentException("Node config field is required: " + fieldName);
        }
        return node.path(fieldName).asText();
    }

    private void requireNonEmptyArray(JsonNode node, String fieldName) {
        JsonNode value = node == null ? null : node.path(fieldName);
        if (value == null || !value.isArray() || value.isEmpty()) {
            throw new IllegalArgumentException("Node config field is required: " + fieldName);
        }
    }

    private void validateRenameMapping(JsonNode config) {
        JsonNode mapping = config == null ? null : config.path("mapping");
        if (mapping == null || !mapping.isObject() || !mapping.fields().hasNext()) {
            throw new IllegalArgumentException("Node config mapping must contain at least one field mapping.");
        }
        mapping.fields().forEachRemaining(entry -> {
            if (entry.getKey() == null || entry.getKey().isBlank()) {
                throw new IllegalArgumentException("Node config mapping source field is required.");
            }
            if (entry.getValue() == null || entry.getValue().asText().isBlank()) {
                throw new IllegalArgumentException("Node config mapping target field is required.");
            }
        });
    }

    private void validateConsumeMode(String consumeMode) {
        if (!SUPPORTED_CONSUME_MODES.contains(consumeMode)) {
            throw new IllegalArgumentException(
                    "Node config consumeMode must be one of: " + String.join(", ", SUPPORTED_CONSUME_MODES));
        }
    }

    private void validatePreviewSource(JsonNode config) {
        String format = validateFormat(config.path("format").asText("JSON"));
        JsonNode sampleData = config.path("sampleData");
        if (!sampleData.isArray()) {
            throw new IllegalArgumentException("Preview requires Kafka Source sampleData to be a string array.");
        }
        for (JsonNode item : sampleData) {
            if (!item.isTextual()) {
                throw new IllegalArgumentException("Preview requires Kafka Source sampleData to be a string array.");
            }
            if ("JSON".equals(format)) {
                validatePreviewJsonSample(item.asText());
            }
        }
    }

    private void validateKafkaAuth(JsonNode config, String nodeLabel) {
        String authType = requireJsonText(config, "authType");
        if (!SUPPORTED_AUTH_TYPES.contains(authType)) {
            throw new IllegalArgumentException(
                    nodeLabel + " authType must be one of: " + String.join(", ", SUPPORTED_AUTH_TYPES) + ".");
        }
        switch (authType) {
            case "NONE" -> {
                return;
            }
            case "SASL_PLAIN" -> {
                requireAuthField(config, nodeLabel, "username", authType);
                requireAuthField(config, nodeLabel, "password", authType);
            }
            case "SASL_SCRAM" -> {
                requireAuthField(config, nodeLabel, "username", authType);
                requireAuthField(config, nodeLabel, "password", authType);
                String scramMechanism = requireAuthField(config, nodeLabel, "scramMechanism", authType);
                if (!SUPPORTED_SCRAM_MECHANISMS.contains(scramMechanism)) {
                    throw new IllegalArgumentException(nodeLabel + " scramMechanism must be one of: "
                            + String.join(", ", SUPPORTED_SCRAM_MECHANISMS) + ".");
                }
            }
            default -> throw new IllegalArgumentException(
                    nodeLabel + " authType must be one of: " + String.join(", ", SUPPORTED_AUTH_TYPES) + ".");
        }
    }

    private void validatePreviewJsonSample(String sample) {
        try {
            JsonNode json = objectMapper.readTree(sample);
            if (!json.isObject()) {
                throw new IllegalArgumentException("Preview JSON sample must be a JSON object.");
            }
        } catch (IOException exception) {
            throw new IllegalArgumentException("Preview JSON sample must be a JSON object.", exception);
        }
    }

    private String requireAuthField(JsonNode config, String nodeLabel, String fieldName, String authType) {
        String value = config.path(fieldName).asText(null);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    nodeLabel + " " + fieldName + " is required when authType is " + authType + ".");
        }
        return value;
    }

    private String validateFormat(String format) {
        if (!SUPPORTED_FORMATS.contains(format)) {
            throw new IllegalArgumentException("Node config format must be one of: JSON, TEXT");
        }
        return format;
    }

    private String validateTransformSerdeFormat(JsonNode config) {
        String format = requireJsonText(config, "format");
        if (!SUPPORTED_TRANSFORM_SERDE_FORMATS.contains(format)) {
            throw new IllegalArgumentException("Node config format must be one of: "
                    + String.join(", ", SUPPORTED_TRANSFORM_SERDE_FORMATS));
        }
        return format;
    }

    private void validateCastTargetType(String targetType) {
        String normalized = targetType == null ? "" : targetType.trim().toUpperCase();
        if (!Set.of("STRING", "INT", "INTEGER", "LONG", "DOUBLE", "FLOAT", "BOOLEAN").contains(normalized)) {
            throw new IllegalArgumentException(
                    "Node config targetType must be one of: STRING, INT, INTEGER, LONG, DOUBLE, FLOAT, BOOLEAN");
        }
    }

    private void validateCustomCodeLanguage(String language) {
        if (!"JAVA".equals(normalize(language))) {
            throw new IllegalArgumentException("Node config language must be JAVA");
        }
    }

    private void validateCustomCodeCompilePattern(String compilePattern) {
        if (!"SOURCE_CODE".equals(normalize(compilePattern))) {
            throw new IllegalArgumentException("Node config compilePattern must be SOURCE_CODE");
        }
    }

    private void validateCustomCodeErrorStrategy(String errorStrategy) {
        if (!Set.of("KEEP_ORIGINAL", "SKIP", "FAIL").contains(normalize(errorStrategy))) {
            throw new IllegalArgumentException("Node config errorStrategy must be one of: KEEP_ORIGINAL, SKIP, FAIL");
        }
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase();
    }

    private RuntimePipelineValidationSupport.RuntimeEdgeDescriptor toRuntimeEdge(PipelineEdge edge) {
        requireText(edge.sourceNodeId(), "Edge sourceNodeId is required.");
        requireText(edge.targetNodeId(), "Edge targetNodeId is required.");
        return new RuntimePipelineValidationSupport.RuntimeEdgeDescriptor(
                edge.id(),
                edge.sourceNodeId(),
                edge.sourcePortId(),
                edge.targetNodeId(),
                edge.targetPortId());
    }
}
