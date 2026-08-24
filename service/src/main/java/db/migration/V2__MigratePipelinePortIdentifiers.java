package db.migration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streamcraft.shared.port.RuntimePortContract;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

/**
 * Migrates persisted edge identifiers to the shared semantic port contract.
 * Only edge port fields owned by the source or target operator are changed.
 */
public class V2__MigratePipelinePortIdentifiers extends BaseJavaMigration {

    private static final String SELECT_PIPELINES = "select id, definition_json from pipelines order by id";
    private static final String UPDATE_PIPELINE = "update pipelines set definition_json = ? where id = ?";

    private final ObjectMapper objectMapper;

    public V2__MigratePipelinePortIdentifiers() {
        this(new ObjectMapper());
    }

    V2__MigratePipelinePortIdentifiers(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void migrate(Context context) throws Exception {
        List<DefinitionUpdate> updates = new ArrayList<>();
        try (PreparedStatement select = context.getConnection().prepareStatement(SELECT_PIPELINES);
                ResultSet resultSet = select.executeQuery()) {
            while (resultSet.next()) {
                long pipelineId = resultSet.getLong(1);
                String definitionJson = resultSet.getString(2);
                JsonNode definition = parseDefinition(definitionJson, pipelineId);
                if (!migrateEdgePorts(definition)) {
                    continue;
                }
                updates.add(new DefinitionUpdate(pipelineId, serializeDefinition(definition, pipelineId)));
            }
        }

        updateDefinitions(context.getConnection(), updates);
    }

    private JsonNode parseDefinition(String definitionJson, long pipelineId) {
        try {
            JsonNode definition = objectMapper.readTree(definitionJson);
            if (definition == null || !definition.isObject()) {
                throw new IllegalStateException("Pipeline definition must be a JSON object.");
            }
            return definition;
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException(
                    "Failed to parse pipeline definition for pipeline row " + pipelineId + ".", exception);
        }
    }

    private boolean migrateEdgePorts(JsonNode definition) {
        JsonNode nodesNode = definition.path("nodes");
        JsonNode edgesNode = definition.path("edges");
        if (!nodesNode.isArray() || !edgesNode.isArray()) {
            return false;
        }

        Map<String, String> operatorByNodeId = indexOperators((ArrayNode) nodesNode);
        boolean changed = false;
        for (JsonNode edgeNode : edgesNode) {
            if (!(edgeNode instanceof ObjectNode edge)) {
                continue;
            }

            String sourceOperator = operatorByNodeId.get(edge.path("sourceNodeId").asText(null));
            String targetOperator = operatorByNodeId.get(edge.path("targetNodeId").asText(null));
            changed |= replacePort(edge, "sourcePortId", RuntimePortContract.migrateOutputPort(
                    sourceOperator, edge.path("sourcePortId").asText(null)));
            changed |= replacePort(edge, "targetPortId", RuntimePortContract.migrateInputPort(
                    targetOperator, edge.path("targetPortId").asText(null)));
        }
        return changed;
    }

    private Map<String, String> indexOperators(ArrayNode nodes) {
        Map<String, String> operatorByNodeId = new HashMap<>();
        for (JsonNode node : nodes) {
            if (!(node instanceof ObjectNode)) {
                continue;
            }
            String nodeId = node.path("id").asText(null);
            String operator = node.path("operator").asText(null);
            if (hasText(nodeId) && hasText(operator)) {
                operatorByNodeId.put(nodeId, operator);
            }
        }
        return operatorByNodeId;
    }

    private boolean replacePort(ObjectNode edge, String fieldName, String migratedPortId) {
        JsonNode currentPort = edge.get(fieldName);
        if (currentPort == null || migratedPortId == null || currentPort.isTextual()
                && migratedPortId.equals(currentPort.textValue())) {
            return false;
        }
        if (!currentPort.isTextual()) {
            return false;
        }
        edge.put(fieldName, migratedPortId);
        return true;
    }

    private String serializeDefinition(JsonNode definition, long pipelineId) {
        try {
            return objectMapper.writeValueAsString(definition);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException(
                    "Failed to serialize migrated pipeline definition for pipeline row " + pipelineId + ".",
                    exception);
        }
    }

    private void updateDefinitions(Connection connection, List<DefinitionUpdate> updates) throws Exception {
        if (updates.isEmpty()) {
            return;
        }
        try (PreparedStatement update = connection.prepareStatement(UPDATE_PIPELINE)) {
            for (DefinitionUpdate definitionUpdate : updates) {
                update.setString(1, definitionUpdate.definitionJson());
                update.setLong(2, definitionUpdate.pipelineId());
                update.addBatch();
            }
            update.executeBatch();
        }
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private record DefinitionUpdate(long pipelineId, String definitionJson) {
    }
}
