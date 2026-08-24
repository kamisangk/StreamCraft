package com.streamcraft.shared.port;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Defines the port names shared by the persisted pipeline DSL, Service validation,
 * the Core runtime, and the studio editor.
 */
public final class RuntimePortContract {

    public static final String RECORDS_PORT = "records";
    public static final String MATCHED_PORT = "matched";
    public static final String REJECTED_PORT = "rejected";
    public static final String CLEAN_PORT = "clean";
    public static final String DIRTY_PORT = "dirty";
    public static final String LEFT_PORT = "left";
    public static final String RIGHT_PORT = "right";

    private static final String FILTER_OPERATOR = "FILTER";
    private static final String DATA_QUALITY_OPERATOR = "DATA_QUALITY";
    private static final String STREAM_JOIN_OPERATOR = "STREAM_JOIN";
    private static final String ROUTE_OPERATOR = "ROUTE";

    private static final Set<String> SOURCE_OPERATORS = Set.of(
            "KAFKA_SOURCE",
            "JDBC_SOURCE",
            "ELASTICSEARCH_SOURCE",
            "INFLUXDB_SOURCE",
            "HDFS_FILE_SOURCE");
    private static final Set<String> SINK_OPERATORS = Set.of(
            "KAFKA_SINK",
            "JDBC_SINK",
            "ELASTICSEARCH_SINK",
            "INFLUXDB_SINK",
            "HDFS_FILE_SINK");
    private static final Set<String> SINGLE_RECORD_TRANSFORM_OPERATORS = Set.of(
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
            "CASE_WHEN");

    private static final Map<String, OperatorPortContract> CONTRACTS = createContracts();

    private RuntimePortContract() {
    }

    public static OperatorPortContract forOperator(String operator) {
        return CONTRACTS.getOrDefault(operator, OperatorPortContract.empty());
    }

    public static boolean isInputPortDeclared(String operator, String portId) {
        return forOperator(operator).inputPorts().contains(portId);
    }

    public static boolean isOutputPortDeclared(String operator, String portId) {
        OperatorPortContract contract = forOperator(operator);
        return contract.outputPorts().contains(portId)
                || contract.outputPortsDynamic() && hasText(portId);
    }

    public static String migrateInputPort(String operator, String portId) {
        if (isSingleRecordInputOperator(operator) && "input-0".equals(portId)) {
            return RECORDS_PORT;
        }
        return portId;
    }

    public static String migrateOutputPort(String operator, String portId) {
        if (FILTER_OPERATOR.equals(operator)) {
            return switch (portId) {
                case "true" -> MATCHED_PORT;
                case "false" -> REJECTED_PORT;
                default -> portId;
            };
        }
        if (DATA_QUALITY_OPERATOR.equals(operator) && "output-0".equals(portId)) {
            return CLEAN_PORT;
        }
        if (isSingleRecordOutputOperator(operator) && "output-0".equals(portId)) {
            return RECORDS_PORT;
        }
        return portId;
    }

    private static Map<String, OperatorPortContract> createContracts() {
        Map<String, OperatorPortContract> contracts = new LinkedHashMap<>();
        for (String operator : SOURCE_OPERATORS) {
            contracts.put(operator, contract(Set.of(), Set.of(RECORDS_PORT), false));
        }
        for (String operator : SINK_OPERATORS) {
            contracts.put(operator, contract(Set.of(RECORDS_PORT), Set.of(), false));
        }
        for (String operator : SINGLE_RECORD_TRANSFORM_OPERATORS) {
            contracts.put(operator, contract(Set.of(RECORDS_PORT), Set.of(RECORDS_PORT), false));
        }
        contracts.put(FILTER_OPERATOR, contract(
                Set.of(RECORDS_PORT), orderedSet(MATCHED_PORT, REJECTED_PORT), false));
        contracts.put(DATA_QUALITY_OPERATOR, contract(
                Set.of(RECORDS_PORT), orderedSet(CLEAN_PORT, DIRTY_PORT), false));
        contracts.put(STREAM_JOIN_OPERATOR, contract(
                orderedSet(LEFT_PORT, RIGHT_PORT), Set.of(RECORDS_PORT), false));
        contracts.put(ROUTE_OPERATOR, contract(Set.of(RECORDS_PORT), Set.of(), true));
        return Collections.unmodifiableMap(contracts);
    }

    private static boolean isSingleRecordInputOperator(String operator) {
        return SINK_OPERATORS.contains(operator)
                || SINGLE_RECORD_TRANSFORM_OPERATORS.contains(operator)
                || FILTER_OPERATOR.equals(operator)
                || DATA_QUALITY_OPERATOR.equals(operator)
                || ROUTE_OPERATOR.equals(operator);
    }

    private static boolean isSingleRecordOutputOperator(String operator) {
        return SOURCE_OPERATORS.contains(operator)
                || SINGLE_RECORD_TRANSFORM_OPERATORS.contains(operator)
                || STREAM_JOIN_OPERATOR.equals(operator);
    }

    private static OperatorPortContract contract(
            Set<String> inputPorts, Set<String> outputPorts, boolean outputPortsDynamic) {
        return new OperatorPortContract(inputPorts, outputPorts, outputPortsDynamic);
    }

    private static Set<String> orderedSet(String first, String second) {
        LinkedHashSet<String> ports = new LinkedHashSet<>();
        ports.add(first);
        ports.add(second);
        return ports;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    public record OperatorPortContract(
            Set<String> inputPorts,
            Set<String> outputPorts,
            boolean outputPortsDynamic) {

        public OperatorPortContract {
            inputPorts = immutableSet(inputPorts);
            outputPorts = immutableSet(outputPorts);
        }

        public String singleInputPort() {
            return singlePort(inputPorts, "input");
        }

        public String singleOutputPort() {
            return singlePort(outputPorts, "output");
        }

        private static OperatorPortContract empty() {
            return new OperatorPortContract(Set.of(), Set.of(), false);
        }

        private static Set<String> immutableSet(Set<String> ports) {
            if (ports == null || ports.isEmpty()) {
                return Set.of();
            }
            return Collections.unmodifiableSet(new LinkedHashSet<>(ports));
        }

        private static String singlePort(Set<String> ports, String direction) {
            if (ports.size() != 1) {
                throw new IllegalStateException(
                        "Expected exactly one " + direction + " port, but declared " + ports + ".");
            }
            return ports.iterator().next();
        }
    }
}
