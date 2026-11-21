package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.shared.jdbc.MysqlConnectorConfig;
import com.streamcraft.shared.port.RuntimePortContract;
import com.streamcraft.shared.validation.RuntimePipelineValidationSupport;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class MysqlSourceContractTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void resolvesMysqlDriverQualifiedTableAndPartitionBounds() throws Exception {
        var config = MysqlConnectorConfig.parseSource(mapper.readTree("""
                {"url":"jdbc:mysql://localhost:3306/shop", "tablePath":"shop.order",
                 "partitionNum":2,"partitionColumn":"id","partitionLowerBound":"10",
                 "partitionUpperBound":"20","fetchSize":128,"password":" a "}
                """), IllegalArgumentException::new);

        assertTrue(java.sql.Driver.class.isAssignableFrom(Class.forName(config.driver())));
        assertEquals("select * from `shop`.`order`", config.resolvedQuery());
        assertEquals(128, config.fetchSize());
        assertEquals(" a ", config.password());
        var splits = JdbcSource.splitPlan(config);
        assertEquals(List.of("10", "15"), splits.stream().map(JdbcSourceSplit::lowerBound).toList());
        assertEquals(List.of("15", "20"), splits.stream().map(JdbcSourceSplit::upperBound).toList());
        assertTrue(splits.get(1).upperInclusive());
    }

    @Test
    void registersTheRecordsPortAndRejectsNonMysqlUrls() throws Exception {
        RuntimePipelineValidationSupport.validateRuntimeNode(
                new RuntimePipelineValidationSupport.RuntimeNodeDescriptor("s", "SOURCE", "MYSQL_SOURCE"),
                RuntimePipelineValidationSupport.supportedTransformOperators());
        assertEquals(Set.of("records"), RuntimePortContract.forOperator("MYSQL_SOURCE").outputPorts());
        var config = mapper.readTree("""
                {"url":"jdbc:h2:mem:shop", "query":"select * from orders"}
                """);
        assertThrows(IllegalArgumentException.class,
                () -> MysqlConnectorConfig.parseSource(config, IllegalArgumentException::new));
    }
}
