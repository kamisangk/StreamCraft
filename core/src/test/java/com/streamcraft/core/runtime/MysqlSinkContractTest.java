package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.jdbc.MysqlConnectorConfig;
import java.sql.DriverManager;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MysqlSinkContractTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void nativeUpsertCommitsDuplicateKeysInTheSameCheckpointBatch() throws Exception {
        var config = MysqlConnectorConfig.parseSink(mapper.readTree("""
                {"url":"jdbc:mysql://localhost:3306/shop","database":"shop","table":"order",
                 "fields":["id","name"],"primaryKeys":["id"],"enableUpsert":true,
                 "batchSize":100,"batchIntervalMs":5000,"password":" secret "}
                """), IllegalArgumentException::new);
        assertEquals(" secret ", config.password());
        assertEquals(100, config.batchSize());
        assertEquals(5000, config.flushIntervalMillis());
        String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL";
        try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
            statement.execute("create schema shop");
            statement.execute("create table `shop`.`order` (`id` integer primary key, `name` varchar(32))");
            var sink = new JdbcSinkFactory.JdbcSinkFunction(config, connection, true);
            sink.open(null);
            sink.invoke(new DataEntity("1", 0, Map.of("id", 1, "name", "first"), Map.of()), null);
            sink.invoke(new DataEntity("1", 0, Map.of("id", 1, "name", "last"), Map.of()), null);
            sink.snapshotState(null);
            try (var observer = DriverManager.getConnection(url); var query = observer.createStatement();
                    var result = query.executeQuery("select count(*), max(`name`) from `shop`.`order`")) {
                result.next();
                assertEquals(1, result.getInt(1));
                assertEquals("last", result.getString(2));
            }
            assertEquals(0, sink.bufferedRecordCount());
        }
    }

    @Test
    void rejectsUpsertWithoutPrimaryKeysAndNonMysqlUrls() throws Exception {
        var config = mapper.readTree("""
                {"url":"jdbc:mysql://localhost:3306/shop","table":"orders",
                 "fields":["id","name"],"enableUpsert":true}
                """);
        assertThrows(IllegalArgumentException.class,
                () -> MysqlConnectorConfig.parseSink(config, IllegalArgumentException::new));
        var invalidUrl = mapper.readTree("""
                {"url":"jdbc:h2:mem:test","table":"orders","fields":["id"]}
                """);
        assertThrows(IllegalArgumentException.class,
                () -> MysqlConnectorConfig.parseSink(invalidUrl, IllegalArgumentException::new));
    }
}
