package com.streamcraft.core.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.jdbc.JdbcSinkConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.junit.jupiter.api.Test;

class JdbcSinkCheckpointTest {

    @Test
    void lowVolumeRecordsAreDurableBeforeSnapshotReturns() throws Exception {
        String url = createDatabase("checkpoint_flush");
        try (Connection connection = DriverManager.getConnection(url)) {
            JdbcSinkFactory.JdbcSinkFunction sink = new JdbcSinkFactory.JdbcSinkFunction(config(url), connection);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);

            sink.snapshotState(context(7L));
            try (Connection observer = DriverManager.getConnection(url)) {
                assertEquals(1, countRows(observer));
            }

            sink.notifyCheckpointComplete(7L);

            assertEquals(1, countRows(connection));
            assertEquals(0, sink.bufferedRecordCount());
        }
    }

    @Test
    void recordsArrivingAfterSnapshotWaitForALaterCheckpoint() throws Exception {
        String url = createDatabase("checkpoint_snapshot_boundary");
        try (Connection connection = DriverManager.getConnection(url)) {
            JdbcSinkFactory.JdbcSinkFunction sink = new JdbcSinkFactory.JdbcSinkFunction(config(url), connection);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(8L));
            sink.invoke(entity(2L, "Grace"), null);

            sink.notifyCheckpointComplete(8L);

            assertEquals(1, countRows(connection));
            assertEquals(1, sink.bufferedRecordCount());

            sink.snapshotState(context(9L));
            sink.notifyCheckpointComplete(9L);
            assertEquals(2, countRows(connection));
            assertEquals(0, sink.bufferedRecordCount());
        }
    }

    @Test
    void abortedCheckpointMayExposeWritesWithAtLeastOnceDelivery() throws Exception {
        String url = createDatabase("checkpoint_abort");
        try (Connection connection = DriverManager.getConnection(url)) {
            JdbcSinkFactory.JdbcSinkFunction sink = new JdbcSinkFactory.JdbcSinkFunction(config(url), connection);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.snapshotState(context(10L));
            sink.notifyCheckpointAborted(10L);
            sink.notifyCheckpointComplete(10L);

            assertEquals(1, countRows(connection));
            assertEquals(0, sink.bufferedRecordCount());
        }
    }

    @Test
    void failedBatchRollsBackAndRetainsBufferedRecords() throws Exception {
        String url = createDatabase("checkpoint_rollback");
        try (Connection connection = DriverManager.getConnection(url)) {
            JdbcSinkFactory.JdbcSinkFunction sink = new JdbcSinkFactory.JdbcSinkFunction(config(url), connection);
            sink.open(null);
            sink.invoke(entity(1L, "Ada"), null);
            sink.invoke(entity(1L, "Grace"), null);
            assertThrows(Exception.class, () -> sink.snapshotState(context(11L)));

            try (Connection observer = DriverManager.getConnection(url)) {
                assertEquals(0, countRows(observer));
            }
            assertEquals(2, sink.bufferedRecordCount());
            assertFalse(connection.getAutoCommit());
        }
    }

    @Test
    void recoveryAfterSnapshotDoesNotNeedTheCompletionCallback() throws Exception {
        String url = createDatabase("checkpoint_recovery");
        try (Connection originalConnection = DriverManager.getConnection(url)) {
            JdbcSinkFactory.JdbcSinkFunction original = new JdbcSinkFactory.JdbcSinkFunction(config(url), originalConnection);
            original.open(null);
            original.invoke(entity(1L, "Ada"), null);
            original.snapshotState(context(12L));
        }

        try (Connection restoredConnection = DriverManager.getConnection(url)) {
            JdbcSinkFactory.JdbcSinkFunction restored = new JdbcSinkFactory.JdbcSinkFunction(config(url), restoredConnection);
            restored.initializeState(null);
            restored.open(null);
            restored.snapshotState(context(13L));
            restored.notifyCheckpointComplete(13L);

            assertEquals(1, countRows(restoredConnection));
        }
    }

    @Test
    void failedSnapshotCanRetryTheRetainedBatch() throws Exception {
        String url = createDatabase("checkpoint_retry");
        try (Connection connection = DriverManager.getConnection(url);
                Connection control = DriverManager.getConnection(url)) {
            JdbcSinkFactory.JdbcSinkFunction sink = new JdbcSinkFactory.JdbcSinkFunction(config(url), connection);
            sink.open(null);
            try (Statement statement = control.createStatement()) {
                statement.execute("drop table orders_out");
            }
            sink.invoke(entity(1L, "Ada"), null);
            assertThrows(Exception.class, () -> sink.snapshotState(context(14L)));
            assertEquals(1, sink.bufferedRecordCount());

            try (Statement statement = control.createStatement()) {
                statement.execute("create table orders_out (id bigint primary key, customer_name varchar(64))");
            }
            sink.snapshotState(context(15L));
            assertEquals(1, countRows(control));
            assertEquals(0, sink.bufferedRecordCount());
        }
    }

    private static JdbcSinkConfig config(String url) {
        return new JdbcSinkConfig(
                url,
                "org.h2.Driver",
                "",
                "",
                "orders_out",
                JdbcSinkConfig.WriteMode.INSERT,
                List.of("id", "customer_name"),
                List.of(),
                100,
                60_000L);
    }

    private static DataEntity entity(long id, String name) {
        return new DataEntity(String.valueOf(id), 0L, Map.of("id", id, "customer_name", name), Map.of());
    }

    private static FunctionSnapshotContext context(long checkpointId) {
        return new FunctionSnapshotContext() {
            @Override
            public long getCheckpointId() {
                return checkpointId;
            }

            @Override
            public long getCheckpointTimestamp() {
                return checkpointId;
            }
        };
    }

    private static String createDatabase(String name) throws Exception {
        String url = "jdbc:h2:mem:jdbc_sink_" + name + "_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        try (Connection connection = DriverManager.getConnection(url);
                Statement statement = connection.createStatement()) {
            statement.execute("create table orders_out (id bigint primary key, customer_name varchar(64))");
        }
        return url;
    }

    private static int countRows(Connection connection) throws Exception {
        try (var statement = connection.createStatement(); var resultSet = statement.executeQuery("select count(*) from orders_out")) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }
}
