package com.streamcraft.core.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.shared.jdbc.JdbcSinkConfig;
import com.streamcraft.shared.jdbc.JdbcSinkConfig.WriteMode;
import com.streamcraft.shared.jdbc.JdbcSinkConfigParser;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

public class JdbcSinkFactory {

    public void attach(DataStream<DataEntity> stream, PipelineNode sinkNode) {
        JdbcSinkConfig config = JdbcSinkConfigParser.parse(
                sinkNode.config(),
                IllegalArgumentException::new);
        stream.addSink(new JdbcSinkFunction(config))
                .name(sinkNode.name());
    }

    /**
     * Checkpoint-aware legacy sink with at-least-once delivery semantics.
     *
     * <p>Records captured by a completed checkpoint are committed in one JDBC
     * transaction. A task failure can replay records that were committed just
     * before the checkpoint boundary because JDBC and Flink do not share a
     * distributed transaction.
     */
    static final class JdbcSinkFunction extends RichSinkFunction<DataEntity>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        private final JdbcSinkConfig config;
        private transient List<DataEntity> buffer;
        private transient Map<Long, List<DataEntity>> checkpointSnapshots;
        private transient Connection connection;
        private transient ObjectMapper objectMapper;
        private transient long lastFlushAt;
        private transient Counter recordsFlushed;
        private transient Counter checkpointFlushes;
        private transient Counter writeFailures;

        private JdbcSinkFunction(JdbcSinkConfig config) {
            this(config, null);
        }

        JdbcSinkFunction(JdbcSinkConfig config, Connection connection) {
            this.config = config;
            this.connection = connection;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            if (connection == null) {
                Class.forName(config.driver());
                connection = DriverManager.getConnection(config.url(), connectionProperties(config));
            }
            connection.setAutoCommit(false);
            objectMapper = new ObjectMapper();
            buffer = new ArrayList<>();
            checkpointSnapshots = new HashMap<>();
            lastFlushAt = System.currentTimeMillis();
            try {
                recordsFlushed = getRuntimeContext().getMetricGroup().counter("records_flushed");
                checkpointFlushes = getRuntimeContext().getMetricGroup().counter("checkpoint_flushes");
                writeFailures = getRuntimeContext().getMetricGroup().counter("write_failures");
            } catch (RuntimeException ignored) {
                // Direct unit tests and local construction have no task metric group.
            }
        }

        @Override
        public void invoke(DataEntity value, Context context) throws Exception {
            synchronized (this) {
                buffer.add(value);
                long now = System.currentTimeMillis();
                if (buffer.size() >= config.batchSize() || now - lastFlushAt >= config.flushIntervalMillis()) {
                    flush();
                }
            }
        }

        @Override
        public void close() throws Exception {
            synchronized (this) {
                if (buffer != null) {
                    flush();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            // The live buffer is intentionally replayed from the upstream checkpoint.
        }

        @Override
        public synchronized void snapshotState(FunctionSnapshotContext context) {
            if (buffer == null || checkpointSnapshots == null) {
                return;
            }
            checkpointSnapshots.put(context.getCheckpointId(), List.copyOf(buffer));
        }

        @Override
        public synchronized void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (checkpointSnapshots == null) {
                return;
            }
            List<DataEntity> snapshot = checkpointSnapshots.get(checkpointId);
            if (snapshot == null) {
                return;
            }
            List<DataEntity> recordsToFlush = recordsStillBuffered(snapshot);
            if (!recordsToFlush.isEmpty()) {
                try {
                    flushRecords(recordsToFlush);
                } catch (Exception exception) {
                    increment(writeFailures);
                    throw exception;
                }
                removeByIdentity(recordsToFlush);
                increment(recordsFlushed, recordsToFlush.size());
            }
            increment(checkpointFlushes);
            checkpointSnapshots.entrySet().removeIf(entry -> entry.getKey() <= checkpointId);
            lastFlushAt = System.currentTimeMillis();
        }

        @Override
        public synchronized void notifyCheckpointAborted(long checkpointId) {
            if (checkpointSnapshots != null) {
                checkpointSnapshots.remove(checkpointId);
            }
        }

        synchronized int bufferedRecordCount() {
            return buffer == null ? 0 : buffer.size();
        }

        private void flush() throws Exception {
            if (buffer.isEmpty()) {
                return;
            }
            List<DataEntity> records = List.copyOf(buffer);
            try {
                flushRecords(records);
            } catch (Exception exception) {
                increment(writeFailures);
                throw exception;
            }
            buffer.clear();
            increment(recordsFlushed, records.size());
            lastFlushAt = System.currentTimeMillis();
        }

        private void flushRecords(List<DataEntity> records) throws Exception {
            try {
                if (config.writeMode() == WriteMode.UPSERT) {
                    flushUpserts(records);
                } else {
                    flushInserts(records);
                }
                connection.commit();
            } catch (Exception exception) {
                try {
                    connection.rollback();
                } catch (Exception rollbackException) {
                    exception.addSuppressed(rollbackException);
                }
                throw exception;
            }
        }

        private void flushInserts(List<DataEntity> records) throws Exception {
            try (PreparedStatement statement = connection.prepareStatement(insertSql(config))) {
                for (DataEntity record : records) {
                    bindFields(statement, config.fields(), record.fields(), 1);
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        }

        private void flushUpserts(List<DataEntity> records) throws Exception {
            try (PreparedStatement updateStatement = connection.prepareStatement(updateSql(config));
                    PreparedStatement insertStatement = connection.prepareStatement(insertSql(config))) {
                boolean hasPendingInserts = false;
                for (DataEntity record : records) {
                    bindUpsertUpdate(updateStatement, record.fields());
                    int updatedRows = updateStatement.executeUpdate();
                    if (updatedRows == 0) {
                        bindFields(insertStatement, config.fields(), record.fields(), 1);
                        insertStatement.addBatch();
                        hasPendingInserts = true;
                    }
                }
                if (hasPendingInserts) {
                    insertStatement.executeBatch();
                }
            }
        }

        private List<DataEntity> recordsStillBuffered(List<DataEntity> snapshot) {
            List<DataEntity> available = new ArrayList<>(buffer);
            List<DataEntity> recordsToFlush = new ArrayList<>();
            for (DataEntity snapshotRecord : snapshot) {
                for (int index = 0; index < available.size(); index++) {
                    if (available.get(index) == snapshotRecord) {
                        recordsToFlush.add(snapshotRecord);
                        available.remove(index);
                        break;
                    }
                }
            }
            return recordsToFlush;
        }

        private void removeByIdentity(List<DataEntity> records) {
            for (DataEntity record : records) {
                for (int index = 0; index < buffer.size(); index++) {
                    if (buffer.get(index) == record) {
                        buffer.remove(index);
                        break;
                    }
                }
            }
        }

        private static void increment(Counter counter) {
            if (counter != null) {
                counter.inc();
            }
        }

        private static void increment(Counter counter, int amount) {
            if (counter != null) {
                counter.inc(amount);
            }
        }

        private void bindUpsertUpdate(PreparedStatement statement, Map<String, Object> fields) throws Exception {
            int index = bindFields(statement, config.nonKeyFields(), fields, 1);
            bindFields(statement, config.keyFields(), fields, index);
        }

        private int bindFields(
                PreparedStatement statement,
                List<String> fieldNames,
                Map<String, Object> fields,
                int startIndex) throws Exception {
            int index = startIndex;
            for (String fieldName : fieldNames) {
                statement.setObject(index, normalizeJdbcValue(resolveField(fields, fieldName)));
                index++;
            }
            return index;
        }

        private Object normalizeJdbcValue(Object value) throws Exception {
            if (value instanceof Map<?, ?> || value instanceof List<?>) {
                return objectMapper.writeValueAsString(value);
            }
            return value;
        }
    }

    static String insertSql(JdbcSinkConfig config) {
        return "insert into "
                + config.tablePath()
                + " ("
                + String.join(", ", config.fields())
                + ") values ("
                + placeholders(config.fields().size())
                + ")";
    }

    static String updateSql(JdbcSinkConfig config) {
        String assignments = config.nonKeyFields().stream()
                .map(field -> field + " = ?")
                .reduce((left, right) -> left + ", " + right)
                .orElseThrow(() -> new IllegalArgumentException(
                        "JDBC Sink UPSERT mode requires at least one non-key field."));
        String predicates = config.keyFields().stream()
                .map(field -> field + " = ?")
                .reduce((left, right) -> left + " and " + right)
                .orElseThrow(() -> new IllegalArgumentException(
                        "JDBC Sink UPSERT mode requires at least one key field."));
        return "update " + config.tablePath() + " set " + assignments + " where " + predicates;
    }

    private static String placeholders(int count) {
        return String.join(", ", java.util.Collections.nCopies(count, "?"));
    }

    private static Object resolveField(Map<String, Object> fields, String fieldName) {
        if (fields == null || fieldName == null || fieldName.isBlank()) {
            return null;
        }
        if (fields.containsKey(fieldName)) {
            return fields.get(fieldName);
        }

        Object current = fields;
        for (String segment : fieldName.split("\\.", -1)) {
            if (segment.isEmpty()) {
                return null;
            }
            if (!(current instanceof Map<?, ?> currentMap)) {
                return null;
            }
            current = currentMap.get(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private static Properties connectionProperties(JdbcSinkConfig config) {
        Properties properties = new Properties();
        if (!config.username().isBlank()) {
            properties.setProperty("user", config.username());
        }
        if (!config.password().isEmpty()) {
            properties.setProperty("password", config.password());
        }
        return properties;
    }
}
