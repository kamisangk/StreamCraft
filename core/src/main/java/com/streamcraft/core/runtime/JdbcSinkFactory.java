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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.functions.OpenContext;
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

    private static final class JdbcSinkFunction extends RichSinkFunction<DataEntity> {

        private static final long serialVersionUID = 1L;

        private final JdbcSinkConfig config;
        private final List<DataEntity> buffer = new ArrayList<>();
        private transient Connection connection;
        private transient ObjectMapper objectMapper;
        private transient long lastFlushAt;

        private JdbcSinkFunction(JdbcSinkConfig config) {
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            Class.forName(config.driver());
            connection = DriverManager.getConnection(config.url(), connectionProperties(config));
            connection.setAutoCommit(true);
            objectMapper = new ObjectMapper();
            lastFlushAt = System.currentTimeMillis();
        }

        @Override
        public void invoke(DataEntity value, Context context) throws Exception {
            buffer.add(value);
            long now = System.currentTimeMillis();
            if (buffer.size() >= config.batchSize() || now - lastFlushAt >= config.flushIntervalMillis()) {
                flush();
            }
        }

        @Override
        public void close() throws Exception {
            flush();
            if (connection != null) {
                connection.close();
            }
        }

        private void flush() throws Exception {
            if (buffer.isEmpty()) {
                return;
            }
            if (config.writeMode() == WriteMode.UPSERT) {
                flushUpserts();
            } else {
                flushInserts();
            }
            buffer.clear();
            lastFlushAt = System.currentTimeMillis();
        }

        private void flushInserts() throws Exception {
            try (PreparedStatement statement = connection.prepareStatement(insertSql(config))) {
                for (DataEntity record : buffer) {
                    bindFields(statement, config.fields(), record.fields(), 1);
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        }

        private void flushUpserts() throws Exception {
            try (PreparedStatement updateStatement = connection.prepareStatement(updateSql(config));
                 PreparedStatement insertStatement = connection.prepareStatement(insertSql(config))) {
                boolean hasPendingInserts = false;
                for (DataEntity record : buffer) {
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
