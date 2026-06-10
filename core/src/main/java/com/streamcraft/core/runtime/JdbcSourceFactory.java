package com.streamcraft.core.runtime;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.shared.jdbc.JdbcSourceConfig;
import com.streamcraft.shared.jdbc.JdbcSourceConfig.CursorType;
import com.streamcraft.shared.jdbc.JdbcSourceConfig.ReadMode;
import com.streamcraft.shared.jdbc.JdbcSourceConfigParser;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceFactory.class);

    public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
        JdbcSourceConfig config = JdbcSourceConfigParser.parse(
                sourceNode.config(),
                IllegalArgumentException::new);
        return env.addSource(new JdbcSourceFunction(config))
                .name(sourceNode.name())
                .setParallelism(1);
    }

    static String incrementalQuery(String baseQuery, String cursorField, boolean hasCursor) {
        String whereClause = hasCursor ? " where " + cursorField + " > ?" : "";
        return "select * from (" + baseQuery.trim() + ") streamcraft_jdbc_source"
                + whereClause
                + " order by "
                + cursorField;
    }

    private static final class JdbcSourceFunction extends RichParallelSourceFunction<DataEntity> {

        private static final long serialVersionUID = 1L;

        private final JdbcSourceConfig config;
        private volatile boolean running = true;
        private transient Connection connection;

        private JdbcSourceFunction(JdbcSourceConfig config) {
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            Class.forName(config.driver());
            connection = DriverManager.getConnection(config.url(), connectionProperties(config));
        }

        @Override
        public void run(SourceContext<DataEntity> sourceContext) throws Exception {
            if (config.readMode() == ReadMode.FULL) {
                runFull(sourceContext);
                return;
            }
            runIncremental(sourceContext);
        }

        private void runFull(SourceContext<DataEntity> sourceContext) throws Exception {
            try (PreparedStatement statement = connection.prepareStatement(config.resolvedQuery())) {
                applyFetchSize(statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    collectRows(sourceContext, resultSet, -1);
                }
            }
        }

        private void runIncremental(SourceContext<DataEntity> sourceContext) throws Exception {
            Object cursor = coerceInitialCursor(config.initialCursorValue(), config.cursorType());
            int polls = 0;
            while (running && (config.maxPolls() == 0 || polls < config.maxPolls())) {
                Object nextCursor = runIncrementalPoll(sourceContext, cursor);
                if (nextCursor != null) {
                    cursor = nextCursor;
                }
                polls++;
                if (running && (config.maxPolls() == 0 || polls < config.maxPolls())) {
                    sleepIfRunning(config.pollIntervalMillis());
                }
            }
        }

        private Object runIncrementalPoll(SourceContext<DataEntity> sourceContext, Object cursor) throws Exception {
            String query = incrementalQuery(config.resolvedQuery(), config.cursorField(), cursor != null);
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                applyFetchSize(statement);
                if (cursor != null) {
                    statement.setObject(1, cursor);
                }
                try (ResultSet resultSet = statement.executeQuery()) {
                    int cursorIndex = findColumnIndex(resultSet.getMetaData(), config.cursorField());
                    if (cursorIndex < 1) {
                        throw new IllegalArgumentException(
                                "JDBC Source cursorField is not present in query result: " + config.cursorField());
                    }
                    return collectRows(sourceContext, resultSet, cursorIndex);
                }
            }
        }

        private Object collectRows(SourceContext<DataEntity> sourceContext, ResultSet resultSet, int cursorIndex)
                throws Exception {
            ResultSetMetaData metadata = resultSet.getMetaData();
            Object lastCursor = null;
            while (running && resultSet.next()) {
                Object cursorValue = cursorIndex > 0 ? resultSet.getObject(cursorIndex) : null;
                if (cursorIndex > 0 && cursorValue == null) {
                    LOG.warn("Skipping JDBC Source row because cursorField '{}' is null.", config.cursorField());
                    continue;
                }

                Map<String, Object> fields = rowFields(resultSet, metadata);
                synchronized (sourceContext.getCheckpointLock()) {
                    sourceContext.collect(new DataEntity(resolveId(fields), resolveTimestamp(fields), fields, headers()));
                }
                if (cursorValue != null) {
                    lastCursor = cursorValue;
                }
            }
            return lastCursor;
        }

        private Map<String, Object> rowFields(ResultSet resultSet, ResultSetMetaData metadata) throws SQLException {
            Map<String, Object> fields = new LinkedHashMap<>();
            for (int index = 1; index <= metadata.getColumnCount(); index++) {
                String label = metadata.getColumnLabel(index);
                if (label == null || label.isBlank()) {
                    label = metadata.getColumnName(index);
                }
                fields.put(label, normalizeSqlValue(resultSet.getObject(index)));
            }
            return fields;
        }

        private String resolveId(Map<String, Object> fields) {
            if (!config.idField().isBlank()) {
                Object value = fields.get(config.idField());
                if (value != null) {
                    return String.valueOf(value);
                }
            }
            return UUID.randomUUID().toString();
        }

        private long resolveTimestamp(Map<String, Object> fields) {
            if (config.timestampField().isBlank()) {
                return System.currentTimeMillis();
            }
            Object value = fields.get(config.timestampField());
            if (value instanceof Number number) {
                return number.longValue();
            }
            if (value instanceof String text && !text.isBlank()) {
                try {
                    return Instant.parse(text).toEpochMilli();
                } catch (Exception ignored) {
                    try {
                        return Timestamp.valueOf(text).getTime();
                    } catch (Exception ignoredAgain) {
                        return System.currentTimeMillis();
                    }
                }
            }
            return System.currentTimeMillis();
        }

        private Map<String, String> headers() {
            Map<String, String> headers = new HashMap<>();
            headers.put("source", "jdbc");
            headers.put("readMode", config.readMode().name());
            if (!config.cursorField().isBlank()) {
                headers.put("cursorField", config.cursorField());
            }
            return headers;
        }

        private void applyFetchSize(PreparedStatement statement) {
            try {
                statement.setFetchSize(config.fetchSize());
            } catch (SQLException exception) {
                LOG.warn("JDBC driver ignored fetchSize {}: {}", config.fetchSize(), exception.getMessage());
            }
        }

        private void sleepIfRunning(long millis) throws InterruptedException {
            long remaining = millis;
            while (running && remaining > 0) {
                long chunk = Math.min(remaining, 200L);
                Thread.sleep(chunk);
                remaining -= chunk;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void close() throws Exception {
            running = false;
            if (connection != null) {
                connection.close();
            }
        }
    }

    private static Properties connectionProperties(JdbcSourceConfig config) {
        Properties properties = new Properties();
        if (!config.username().isBlank()) {
            properties.setProperty("user", config.username());
        }
        if (!config.password().isEmpty()) {
            properties.setProperty("password", config.password());
        }
        return properties;
    }

    private static int findColumnIndex(ResultSetMetaData metadata, String fieldName) throws SQLException {
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
            String label = metadata.getColumnLabel(index);
            String columnName = metadata.getColumnName(index);
            if (fieldName.equals(label) || fieldName.equalsIgnoreCase(label)
                    || fieldName.equals(columnName) || fieldName.equalsIgnoreCase(columnName)) {
                return index;
            }
        }
        return -1;
    }

    private static Object coerceInitialCursor(String value, CursorType cursorType) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return switch (cursorType) {
            case LONG -> Long.parseLong(value);
            case DOUBLE -> Double.parseDouble(value);
            case TIMESTAMP -> coerceTimestamp(value);
            case STRING -> value;
        };
    }

    private static Timestamp coerceTimestamp(String value) {
        try {
            return Timestamp.from(Instant.parse(value));
        } catch (Exception ignored) {
            return Timestamp.valueOf(value);
        }
    }

    private static Object normalizeSqlValue(Object value) {
        if (value instanceof Timestamp timestamp) {
            return timestamp.toInstant().toString();
        }
        if (value instanceof Date date) {
            return date.toLocalDate().toString();
        }
        if (value instanceof Time time) {
            return time.toLocalTime().toString();
        }
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        return value;
    }
}
