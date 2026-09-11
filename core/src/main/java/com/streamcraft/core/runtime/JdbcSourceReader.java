package com.streamcraft.core.runtime;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.jdbc.JdbcSourceConfig;
import com.streamcraft.shared.jdbc.JdbcSourceConfig.CursorType;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JdbcSourceReader implements SourceReader<DataEntity, JdbcSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceReader.class);

    private final org.apache.flink.api.connector.source.SourceReaderContext context;
    private final JdbcSourceConfig config;
    private final Object lock = new Object();
    private final ArrayDeque<JdbcRecord> records = new ArrayDeque<>();
    private JdbcSourceSplit split;
    private Connection connection;
    private boolean started;
    private boolean exhausted;
    private int polls;
    private long nextPollAtMillis;

    JdbcSourceReader(
            org.apache.flink.api.connector.source.SourceReaderContext context,
            JdbcSourceConfig config) {
        this.context = context;
        this.config = config;
    }

    @Override
    public void start() {
        started = true;
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<DataEntity> output) throws Exception {
        synchronized (lock) {
            if (!started || split == null) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            if (!records.isEmpty()) {
                emitOne(output);
                return records.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
            }
            if (exhausted) {
                return InputStatus.END_OF_INPUT;
            }
            if (config.readMode() == JdbcSourceConfig.ReadMode.INCREMENTAL
                    && config.maxPolls() > 0
                    && polls >= config.maxPolls()) {
                exhausted = true;
                return InputStatus.END_OF_INPUT;
            }
            long now = System.currentTimeMillis();
            if (now < nextPollAtMillis) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            loadBatch();
            if (!records.isEmpty()) {
                emitOne(output);
                return records.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
            }
            if (config.readMode() == JdbcSourceConfig.ReadMode.FULL
                    || (config.maxPolls() > 0 && polls >= config.maxPolls())) {
                exhausted = true;
                return InputStatus.END_OF_INPUT;
            }
            nextPollAtMillis = System.currentTimeMillis() + config.pollIntervalMillis();
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    @Override
    public List<JdbcSourceSplit> snapshotState(long checkpointId) {
        synchronized (lock) {
            return split == null ? List.of() : List.of(split);
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        synchronized (lock) {
            if (!records.isEmpty() || exhausted || split == null || System.currentTimeMillis() >= nextPollAtMillis) {
                return CompletableFuture.completedFuture(null);
            }
            long delay = nextPollAtMillis - System.currentTimeMillis();
            CompletableFuture<Void> future = new CompletableFuture<>();
            CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS).execute(() -> future.complete(null));
            return future;
        }
    }

    @Override
    public void addSplits(List<JdbcSourceSplit> splits) {
        synchronized (lock) {
            for (JdbcSourceSplit candidate : splits) {
                if (split == null || split.state().cursorValue() == null
                        || candidate.state().cursorValue() != null) {
                    split = candidate;
                }
            }
            exhausted = false;
            context.sendSplitRequest();
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        // The singleton split is assigned asynchronously after the reader starts.
        // Do not treat this callback as end-of-input before that split arrives.
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            exhausted = true;
            if (connection != null) {
                connection.close();
                connection = null;
            }
        }
    }

    private void loadBatch() throws Exception {
        ensureConnection();
        IncrementalSourceState state = split.state();
        String query;
        if (config.readMode() == JdbcSourceConfig.ReadMode.FULL) {
            query = config.resolvedQuery();
        } else {
            query = JdbcSourceFactory.incrementalQuery(
                    config.resolvedQuery(),
                    config.cursorField(),
                    config.idField(),
                    state.cursorValue() != null);
        }
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            applyFetchSize(statement);
            if (config.readMode() == JdbcSourceConfig.ReadMode.INCREMENTAL && state.cursorValue() != null) {
                bindIncrementalParameters(statement, state);
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                readRows(resultSet);
            }
        }
        if (config.readMode() == JdbcSourceConfig.ReadMode.FULL) {
            exhausted = true;
        }
        polls++;
    }

    private void readRows(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        int cursorIndex = config.readMode() == JdbcSourceConfig.ReadMode.INCREMENTAL
                ? findColumnIndex(metadata, config.cursorField())
                : -1;
        int idIndex = config.idField().isBlank() ? -1 : findColumnIndex(metadata, config.idField());
        if (config.readMode() == JdbcSourceConfig.ReadMode.INCREMENTAL && cursorIndex < 1) {
            throw new IllegalArgumentException(
                    "JDBC Source cursorField is not present in query result: " + config.cursorField());
        }
        if (!config.idField().isBlank() && idIndex < 1) {
            throw new IllegalArgumentException(
                    "JDBC Source idField is not present in query result: " + config.idField());
        }
        while (resultSet.next()) {
            Object cursor = cursorIndex > 0 ? resultSet.getObject(cursorIndex) : null;
            if (cursorIndex > 0 && cursor == null) {
                LOG.warn("Skipping JDBC Source row because cursorField '{}' is null.", config.cursorField());
                continue;
            }
            Object uniqueId = idIndex > 0 ? resultSet.getObject(idIndex) : null;
            if (idIndex > 0 && uniqueId == null) {
                LOG.warn("Skipping JDBC Source row because idField '{}' is null.", config.idField());
                continue;
            }
            Map<String, Object> fields = rowFields(resultSet, metadata);
            records.add(new JdbcRecord(
                    new DataEntity(resolveId(fields), resolveTimestamp(fields), fields, headers()),
                    cursor == null ? null : cursorString(cursor),
                    uniqueId == null ? null : String.valueOf(uniqueId)));
        }
    }

    private void emitOne(ReaderOutput<DataEntity> output) {
        JdbcRecord record = records.removeFirst();
        output.collect(record.entity());
        if (config.readMode() == JdbcSourceConfig.ReadMode.INCREMENTAL && record.cursorValue() != null) {
            split = new JdbcSourceSplit(
                    split.splitId(),
                    new IncrementalSourceState(
                            config.cursorType().name(),
                            record.cursorValue(),
                            config.idField().isBlank() ? null : config.idField(),
                            config.idField().isBlank() ? null : record.uniqueId()));
        }
    }

    private void bindIncrementalParameters(PreparedStatement statement, IncrementalSourceState state)
            throws SQLException {
        Object cursor = JdbcSourceFactory.coerceCursor(state.cursorValue(), config.cursorType());
        statement.setObject(1, cursor);
        if (!config.idField().isBlank()) {
            statement.setObject(2, cursor);
            statement.setObject(3, state.uniqueIdValue());
        }
    }

    private void ensureConnection() throws Exception {
        if (connection != null) {
            return;
        }
        Class.forName(config.driver());
        connection = DriverManager.getConnection(config.url(), connectionProperties(config));
    }

    private void applyFetchSize(PreparedStatement statement) {
        try {
            statement.setFetchSize(config.fetchSize());
        } catch (SQLException exception) {
            LOG.warn("JDBC driver ignored fetchSize {}: {}", config.fetchSize(), exception.getMessage());
        }
    }

    private Map<String, Object> rowFields(ResultSet resultSet, ResultSetMetaData metadata) throws SQLException {
        Map<String, Object> fields = new java.util.LinkedHashMap<>();
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
        Object value = fieldValue(fields, config.idField());
        return value == null ? UUID.randomUUID().toString() : String.valueOf(value);
    }

    private long resolveTimestamp(Map<String, Object> fields) {
        if (config.timestampField().isBlank()) {
            return System.currentTimeMillis();
        }
        Object value = fieldValue(fields, config.timestampField());
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

    private static Object fieldValue(Map<String, Object> fields, String field) {
        if (field == null || field.isBlank()) {
            return null;
        }
        Object value = fields.get(field);
        if (value != null) {
            return value;
        }
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            if (field.equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String cursorString(Object value) {
        if (value instanceof Timestamp timestamp) {
            return timestamp.toInstant().toString();
        }
        return String.valueOf(value);
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

    private record JdbcRecord(DataEntity entity, String cursorValue, String uniqueId) {
    }
}
