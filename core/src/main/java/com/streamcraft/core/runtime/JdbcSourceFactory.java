package com.streamcraft.core.runtime;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.shared.jdbc.JdbcSourceConfig;
import com.streamcraft.shared.jdbc.JdbcSourceConfig.CursorType;
import com.streamcraft.shared.jdbc.JdbcSourceConfigParser;
import java.sql.Timestamp;
import java.time.Instant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JdbcSourceFactory {

    public DataStream<DataEntity> create(StreamExecutionEnvironment env, PipelineNode sourceNode) {
        JdbcSourceConfig config = JdbcSourceConfigParser.parse(
                sourceNode.config(),
                IllegalArgumentException::new);
        if (config.rangeSplittingEnabled()) {
            return env.fromSource(new JdbcSource(config), WatermarkStrategy.noWatermarks(), sourceNode.name())
                    .setParallelism(config.splitCount());
        }
        return env.fromSource(new JdbcSource(config), WatermarkStrategy.noWatermarks(), sourceNode.name())
                .setParallelism(1);
    }

    static String incrementalQuery(String baseQuery, String cursorField, boolean hasCursor) {
        return incrementalQuery(baseQuery, cursorField, "", hasCursor);
    }

    static String incrementalQuery(String baseQuery, String cursorField, String idField, boolean hasCursor) {
        String whereClause;
        if (!hasCursor) {
            whereClause = "";
        } else if (idField == null || idField.isBlank()) {
            whereClause = " where " + cursorField + " > ?";
        } else {
            whereClause = " where (" + cursorField + " > ? or (" + cursorField + " = ? and "
                    + idField + " > ?))";
        }
        String orderBy = idField == null || idField.isBlank()
                ? cursorField
                : cursorField + ", " + idField;
        return "select * from (" + baseQuery.trim() + ") streamcraft_jdbc_source"
                + whereClause
                + " order by "
                + orderBy;
    }

    static Object coerceCursor(String value, CursorType cursorType) {
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
}
