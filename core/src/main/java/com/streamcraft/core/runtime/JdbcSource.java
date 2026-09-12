package com.streamcraft.core.runtime;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.jdbc.JdbcSourceConfig;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

final class JdbcSource implements Source<DataEntity, JdbcSourceSplit, JdbcSourceEnumeratorState> {

    private static final long serialVersionUID = 1L;

    private final JdbcSourceConfig config;

    JdbcSource(JdbcSourceConfig config) {
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return config.readMode() == JdbcSourceConfig.ReadMode.FULL
                ? Boundedness.BOUNDED
                : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<DataEntity, JdbcSourceSplit> createReader(SourceReaderContext readerContext) {
        return new JdbcSourceReader(readerContext, config);
    }

    @Override
    public SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> enumContext) {
        return new JdbcSourceEnumerator(enumContext, initialState(), config);
    }

    @Override
    public SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> enumContext, JdbcSourceEnumeratorState checkpoint) {
        return new JdbcSourceEnumerator(
                enumContext, checkpoint == null ? initialState() : checkpoint, config);
    }

    @Override
    public SimpleVersionedSerializer<JdbcSourceSplit> getSplitSerializer() {
        return new JdbcSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<JdbcSourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new JdbcSourceEnumeratorStateSerializer();
    }

    private JdbcSourceEnumeratorState initialState() {
        return new JdbcSourceEnumeratorState(splitPlan(config));
    }

    static List<JdbcSourceSplit> splitPlan(JdbcSourceConfig config) {
        IncrementalSourceState initial = new IncrementalSourceState(
                config.cursorType().name(),
                config.cursorField(),
                config.initialCursorValue().isBlank() ? null : config.initialCursorValue(),
                config.idField().isBlank() ? null : config.idField(),
                null);
        if (!config.rangeSplittingEnabled()) {
            return List.of(new JdbcSourceSplit("jdbc-source", initial));
        }
        int count = config.splitCount();
        List<JdbcSourceSplit> result = new ArrayList<>(count);
        if (config.cursorType() == JdbcSourceConfig.CursorType.LONG) {
            long lower = parseLongBound(config.splitLowerBound(), "splitLowerBound");
            long upper = parseLongBound(config.splitUpperBound(), "splitUpperBound");
            if (upper <= lower) {
                throw new IllegalArgumentException("JDBC Source splitUpperBound must be greater than splitLowerBound.");
            }
            if (BigDecimal.valueOf(upper).subtract(BigDecimal.valueOf(lower))
                    .compareTo(BigDecimal.valueOf(count)) < 0) {
                throw new IllegalArgumentException("JDBC Source splitCount exceeds the numeric range cardinality.");
            }
            BigDecimal width = BigDecimal.valueOf(upper).subtract(BigDecimal.valueOf(lower))
                    .divide(BigDecimal.valueOf(count), 12, java.math.RoundingMode.DOWN);
            for (int index = 0; index < count; index++) {
                long start = index == 0 ? lower : decimalToLong(BigDecimal.valueOf(lower).add(width.multiply(BigDecimal.valueOf(index))));
                long end = index == count - 1 ? upper : decimalToLong(BigDecimal.valueOf(lower).add(width.multiply(BigDecimal.valueOf(index + 1))));
                result.add(rangeSplit(config, initial, index, start, end, index == count - 1));
            }
        } else {
            BigDecimal lower = parseDecimalBound(config.splitLowerBound(), "splitLowerBound");
            BigDecimal upper = parseDecimalBound(config.splitUpperBound(), "splitUpperBound");
            if (upper.compareTo(lower) <= 0) {
                throw new IllegalArgumentException("JDBC Source splitUpperBound must be greater than splitLowerBound.");
            }
            BigDecimal width = upper.subtract(lower).divide(BigDecimal.valueOf(count), 12, java.math.RoundingMode.DOWN);
            if (width.signum() <= 0) {
                throw new IllegalArgumentException("JDBC Source split range is too narrow for splitCount.");
            }
            for (int index = 0; index < count; index++) {
                BigDecimal start = index == 0 ? lower : lower.add(width.multiply(BigDecimal.valueOf(index)));
                BigDecimal end = index == count - 1 ? upper : lower.add(width.multiply(BigDecimal.valueOf(index + 1)));
                result.add(rangeSplit(config, initial, index, start.stripTrailingZeros().toPlainString(),
                        end.stripTrailingZeros().toPlainString(), index == count - 1));
            }
        }
        return List.copyOf(result);
    }

    private static JdbcSourceSplit rangeSplit(
            JdbcSourceConfig config,
            IncrementalSourceState initial,
            int index,
            Object lower,
            Object upper,
            boolean upperInclusive) {
        return new JdbcSourceSplit(
                "jdbc:" + config.splitColumn() + ":" + index + "/" + config.splitCount(),
                initial,
                config.splitColumn(),
                String.valueOf(lower),
                String.valueOf(upper),
                upperInclusive);
    }

    private static long parseLongBound(String value, String field) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("JDBC Source " + field + " must be a valid LONG.", exception);
        }
    }

    private static BigDecimal parseDecimalBound(String value, String field) {
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("JDBC Source " + field + " must be a valid DOUBLE.", exception);
        }
    }

    private static long decimalToLong(BigDecimal value) {
        return value.setScale(0, java.math.RoundingMode.DOWN).longValueExact();
    }
}
