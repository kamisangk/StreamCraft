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
        return new JdbcSourceEnumerator(enumContext, initialState());
    }

    @Override
    public SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> enumContext, JdbcSourceEnumeratorState checkpoint) {
        return new JdbcSourceEnumerator(enumContext, checkpoint == null ? initialState() : checkpoint);
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
        return new JdbcSourceEnumeratorState(new JdbcSourceSplit(
                "jdbc-source",
                new IncrementalSourceState(
                        config.cursorType().name(),
                        config.cursorField(),
                        config.initialCursorValue().isBlank() ? null : config.initialCursorValue(),
                        config.idField().isBlank() ? null : config.idField(),
                        null)));
    }
}
