package com.streamcraft.core.runtime;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.elasticsearch.ElasticsearchSourceConfig;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class ElasticsearchSource implements Source<DataEntity, ElasticsearchSourceSplit, ElasticsearchSourceEnumeratorState> {

    private static final long serialVersionUID = 1L;
    private final ElasticsearchSourceConfig config;

    ElasticsearchSource(ElasticsearchSourceConfig config) {
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return config.readMode() == ElasticsearchSourceConfig.ReadMode.FULL
                ? Boundedness.BOUNDED
                : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<DataEntity, ElasticsearchSourceSplit> createReader(SourceReaderContext readerContext) {
        return new ElasticsearchSourceReader(readerContext, config);
    }

    @Override
    public SplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<ElasticsearchSourceSplit> enumContext) {
        return new ElasticsearchSourceEnumerator(enumContext, initialState());
    }

    @Override
    public SplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<ElasticsearchSourceSplit> enumContext,
            ElasticsearchSourceEnumeratorState checkpoint) {
        return new ElasticsearchSourceEnumerator(enumContext, checkpoint == null ? initialState() : checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<ElasticsearchSourceSplit> getSplitSerializer() {
        return new ElasticsearchSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<ElasticsearchSourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new ElasticsearchSourceEnumeratorStateSerializer();
    }

    private ElasticsearchSourceEnumeratorState initialState() {
        return new ElasticsearchSourceEnumeratorState(new ElasticsearchSourceSplit(
                "elasticsearch-source",
                new IncrementalSourceState(
                        config.cursorType().name(),
                        config.initialCursorValue().isBlank() ? null : config.initialCursorValue(),
                        config.idField().isBlank() ? null : config.idField(),
                        null),
                "",
                0L));
    }
}
