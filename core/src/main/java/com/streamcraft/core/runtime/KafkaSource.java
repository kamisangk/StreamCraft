package com.streamcraft.core.runtime;

import com.streamcraft.core.model.DataEntity;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class KafkaSource implements Source<DataEntity, KafkaPartitionSplit, KafkaEnumeratorState> {

    private static final long serialVersionUID = 1L;

    private final KafkaSourceConfig config;

    KafkaSource(KafkaSourceConfig config) {
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<DataEntity, KafkaPartitionSplit> createReader(SourceReaderContext readerContext) {
        return new KafkaSourceReader(readerContext, config);
    }

    @Override
    public SplitEnumerator<KafkaPartitionSplit, KafkaEnumeratorState> createEnumerator(
            SplitEnumeratorContext<KafkaPartitionSplit> enumContext) {
        return new KafkaSplitEnumerator(
                enumContext,
                config,
                new KafkaAdminMetadataClient(config),
                new KafkaEnumeratorState(java.util.List.of(), java.util.Set.of()));
    }

    @Override
    public SplitEnumerator<KafkaPartitionSplit, KafkaEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<KafkaPartitionSplit> enumContext, KafkaEnumeratorState checkpoint) {
        return new KafkaSplitEnumerator(enumContext, config, new KafkaAdminMetadataClient(config), checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<KafkaPartitionSplit> getSplitSerializer() {
        return new KafkaPartitionSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<KafkaEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new KafkaEnumeratorStateSerializer();
    }
}
