package com.streamcraft.core.runtime;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import org.apache.flink.streaming.api.datastream.DataStream;

/** Common runtime attachment contract for production Kafka sinks and offline preview sinks. */
public interface KafkaSinkAttacher {

    /** Legacy hook retained for test doubles and callers that do not carry runtime identity. */
    default void attach(DataStream<DataEntity> stream, PipelineNode sinkNode) {
        throw new UnsupportedOperationException("Kafka sink attachment is not implemented.");
    }

    /** Runtime-aware hook used by production graph construction. */
    default void attach(
            DataStream<DataEntity> stream,
            PipelineNode sinkNode,
            String pipelineId,
            String deploymentNamespace,
            boolean checkpointingEnabled) {
        attach(stream, sinkNode);
    }
}
