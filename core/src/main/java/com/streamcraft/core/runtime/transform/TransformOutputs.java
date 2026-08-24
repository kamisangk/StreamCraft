package com.streamcraft.core.runtime.transform;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.port.RuntimePortContract;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;

public record TransformOutputs(Map<String, DataStream<DataEntity>> streamsByPort) {

    public TransformOutputs {
        if (streamsByPort == null || streamsByPort.isEmpty()) {
            throw new IllegalArgumentException("Transform outputs must contain at least one named stream.");
        }
        streamsByPort = Map.copyOf(streamsByPort);
    }

    public static TransformOutputs single(DataStream<DataEntity> stream) {
        return new TransformOutputs(Map.of(RuntimePortContract.RECORDS_PORT, stream));
    }
}
