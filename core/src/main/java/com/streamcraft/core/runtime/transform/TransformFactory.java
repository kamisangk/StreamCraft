package com.streamcraft.core.runtime.transform;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface TransformFactory {
    default TransformOutputs apply(Map<String, DataStream<DataEntity>> inputsByPort, PipelineNode node) {
        return apply(requireInput(inputsByPort, "input-0"), node);
    }

    TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node);

    static DataStream<DataEntity> requireInput(Map<String, DataStream<DataEntity>> inputsByPort, String portId) {
        DataStream<DataEntity> input = inputsByPort.get(portId);
        if (input == null) {
            throw new IllegalArgumentException("Missing required transform input port: " + portId);
        }
        return input;
    }
}
