package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;

public class PruneTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        List<String> fields = new ArrayList<>();
        node.config().path("fields").forEach(f -> fields.add(f.asText()));

        return TransformOutputs.single(input.map(entity -> {
            DataEntity result = entity;
            for (String field : fields) {
                result = result.withoutField(field);
            }
            return result;
        }).name(node.name()));
    }
}
