package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.explode.ExplodeConfig;
import com.streamcraft.shared.explode.ExplodeConfigParser;
import com.streamcraft.shared.fields.FieldPathSupport;
import java.util.List;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

public class ExplodeTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        ExplodeConfig config = ExplodeConfigParser.parse(node.config(), IllegalArgumentException::new);
        return TransformOutputs.single(input
                .flatMap(new ExplodeFlatMapFunction(
                        config.sourceField(),
                        config.targetField(),
                        config.keepEmpty()))
                .name(node.name()));
    }

    private static final class ExplodeFlatMapFunction extends RichFlatMapFunction<DataEntity, DataEntity> {

        private static final long serialVersionUID = 1L;
        private final String sourceField;
        private final String targetField;
        private final boolean keepEmpty;

        private ExplodeFlatMapFunction(String sourceField, String targetField, boolean keepEmpty) {
            this.sourceField = sourceField;
            this.targetField = targetField;
            this.keepEmpty = keepEmpty;
        }

        @Override
        public void flatMap(DataEntity entity, Collector<DataEntity> out) {
            FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(entity.fields(), sourceField);
            if (!lookup.found()) {
                collectOriginalWhenKeepingEmpty(entity, out);
                return;
            }

            Object value = lookup.value();
            if (value instanceof List<?> items) {
                if (items.isEmpty()) {
                    collectOriginalWhenKeepingEmpty(entity, out);
                    return;
                }
                for (Object item : items) {
                    out.collect(entity.withField(targetField, item));
                }
                return;
            }

            out.collect(entity.withField(targetField, value));
        }

        private void collectOriginalWhenKeepingEmpty(DataEntity entity, Collector<DataEntity> out) {
            if (keepEmpty) {
                out.collect(entity);
            }
        }
    }
}
