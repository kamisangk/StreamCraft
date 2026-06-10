package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.fields.FieldPathSupport;
import com.streamcraft.shared.lookupjoin.LookupJoinConfig;
import com.streamcraft.shared.lookupjoin.LookupJoinConfig.JoinType;
import com.streamcraft.shared.lookupjoin.LookupJoinConfig.MissingStrategy;
import com.streamcraft.shared.lookupjoin.LookupJoinConfigParser;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

public class LookupJoinTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        LookupJoinConfig config = LookupJoinConfigParser.parse(node.config(), IllegalArgumentException::new);
        Map<String, Map<String, Object>> lookupTable = new LinkedHashMap<>();
        config.entries().forEach(entry -> lookupTable.put(entry.key(), entry.fields()));
        return TransformOutputs.single(input
                .flatMap(new LookupJoinFlatMapFunction(
                        config.sourceField(),
                        config.targetField(),
                        config.joinType(),
                        config.missingStrategy(),
                        config.overwriteTargetField(),
                        lookupTable))
                .name(node.name()));
    }

    private static final class LookupJoinFlatMapFunction extends RichFlatMapFunction<DataEntity, DataEntity> {

        private static final long serialVersionUID = 1L;
        private final String sourceField;
        private final String targetField;
        private final JoinType joinType;
        private final MissingStrategy missingStrategy;
        private final boolean overwriteTargetField;
        private final Map<String, Map<String, Object>> lookupTable;

        private LookupJoinFlatMapFunction(
                String sourceField,
                String targetField,
                JoinType joinType,
                MissingStrategy missingStrategy,
                boolean overwriteTargetField,
                Map<String, Map<String, Object>> lookupTable) {
            this.sourceField = sourceField;
            this.targetField = targetField;
            this.joinType = joinType;
            this.missingStrategy = missingStrategy;
            this.overwriteTargetField = overwriteTargetField;
            this.lookupTable = Map.copyOf(lookupTable);
        }

        @Override
        public void flatMap(DataEntity entity, Collector<DataEntity> out) {
            FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(entity.fields(), sourceField);
            Map<String, Object> joinedFields = lookup.found() && lookup.value() != null
                    ? lookupTable.get(String.valueOf(lookup.value()))
                    : null;

            if (joinedFields == null) {
                if (joinType == JoinType.INNER) {
                    return;
                }
                if (missingStrategy == MissingStrategy.PUT_NULL) {
                    out.collect(join(entity, null));
                    return;
                }
                out.collect(entity);
                return;
            }
            out.collect(join(entity, joinedFields));
        }

        private DataEntity join(DataEntity entity, Object value) {
            if (!overwriteTargetField && FieldPathSupport.lookup(entity.fields(), targetField).found()) {
                return entity;
            }
            return entity.withField(targetField, value);
        }
    }
}
