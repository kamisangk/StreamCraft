package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.fields.FieldPathSupport;
import com.streamcraft.shared.lookup.LookupEnrichConfig;
import com.streamcraft.shared.lookup.LookupEnrichConfig.MissingStrategy;
import com.streamcraft.shared.lookup.LookupEnrichConfigParser;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

public class LookupEnrichTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        LookupEnrichConfig config = LookupEnrichConfigParser.parse(node.config(), IllegalArgumentException::new);
        return TransformOutputs.single(input
                .flatMap(new LookupEnrichFlatMapFunction(
                        config.sourceField(),
                        config.targetField(),
                        config.entries().stream().collect(LinkedHashMap::new,
                                (map, entry) -> map.put(entry.key(), entry.value()),
                                Map::putAll),
                        config.missingStrategy(),
                        config.overwriteTargetField()))
                .name(node.name()));
    }

    private static final class LookupEnrichFlatMapFunction extends RichFlatMapFunction<DataEntity, DataEntity> {

        private static final long serialVersionUID = 1L;
        private final String sourceField;
        private final String targetField;
        private final Map<String, Object> lookupMap;
        private final MissingStrategy missingStrategy;
        private final boolean overwriteTargetField;

        private LookupEnrichFlatMapFunction(
                String sourceField,
                String targetField,
                Map<String, Object> lookupMap,
                MissingStrategy missingStrategy,
                boolean overwriteTargetField) {
            this.sourceField = sourceField;
            this.targetField = targetField;
            this.lookupMap = Map.copyOf(lookupMap);
            this.missingStrategy = missingStrategy;
            this.overwriteTargetField = overwriteTargetField;
        }

        @Override
        public void flatMap(DataEntity entity, Collector<DataEntity> out) {
            FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(entity.fields(), sourceField);
            if (!lookup.found() || lookup.value() == null) {
                handleMissing(entity, out);
                return;
            }

            Object matchedValue = lookupMap.get(String.valueOf(lookup.value()));
            if (matchedValue == null) {
                handleMissing(entity, out);
                return;
            }
            out.collect(enrich(entity, matchedValue));
        }

        private void handleMissing(DataEntity entity, Collector<DataEntity> out) {
            switch (missingStrategy) {
                case PUT_NULL -> out.collect(enrich(entity, null));
                case DISCARD -> {
                }
                case FAIL -> throw new IllegalStateException(
                        "LOOKUP_ENRICH missing lookup value for source field '" + sourceField + "'.");
                case KEEP_ORIGINAL -> out.collect(entity);
            }
        }

        private DataEntity enrich(DataEntity entity, Object value) {
            if (!overwriteTargetField && FieldPathSupport.lookup(entity.fields(), targetField).found()) {
                return entity;
            }
            return entity.withField(targetField, value);
        }
    }
}
