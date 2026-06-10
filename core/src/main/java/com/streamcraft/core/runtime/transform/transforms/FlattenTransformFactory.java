package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.fields.FieldPathSupport;
import com.streamcraft.shared.flatten.FlattenConfig;
import com.streamcraft.shared.flatten.FlattenConfigParser;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class FlattenTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        FlattenConfig config = FlattenConfigParser.parse(node.config(), IllegalArgumentException::new);
        return TransformOutputs.single(input
                .map(new FlattenMapFunction(
                        config.sourceField(),
                        config.targetPrefix(),
                        config.delimiter(),
                        config.removeSourceField()))
                .name(node.name()));
    }

    private static void flattenInto(
            Map<String, Object> targetFields,
            Map<?, ?> source,
            String path,
            String targetPrefix,
            String delimiter) {
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            String key = entry.getKey() == null ? "" : entry.getKey().toString();
            String nextPath = path.isEmpty() ? key : path + delimiter + key;
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> nestedMap) {
                flattenInto(targetFields, nestedMap, nextPath, targetPrefix, delimiter);
            } else {
                String targetField = targetPrefix.isBlank()
                        ? nextPath
                        : targetPrefix + delimiter + nextPath;
                if (!targetField.isBlank()) {
                    putField(targetFields, targetField, value);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void putField(Map<String, Object> fields, String path, Object value) {
        if (!path.contains(".")) {
            fields.put(path, value);
            return;
        }

        String[] segments = path.split("\\.");
        Map<String, Object> current = fields;
        for (int index = 0; index < segments.length - 1; index++) {
            String segment = segments[index];
            Object next = current.get(segment);
            if (!(next instanceof Map<?, ?> nextMap)) {
                Map<String, Object> replacement = new LinkedHashMap<>();
                current.put(segment, replacement);
                current = replacement;
                continue;
            }

            Map<String, Object> mutableNext = new LinkedHashMap<>((Map<String, Object>) nextMap);
            current.put(segment, mutableNext);
            current = mutableNext;
        }
        current.put(segments[segments.length - 1], value);
    }

    private static final class FlattenMapFunction extends RichMapFunction<DataEntity, DataEntity> {

        private static final long serialVersionUID = 1L;
        private final String sourceField;
        private final String targetPrefix;
        private final String delimiter;
        private final boolean removeSourceField;

        private FlattenMapFunction(String sourceField, String targetPrefix, String delimiter, boolean removeSourceField) {
            this.sourceField = sourceField;
            this.targetPrefix = targetPrefix;
            this.delimiter = delimiter;
            this.removeSourceField = removeSourceField;
        }

        @Override
        public DataEntity map(DataEntity entity) {
            FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(entity.fields(), sourceField);
            if (!lookup.found() || !(lookup.value() instanceof Map<?, ?> sourceMap)) {
                return entity;
            }

            DataEntity baseEntity = removeSourceField
                    ? entity.withoutField(sourceField)
                    : entity;
            Map<String, Object> targetFields = new LinkedHashMap<>(baseEntity.fields());
            flattenInto(targetFields, sourceMap, "", targetPrefix, delimiter);
            return new DataEntity(entity.id(), entity.timestamp(), targetFields, entity.headers());
        }
    }
}
