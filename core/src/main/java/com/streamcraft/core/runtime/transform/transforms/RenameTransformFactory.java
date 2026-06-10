package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;

public class RenameTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        Map<String, String> mapping = new LinkedHashMap<>();
        node.config().path("mapping").fields().forEachRemaining(entry ->
                mapping.put(entry.getKey(), entry.getValue().asText()));

        return TransformOutputs.single(input.map(entity -> {
            DataEntity renamedEntity = entity;
            for (Map.Entry<String, String> entry : mapping.entrySet()) {
                FieldLookup fieldLookup = lookupField(renamedEntity.fields(), entry.getKey());
                if (fieldLookup.found()) {
                    renamedEntity = renamedEntity
                            .withoutField(entry.getKey())
                            .withField(entry.getValue(), fieldLookup.value());
                }
            }
            return renamedEntity;
        }).name(node.name()));
    }

    @SuppressWarnings("unchecked")
    private static FieldLookup lookupField(Map<String, Object> fields, String key) {
        if (key == null || key.isBlank()) {
            return FieldLookup.notFound();
        }
        if (!key.contains(".")) {
            return fields.containsKey(key)
                    ? FieldLookup.found(fields.get(key))
                    : FieldLookup.notFound();
        }
        if (fields.containsKey(key)) {
            return FieldLookup.found(fields.get(key));
        }

        String[] path = key.split("\\.");
        Map<String, Object> current = fields;
        for (int index = 0; index < path.length - 1; index++) {
            Object next = current.get(path[index]);
            if (!(next instanceof Map<?, ?> nextMap)) {
                return FieldLookup.notFound();
            }
            current = (Map<String, Object>) nextMap;
        }

        String leafKey = path[path.length - 1];
        return current.containsKey(leafKey)
                ? FieldLookup.found(current.get(leafKey))
                : FieldLookup.notFound();
    }

    private record FieldLookup(boolean found, Object value) {

        private static FieldLookup found(Object value) {
            return new FieldLookup(true, value);
        }

        private static FieldLookup notFound() {
            return new FieldLookup(false, null);
        }
    }
}
