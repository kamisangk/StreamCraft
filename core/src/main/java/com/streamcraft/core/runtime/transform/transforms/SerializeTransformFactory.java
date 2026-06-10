package com.streamcraft.core.runtime.transform.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializeTransformFactory implements TransformFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SerializeTransformFactory.class);

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        String targetField = node.config().path("targetField").asText();
        String format = node.config().path("format").asText("JSON");
        String delimiter = node.config().path("delimiter").asText(",");
        List<String> sourceFields = new ArrayList<>();
        node.config().path("sourceFields").forEach(f -> sourceFields.add(f.asText()));

        return TransformOutputs.single(input.map(new RichMapFunction<DataEntity, DataEntity>() {
            private static final long serialVersionUID = 1L;
            private transient ObjectMapper objectMapper;

            @Override
            public void open(OpenContext openContext) {
                objectMapper = new ObjectMapper();
            }

            @Override
            public DataEntity map(DataEntity entity) throws Exception {
                try {
                    Map<String, Object> toSerialize = new LinkedHashMap<>();
                    for (String field : sourceFields) {
                        if (entity.fields().containsKey(field)) {
                            toSerialize.put(field, entity.fields().get(field));
                        }
                    }
                    SerializationInput serializationInput = resolveSerializationInput(sourceFields, entity.fields());
                    String serialized = switch (format) {
                        case "JSON" -> objectMapper.writeValueAsString(toSerialize);
                        case "KV" -> toKv(serializationInput);
                        case "CSV" -> toCsv(serializationInput, delimiter);
                        case "XML" -> toXml(serializationInput);
                        default -> objectMapper.writeValueAsString(toSerialize);
                    };
                    return entity.withField(targetField, serialized);
                } catch (Exception exception) {
                    LOG.warn(
                            "Skipping Serialize transform for record '{}' on target field '{}' because {}",
                            entity.id(),
                            targetField,
                            exception.getMessage());
                    return entity;
                }
            }
        }).name(node.name()));
    }

    private static String toKv(SerializationInput serializationInput) {
        List<String> parts = new ArrayList<>();
        for (FieldEntry entry : serializationInput.entries()) {
            parts.add(entry.name() + "=" + stringifyScalar(entry.value()));
        }
        return String.join("&", parts);
    }

    private static String toCsv(SerializationInput serializationInput, String delimiter) {
        List<String> parts = new ArrayList<>();
        for (FieldEntry entry : serializationInput.entries()) {
            parts.add(stringifyScalar(entry.value()));
        }
        return String.join(resolveDelimiter(delimiter), parts);
    }

    private static String toXml(SerializationInput serializationInput) {
        StringBuilder builder = new StringBuilder("<").append(serializationInput.rootName()).append('>');
        for (FieldEntry entry : serializationInput.entries()) {
            builder.append('<')
                    .append(entry.name())
                    .append('>')
                    .append(escapeXml(stringifyScalar(entry.value())))
                    .append("</")
                    .append(entry.name())
                    .append('>');
        }
        builder.append("</").append(serializationInput.rootName()).append('>');
        return builder.toString();
    }

    private static SerializationInput resolveSerializationInput(List<String> sourceFields, Map<String, Object> fields) {
        if (sourceFields.size() == 1) {
            String rootField = sourceFields.get(0);
            Object value = fields.get(rootField);
            if (value instanceof Map<?, ?> nestedMap) {
                List<FieldEntry> entries = new ArrayList<>();
                for (Map.Entry<?, ?> entry : nestedMap.entrySet()) {
                    entries.add(new FieldEntry(String.valueOf(entry.getKey()), entry.getValue()));
                }
                return new SerializationInput(rootField, entries);
            }
        }

        List<FieldEntry> entries = new ArrayList<>();
        for (String sourceField : sourceFields) {
            if (!fields.containsKey(sourceField)) {
                continue;
            }
            entries.add(new FieldEntry(sourceField, fields.get(sourceField)));
        }
        return new SerializationInput("root", entries);
    }

    private static String stringifyScalar(Object value) {
        return Objects.toString(value, "null");
    }

    private static String resolveDelimiter(String delimiter) {
        return delimiter == null || delimiter.isEmpty() ? "," : delimiter;
    }

    private static String escapeXml(String value) {
        return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }

    private record FieldEntry(String name, Object value) {}

    private record SerializationInput(String rootName, List<FieldEntry> entries) {}
}
