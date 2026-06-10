package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;

public class PutTransformFactory implements TransformFactory {

    private static final Pattern REFERENCE_PATTERN = Pattern.compile("\\$\\{([^{}]+)}");

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        String field = node.config().path("field").asText();
        String value = node.config().path("value").asText();

        return TransformOutputs.single(
                input.map(entity -> entity.withField(field, resolveValue(entity, value)))
                        .name(node.name()));
    }

    private static Object resolveValue(DataEntity entity, String value) {
        if (value == null) {
            return null;
        }

        Matcher matcher = REFERENCE_PATTERN.matcher(value);
        if (!matcher.find()) {
            return value;
        }

        matcher.reset();
        if (matcher.matches()) {
            return entity.fields().get(matcher.group(1));
        }

        StringBuilder resolved = new StringBuilder();
        int cursor = 0;
        while (matcher.find()) {
            resolved.append(value, cursor, matcher.start());
            resolved.append(Objects.toString(entity.fields().get(matcher.group(1)), "null"));
            cursor = matcher.end();
        }
        resolved.append(value.substring(cursor));
        return resolved.toString();
    }
}
