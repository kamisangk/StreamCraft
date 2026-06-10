package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.fields.FieldPathSupport;
import java.math.BigDecimal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CastTransformFactory implements TransformFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CastTransformFactory.class);

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        String inputField = node.config().path("inputField").asText();
        String outputField = node.config().path("outputField").asText();
        String targetType = node.config().path("targetType").asText("STRING");

        if (inputField == null || inputField.isBlank()) {
            throw new IllegalArgumentException("Input field is required for Cast transform");
        }
        if (outputField == null || outputField.isBlank()) {
            throw new IllegalArgumentException("Output field is required for Cast transform");
        }
        validateTargetType(targetType);

        return TransformOutputs.single(input.map(entity -> {
            FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(entity.fields(), inputField);
            if (!lookup.found() || lookup.value() == null) {
                return entity;
            }

            try {
                Object castedValue = castValue(lookup.value(), targetType);
                return entity.withField(outputField, castedValue);
            } catch (IllegalArgumentException exception) {
                LOG.warn(
                        "Skipping Cast transform for record '{}' on input field '{}' because {}",
                        entity.id(),
                        inputField,
                        exception.getMessage());
                return entity;
            }
        }).name(node.name()));
    }

    private static void validateTargetType(String targetType) {
        switch (targetType.toUpperCase()) {
            case "STRING":
            case "INTEGER":
            case "INT":
            case "LONG":
            case "DOUBLE":
            case "FLOAT":
            case "BOOLEAN":
                return;
            default:
                throw new IllegalArgumentException("Unsupported target type: " + targetType);
        }
    }

    private static Object castValue(Object value, String targetType) {
        try {
            String stringValue = value.toString();

            return switch (targetType.toUpperCase()) {
                case "STRING" -> stringValue;
                case "INTEGER", "INT" -> new BigDecimal(stringValue).intValue();
                case "LONG" -> new BigDecimal(stringValue).longValue();
                case "DOUBLE" -> Double.parseDouble(stringValue);
                case "FLOAT" -> Float.parseFloat(stringValue);
                case "BOOLEAN" -> {
                    if (stringValue.equalsIgnoreCase("true") || stringValue.equals("1")) {
                        yield true;
                    } else if (stringValue.equalsIgnoreCase("false") || stringValue.equals("0")) {
                        yield false;
                    }
                    yield Boolean.parseBoolean(stringValue);
                }
                default -> throw new IllegalArgumentException("Unsupported target type: " + targetType);
            };
        } catch (NumberFormatException | ArithmeticException exception) {
            throw new IllegalArgumentException(
                    "Failed to cast value '%s' to %s".formatted(value, targetType),
                    exception);
        }
    }
}
