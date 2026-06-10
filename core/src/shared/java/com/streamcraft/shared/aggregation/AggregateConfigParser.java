package com.streamcraft.shared.aggregation;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamcraft.shared.aggregation.AggregateConfig.AggregationFunction;
import com.streamcraft.shared.aggregation.AggregateConfig.AggregationSpec;
import com.streamcraft.shared.aggregation.AggregateConfig.EventTimeUnit;
import com.streamcraft.shared.aggregation.AggregateConfig.Mode;
import com.streamcraft.shared.aggregation.AggregateConfig.OutputMode;
import com.streamcraft.shared.aggregation.AggregateConfig.SortDirection;
import com.streamcraft.shared.aggregation.AggregateConfig.TimeMode;
import com.streamcraft.shared.aggregation.AggregateConfig.TimeUnit;
import com.streamcraft.shared.aggregation.AggregateConfig.WindowType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

public final class AggregateConfigParser {

    private AggregateConfigParser() {
    }

    public static AggregateConfig parse(JsonNode config, Function<String, IllegalArgumentException> error) {
        return parseValidated(config, validationError -> error.apply(validationError.defaultMessage()));
    }

    public static AggregateConfig parseValidated(JsonNode config, ValidationErrorFactory error) {
        JsonNode safeConfig = config == null || config.isNull() || config.isMissingNode() ? null : config;
        Mode mode = parseEnum(text(safeConfig, "mode", "GLOBAL"), Mode.class, "mode", error);
        WindowType windowType = parseEnum(text(safeConfig, "windowType", "TUMBLING_TIME"), WindowType.class, "windowType", error);
        TimeMode timeMode = parseEnum(text(safeConfig, "timeMode", "PROCESSING_TIME"), TimeMode.class, "timeMode", error);
        TimeUnit timeUnit = parseEnum(text(safeConfig, "timeUnit", "SECONDS"), TimeUnit.class, "timeUnit", error);
        List<String> groupBy = textArray(safeConfig, "groupBy");
        long windowSize = longValue(safeConfig, "windowSize", 60, error);
        long windowSlide = longValue(safeConfig, "windowSlide", 10, error);
        long watermarkDelay = longValue(safeConfig, "watermarkDelay", 30, error);
        String eventTimeField = text(safeConfig, "eventTimeField", "");
        EventTimeUnit eventTimeUnit = parseEnum(text(safeConfig, "eventTimeUnit", "MILLISECONDS"),
                EventTimeUnit.class,
                "eventTimeUnit",
                error);
        OutputMode outputMode = parseEnum(text(safeConfig, "outputMode", "NESTED"),
                OutputMode.class,
                "outputMode",
                error);
        String windowStartField = text(safeConfig, "windowStartField", "windowStart");
        String windowEndField = text(safeConfig, "windowEndField", "windowEnd");
        long countWindowSize = longValue(safeConfig, "countWindowSize", 100, error);
        List<AggregationSpec> aggregations = aggregations(safeConfig, error);

        if (mode == Mode.GROUPED && groupBy.isEmpty()) {
            throw error.apply(ValidationError.groupByRequired());
        }
        if (timeMode == TimeMode.EVENT_TIME && watermarkDelay < 0) {
            throw error.apply(ValidationError.watermarkDelayNonNegative());
        }
        if (windowType == WindowType.COUNT) {
            if (countWindowSize <= 0) {
                throw error.apply(ValidationError.positive("countWindowSize"));
            }
        } else {
            if (windowSize <= 0) {
                throw error.apply(ValidationError.positive("windowSize"));
            }
            if (windowType == WindowType.SLIDING_TIME) {
                if (windowSlide <= 0) {
                    throw error.apply(ValidationError.positive("windowSlide"));
                }
                if (windowSlide > windowSize) {
                    throw error.apply(ValidationError.windowSlideMaxWindowSize());
                }
            }
        }
        if (aggregations.isEmpty()) {
            throw error.apply(ValidationError.aggregationsRequired());
        }

        Set<String> outputFields = new HashSet<>();
        for (AggregationSpec aggregation : aggregations) {
            if (aggregation.outputField() == null || aggregation.outputField().isBlank()) {
                throw error.apply(ValidationError.outputFieldRequired());
            }
            if (!outputFields.add(aggregation.outputField())) {
                throw error.apply(ValidationError.outputFieldUnique(aggregation.outputField()));
            }
            if (aggregation.function() != AggregationFunction.COUNT
                    && (aggregation.field() == null || aggregation.field().isBlank())) {
                throw error.apply(ValidationError.aggregationFieldRequired(aggregation.function().name()));
            }
            if (aggregation.function() == AggregationFunction.TOP_N && aggregation.limit() <= 0) {
                throw error.apply(ValidationError.positive("limit"));
            }
        }

        return new AggregateConfig(
                mode,
                groupBy,
                windowType,
                timeMode,
                timeUnit,
                windowSize,
                windowSlide,
                watermarkDelay,
                eventTimeField,
                eventTimeUnit,
                outputMode,
                windowStartField,
                windowEndField,
                countWindowSize,
                aggregations);
    }

    private static List<AggregationSpec> aggregations(JsonNode config, ValidationErrorFactory error) {
        JsonNode aggregations = config == null ? null : config.path("aggregations");
        if (aggregations == null || !aggregations.isArray()) {
            return List.of();
        }

        List<AggregationSpec> result = new ArrayList<>();
        for (JsonNode item : aggregations) {
            AggregationFunction function = parseEnum(text(item, "function", ""), AggregationFunction.class, "function", error);
            result.add(new AggregationSpec(
                    function,
                    text(item, "field", ""),
                    text(item, "outputField", ""),
                    text(item, "sortField", ""),
                    parseEnum(text(item, "sortDirection", "DESC"), SortDirection.class, "sortDirection", error),
                    intValue(item, "limit", intValue(item, "topNSize", 10, error), error)));
        }
        return result;
    }

    private static List<String> textArray(JsonNode config, String fieldName) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || !value.isArray()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (JsonNode item : value) {
            String text = item.asText("").trim();
            if (!text.isBlank()) {
                result.add(text);
            }
        }
        return result;
    }

    private static long longValue(
            JsonNode config,
            String fieldName,
            long fallback,
            ValidationErrorFactory error) {
        JsonNode value = config == null ? null : config.path(fieldName);
        if (value == null || value.isMissingNode() || value.isNull()) {
            return fallback;
        }
        if (value.isTextual()) {
            return parseLongText(value.asText(), fieldName, error);
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong()) {
            throw error.apply(ValidationError.integerRequired(fieldName));
        }
        return value.asLong();
    }

    private static long parseLongText(String value, String fieldName, ValidationErrorFactory error) {
        try {
            return Long.parseLong(value.trim());
        } catch (Exception exception) {
            throw error.apply(ValidationError.integerRequired(fieldName));
        }
    }

    private static int intValue(JsonNode config, String fieldName, int fallback, ValidationErrorFactory error) {
        long value = longValue(config, fieldName, fallback, error);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw error.apply(ValidationError.integerRequired(fieldName));
        }
        return (int) value;
    }

    private static String text(JsonNode config, String fieldName, String fallback) {
        JsonNode value = config == null ? null : config.path(fieldName);
        String text = value == null || value.isMissingNode() || value.isNull() ? fallback : value.asText(fallback);
        return text == null ? "" : text.trim();
    }

    private static <T extends Enum<T>> T parseEnum(
            String value,
            Class<T> enumType,
            String fieldName,
            ValidationErrorFactory error) {
        try {
            return Enum.valueOf(enumType, value.trim().toUpperCase(Locale.ROOT));
        } catch (Exception exception) {
            throw error.apply(ValidationError.unsupportedValue(fieldName, value));
        }
    }

    @FunctionalInterface
    public interface ValidationErrorFactory {
        IllegalArgumentException apply(ValidationError error);
    }

    public record ValidationError(String messageKey, String defaultMessage, Object... args) {

        static ValidationError unsupportedValue(String fieldName, String value) {
            return new ValidationError(
                    "pipeline.validation.aggregate.unsupportedValue",
                    "Aggregate config " + fieldName + " has unsupported value: " + value,
                    fieldName,
                    value);
        }

        static ValidationError groupByRequired() {
            return new ValidationError(
                    "pipeline.validation.aggregate.groupByRequired",
                    "Aggregate config groupBy is required when mode is GROUPED.");
        }

        static ValidationError watermarkDelayNonNegative() {
            return new ValidationError(
                    "pipeline.validation.aggregate.watermarkDelayNonNegative",
                    "Aggregate config watermarkDelay must be greater than or equal to 0.");
        }

        static ValidationError positive(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.aggregate.positive",
                    "Aggregate config " + fieldName + " must be greater than 0.",
                    fieldName);
        }

        static ValidationError windowSlideMaxWindowSize() {
            return new ValidationError(
                    "pipeline.validation.aggregate.windowSlideMaxWindowSize",
                    "Aggregate config windowSlide must be less than or equal to windowSize.");
        }

        static ValidationError aggregationsRequired() {
            return new ValidationError(
                    "pipeline.validation.aggregate.aggregationsRequired",
                    "Aggregate config aggregations must contain at least one item.");
        }

        static ValidationError outputFieldRequired() {
            return new ValidationError(
                    "pipeline.validation.aggregate.outputFieldRequired",
                    "Aggregate config aggregation outputField is required.");
        }

        static ValidationError outputFieldUnique(String outputField) {
            return new ValidationError(
                    "pipeline.validation.aggregate.outputFieldUnique",
                    "Aggregate config aggregation outputField must be unique: " + outputField,
                    outputField);
        }

        static ValidationError aggregationFieldRequired(String function) {
            return new ValidationError(
                    "pipeline.validation.aggregate.aggregationFieldRequired",
                    "Aggregate config aggregation field is required for " + function + ".",
                    function);
        }

        static ValidationError integerRequired(String fieldName) {
            return new ValidationError(
                    "pipeline.validation.aggregate.integerRequired",
                    "Aggregate config " + fieldName + " must be a valid integer.",
                    fieldName);
        }
    }
}
