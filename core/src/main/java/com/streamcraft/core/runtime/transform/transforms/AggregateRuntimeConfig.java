package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.aggregation.AggregateConfig;
import com.streamcraft.shared.aggregation.AggregateConfig.AggregationFunction;
import com.streamcraft.shared.aggregation.AggregateConfig.EventTimeUnit;
import com.streamcraft.shared.aggregation.AggregateConfig.OutputMode;
import com.streamcraft.shared.aggregation.AggregateConfig.SortDirection;
import com.streamcraft.shared.aggregation.AggregateConfig.TimeMode;
import com.streamcraft.shared.aggregation.AggregateConfig.TimeUnit;
import com.streamcraft.shared.aggregation.AggregateConfig.WindowType;
import com.streamcraft.shared.fields.FieldPathSupport;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

final class AggregateRuntimeConfig {

    private AggregateRuntimeConfig() {
    }

    static RuntimeAggregateConfig from(AggregateConfig config) {
        return RuntimeAggregateConfig.from(config);
    }

    static Object rawValue(DataEntity entity, RuntimeFieldPath field) {
        FieldPathSupport.Lookup lookup = field.lookup(entity.fields());
        if (!lookup.found() || lookup.value() == null) {
            return null;
        }
        return lookup.value();
    }

    record RuntimeAggregateConfig(
            boolean grouped,
            WindowType windowType,
            TimeMode timeMode,
            TimeUnit timeUnit,
            long windowSize,
            long windowSlide,
            long watermarkDelay,
            long countWindowSize,
            RuntimeFieldPath eventTimePath,
            EventTimeUnit eventTimeUnit,
            OutputMode outputMode,
            String windowStartField,
            String windowEndField,
            List<RuntimeFieldPath> groupPaths,
            List<RuntimeAggregationSpec> aggregations) implements Serializable {

        private static final long serialVersionUID = 1L;

        private static RuntimeAggregateConfig from(AggregateConfig config) {
            return new RuntimeAggregateConfig(
                    config.grouped(),
                    config.windowType(),
                    config.timeMode(),
                    config.timeUnit(),
                    config.windowSize(),
                    config.windowSlide(),
                    config.watermarkDelay(),
                    config.countWindowSize(),
                    RuntimeFieldPath.from(config.eventTimeField()),
                    config.eventTimeUnit(),
                    config.outputMode(),
                    config.windowStartField(),
                    config.windowEndField(),
                    config.groupBy().stream()
                            .map(RuntimeFieldPath::from)
                            .toList(),
                    config.aggregations().stream()
                            .map(spec -> new RuntimeAggregationSpec(
                                    spec.function(),
                                    RuntimeFieldPath.from(spec.field()),
                                    spec.outputField(),
                                    RuntimeFieldPath.from(spec.sortField()),
                                    spec.sortDirection(),
                                    spec.limit()))
                            .toList());
        }

        boolean timeWindow() {
            return windowType == WindowType.TUMBLING_TIME || windowType == WindowType.SLIDING_TIME;
        }
    }

    record RuntimeAggregationSpec(
            AggregationFunction function,
            RuntimeFieldPath fieldPath,
            String outputField,
            RuntimeFieldPath sortPath,
            SortDirection sortDirection,
            int limit) implements Serializable {

        private static final long serialVersionUID = 1L;
    }

    record RuntimeFieldPath(String path, List<String> segments) implements Serializable {

        private static final long serialVersionUID = 1L;

        private static RuntimeFieldPath from(String path) {
            String normalized = path == null ? "" : path.trim();
            List<String> segments = normalized.contains(".")
                    ? Arrays.asList(normalized.split("\\.", -1))
                    : List.of();
            return new RuntimeFieldPath(normalized, segments);
        }

        FieldPathSupport.Lookup lookup(Map<String, Object> fields) {
            if (fields == null || path.isBlank()) {
                return FieldPathSupport.Lookup.notFound();
            }
            if (fields.containsKey(path)) {
                return FieldPathSupport.Lookup.found(fields.get(path));
            }
            if (segments.isEmpty()) {
                return FieldPathSupport.Lookup.notFound();
            }

            Object current = fields;
            for (String segment : segments) {
                if (segment.isEmpty() || !(current instanceof Map<?, ?> currentMap)) {
                    return FieldPathSupport.Lookup.notFound();
                }
                if (!currentMap.containsKey(segment)) {
                    return FieldPathSupport.Lookup.notFound();
                }
                current = currentMap.get(segment);
            }
            return FieldPathSupport.Lookup.found(current);
        }
    }
}
