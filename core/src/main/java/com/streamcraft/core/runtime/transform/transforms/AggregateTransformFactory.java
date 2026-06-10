package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.aggregation.AggregateConfig;
import com.streamcraft.shared.aggregation.AggregateConfig.AggregationFunction;
import com.streamcraft.shared.aggregation.AggregateConfig.EventTimeUnit;
import com.streamcraft.shared.aggregation.AggregateConfig.OutputMode;
import com.streamcraft.shared.aggregation.AggregateConfig.SortDirection;
import com.streamcraft.shared.aggregation.AggregateConfig.TimeMode;
import com.streamcraft.shared.aggregation.AggregateConfig.TimeUnit;
import com.streamcraft.shared.aggregation.AggregateConfig.WindowType;
import com.streamcraft.shared.aggregation.AggregateConfigParser;
import com.streamcraft.shared.fields.FieldPathSupport;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AggregateTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        RuntimeAggregateConfig config = RuntimeAggregateConfig.from(
                AggregateConfigParser.parse(node.config(), IllegalArgumentException::new));
        AggregateContext context = new AggregateContext(node.id(), config);
        DataStream<DataEntity> stream = config.timeMode() == TimeMode.EVENT_TIME && config.timeWindow()
                ? input.assignTimestampsAndWatermarks(watermarkStrategy(config))
                : input;

        if (config.windowType() == WindowType.COUNT) {
            return TransformOutputs.single(applyCountWindow(stream, context));
        }
        return TransformOutputs.single(applyTimeWindow(stream, context));
    }

    private static DataStream<DataEntity> applyCountWindow(DataStream<DataEntity> input, AggregateContext context) {
        AggregateFunction<DataEntity, MetricsAccumulator, MetricsAccumulator> aggregateFunction =
                new MetricsAggregateFunction(context.config());
        if (context.config().grouped()) {
            return input.keyBy(new GroupKeySelector(context.config()))
                    .countWindow(context.config().countWindowSize())
                    .aggregate(aggregateFunction, new GroupedGlobalWindowProcessFunction(context));
        }
        return input.countWindowAll(context.config().countWindowSize())
                .aggregate(aggregateFunction, new GlobalWindowProcessFunction(context));
    }

    private static DataStream<DataEntity> applyTimeWindow(DataStream<DataEntity> input, AggregateContext context) {
        AggregateFunction<DataEntity, MetricsAccumulator, MetricsAccumulator> aggregateFunction =
                new MetricsAggregateFunction(context.config());
        Duration windowSize = duration(context.config().windowSize(), context.config().timeUnit());
        Duration windowSlide = duration(context.config().windowSlide(), context.config().timeUnit());

        if (context.config().grouped()) {
            if (context.config().windowType() == WindowType.SLIDING_TIME) {
                if (context.config().timeMode() == TimeMode.EVENT_TIME) {
                    return input.keyBy(new GroupKeySelector(context.config()))
                            .window(SlidingEventTimeWindows.of(windowSize, windowSlide))
                            .aggregate(aggregateFunction, new GroupedTimeWindowProcessFunction(context));
                }
                return input.keyBy(new GroupKeySelector(context.config()))
                        .window(SlidingProcessingTimeWindows.of(windowSize, windowSlide))
                        .aggregate(aggregateFunction, new GroupedTimeWindowProcessFunction(context));
            }
            if (context.config().timeMode() == TimeMode.EVENT_TIME) {
                return input.keyBy(new GroupKeySelector(context.config()))
                        .window(TumblingEventTimeWindows.of(windowSize))
                        .aggregate(aggregateFunction, new GroupedTimeWindowProcessFunction(context));
            }
            return input.keyBy(new GroupKeySelector(context.config()))
                    .window(TumblingProcessingTimeWindows.of(windowSize))
                    .aggregate(aggregateFunction, new GroupedTimeWindowProcessFunction(context));
        }

        if (context.config().windowType() == WindowType.SLIDING_TIME) {
            if (context.config().timeMode() == TimeMode.EVENT_TIME) {
                return input.windowAll(SlidingEventTimeWindows.of(windowSize, windowSlide))
                        .aggregate(aggregateFunction, new AllTimeWindowProcessFunction(context));
            }
            return input.windowAll(SlidingProcessingTimeWindows.of(windowSize, windowSlide))
                    .aggregate(aggregateFunction, new AllTimeWindowProcessFunction(context));
        }
        if (context.config().timeMode() == TimeMode.EVENT_TIME) {
            return input.windowAll(TumblingEventTimeWindows.of(windowSize))
                    .aggregate(aggregateFunction, new AllTimeWindowProcessFunction(context));
        }
        return input.windowAll(TumblingProcessingTimeWindows.of(windowSize))
                .aggregate(aggregateFunction, new AllTimeWindowProcessFunction(context));
    }

    private static WatermarkStrategy<DataEntity> watermarkStrategy(RuntimeAggregateConfig config) {
        return WatermarkStrategy
                .<DataEntity>forBoundedOutOfOrderness(Duration.ofMillis(toMillis(config.watermarkDelay(), config.timeUnit())))
                .withTimestampAssigner((SerializableTimestampAssigner<DataEntity>)
                        (element, recordTimestamp) -> eventTimestamp(element, config));
    }

    private static long eventTimestamp(DataEntity element, RuntimeAggregateConfig config) {
        if (config.eventTimePath().path().isBlank()) {
            return element.timestamp();
        }
        Long timestamp = parseEventTimestamp(rawValue(element, config.eventTimePath()), config.eventTimeUnit());
        return timestamp == null ? element.timestamp() : timestamp;
    }

    private static Long parseEventTimestamp(Object value, EventTimeUnit unit) {
        if (value instanceof Number number) {
            return eventTimeNumber(number.longValue(), unit);
        }
        if (value instanceof String text && !text.isBlank()) {
            String normalized = text.trim();
            try {
                return eventTimeNumber(Long.parseLong(normalized), unit);
            } catch (NumberFormatException ignored) {
                try {
                    return Instant.parse(normalized).toEpochMilli();
                } catch (Exception ignoredAgain) {
                    try {
                        return Timestamp.valueOf(normalized).getTime();
                    } catch (Exception ignoredTimestamp) {
                        return null;
                    }
                }
            }
        }
        return null;
    }

    private static long eventTimeNumber(long value, EventTimeUnit unit) {
        return switch (unit) {
            case MILLISECONDS -> value;
            case SECONDS -> value * 1000L;
        };
    }

    private static Duration duration(long value, TimeUnit unit) {
        return switch (unit) {
            case MILLISECONDS -> Duration.ofMillis(value);
            case SECONDS -> Duration.ofSeconds(value);
            case MINUTES -> Duration.ofMinutes(value);
            case HOURS -> Duration.ofHours(value);
        };
    }

    private static long toMillis(long value, TimeUnit unit) {
        return switch (unit) {
            case MILLISECONDS -> value;
            case SECONDS -> value * 1000L;
            case MINUTES -> value * 60_000L;
            case HOURS -> value * 3_600_000L;
        };
    }

    private static DataEntity output(AggregateContext context, MetricsAccumulator accumulator, Map<String, Object> window) {
        Map<String, Object> fields = new LinkedHashMap<>();
        if (context.config().outputMode() == OutputMode.FLAT) {
            fields.put("windowType", window.get("type"));
            if (window.containsKey("start")) {
                fields.put(context.config().windowStartField(), window.get("start"));
            }
            if (window.containsKey("end")) {
                fields.put(context.config().windowEndField(), window.get("end"));
            }
            if (window.containsKey("size")) {
                fields.put("windowSize", window.get("size"));
            }
            fields.putAll(accumulator.group());
            fields.putAll(accumulator.metrics(context.config().aggregations()));
        } else {
            fields.put("window", window);
            fields.put("group", accumulator.group());
            fields.put("metrics", accumulator.metrics(context.config().aggregations()));
        }

        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("operator", "AGGREGATE");
        headers.put("nodeId", context.nodeId());
        headers.put("windowType", context.config().windowType().name());
        return new DataEntity(outputId(context, accumulator, window),
                System.currentTimeMillis(),
                fields,
                headers);
    }

    private static String outputId(AggregateContext context, MetricsAccumulator accumulator, Map<String, Object> window) {
        if (context.config().timeWindow()) {
            return "aggregate:%s:%s:%s:%s".formatted(
                    context.nodeId(),
                    window.get("start"),
                    window.get("end"),
                    groupIdentity(accumulator.group()));
        }
        return "aggregate:%s:count:%s".formatted(context.nodeId(), UUID.randomUUID());
    }

    private static String groupIdentity(Map<String, Object> group) {
        if (group.isEmpty()) {
            return "global";
        }
        StringBuilder builder = new StringBuilder();
        group.forEach((key, value) -> builder.append(key.length())
                .append(':')
                .append(key)
                .append('=')
                .append(value == null ? "null" : value.getClass().getName())
                .append('#')
                .append(value == null ? -1 : Objects.toString(value).length())
                .append(':')
                .append(Objects.toString(value, ""))
                .append(';'));
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(builder.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static Map<String, Object> countWindow(AggregateContext context) {
        Map<String, Object> window = new LinkedHashMap<>();
        window.put("type", context.config().windowType().name());
        window.put("size", context.config().countWindowSize());
        return window;
    }

    private static Map<String, Object> timeWindow(AggregateContext context, TimeWindow timeWindow) {
        Map<String, Object> window = new LinkedHashMap<>();
        window.put("type", context.config().windowType().name());
        window.put("timeMode", context.config().timeMode().name());
        window.put("start", timeWindow.getStart());
        window.put("end", timeWindow.getEnd());
        return window;
    }

    private record AggregateContext(String nodeId, RuntimeAggregateConfig config) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    private record RuntimeAggregateConfig(
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

        private boolean timeWindow() {
            return windowType == WindowType.TUMBLING_TIME || windowType == WindowType.SLIDING_TIME;
        }
    }

    private record RuntimeAggregationSpec(
            AggregationFunction function,
            RuntimeFieldPath fieldPath,
            String outputField,
            RuntimeFieldPath sortPath,
            SortDirection sortDirection,
            int limit) implements Serializable {

        private static final long serialVersionUID = 1L;
    }

    private record RuntimeFieldPath(String path, List<String> segments) implements Serializable {

        private static final long serialVersionUID = 1L;

        private static RuntimeFieldPath from(String path) {
            String normalized = path == null ? "" : path.trim();
            List<String> segments = normalized.contains(".")
                    ? Arrays.asList(normalized.split("\\.", -1))
                    : List.of();
            return new RuntimeFieldPath(normalized, segments);
        }

        private FieldPathSupport.Lookup lookup(Map<String, Object> fields) {
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

    private static final class GroupKey implements Serializable {

        private static final long serialVersionUID = 1L;
        private final Map<String, Object> values;

        private GroupKey(Map<String, Object> values) {
            this.values = new LinkedHashMap<>(values);
        }

        private Map<String, Object> values() {
            return values;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof GroupKey otherKey && values.equals(otherKey.values);
        }

        @Override
        public int hashCode() {
            return values.hashCode();
        }

        @Override
        public String toString() {
            return values.toString();
        }
    }

    private static final class GroupKeySelector implements KeySelector<DataEntity, GroupKey> {

        private static final long serialVersionUID = 1L;
        private final RuntimeAggregateConfig config;

        private GroupKeySelector(RuntimeAggregateConfig config) {
            this.config = config;
        }

        @Override
        public GroupKey getKey(DataEntity entity) {
            Map<String, Object> group = new LinkedHashMap<>();
            for (RuntimeFieldPath path : config.groupPaths()) {
                FieldPathSupport.Lookup lookup = path.lookup(entity.fields());
                group.put(path.path(), lookup.found() ? lookup.value() : null);
            }
            return new GroupKey(group);
        }
    }

    private static final class MetricsAggregateFunction
            implements AggregateFunction<DataEntity, MetricsAccumulator, MetricsAccumulator> {

        private static final long serialVersionUID = 1L;
        private final RuntimeAggregateConfig config;

        private MetricsAggregateFunction(RuntimeAggregateConfig config) {
            this.config = config;
        }

        @Override
        public MetricsAccumulator createAccumulator() {
            return new MetricsAccumulator();
        }

        @Override
        public MetricsAccumulator add(DataEntity value, MetricsAccumulator accumulator) {
            return accumulator.add(value, config);
        }

        @Override
        public MetricsAccumulator getResult(MetricsAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public MetricsAccumulator merge(MetricsAccumulator first, MetricsAccumulator second) {
            return first.merge(second);
        }
    }

    private static final class MetricsAccumulator implements Serializable {

        private static final long serialVersionUID = 1L;
        private Map<String, Object> group = Map.of();
        private Map<String, MetricValue> metrics = new LinkedHashMap<>();

        private MetricsAccumulator add(DataEntity entity, RuntimeAggregateConfig config) {
            for (RuntimeAggregationSpec spec : config.aggregations()) {
                MetricValue metric = metrics.computeIfAbsent(spec.outputField(), ignored -> new MetricValue());
                if (spec.function() == AggregationFunction.COUNT) {
                    metric.count++;
                    continue;
                }
                Object rawValue = rawValue(entity, spec.fieldPath());
                if (rawValue == null) {
                    continue;
                }
                switch (spec.function()) {
                    case SUM, AVG, MIN, MAX -> {
                        Double number = numeric(rawValue);
                        if (number != null) {
                            metric.add(number);
                        }
                    }
                    case COUNT_DISTINCT -> metric.distinctValues.add(rawValue);
                    case FIRST_VALUE -> {
                        if (!metric.hasFirstValue) {
                            metric.firstValue = rawValue;
                            metric.hasFirstValue = true;
                        }
                    }
                    case LAST_VALUE -> {
                        metric.lastValue = rawValue;
                        metric.hasLastValue = true;
                    }
                    case TOP_N -> {
                        Object sortValue = spec.sortPath().path().isBlank()
                                ? rawValue
                                : rawValue(entity, spec.sortPath());
                        metric.topValues.add(new TopValue(rawValue, sortValue == null ? rawValue : sortValue));
                    }
                    case COLLECT_LIST -> metric.listValues.add(rawValue);
                    case COLLECT_SET -> metric.setValues.add(rawValue);
                    case COUNT -> {
                    }
                }
            }
            return this;
        }

        private MetricsAccumulator merge(MetricsAccumulator other) {
            if (group.isEmpty()) {
                group = other.group;
            }
            other.metrics.forEach((key, value) -> metrics.merge(key, value, MetricValue::merge));
            return this;
        }

        private Map<String, Object> group() {
            return group;
        }

        private Map<String, Object> metrics(List<RuntimeAggregationSpec> specs) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (RuntimeAggregationSpec spec : specs) {
                MetricValue metric = metrics.getOrDefault(spec.outputField(), new MetricValue());
                result.put(spec.outputField(), metric.result(spec));
            }
            return result;
        }
    }

    private static final class MetricValue implements Serializable {

        private static final long serialVersionUID = 1L;
        private long count;
        private double sum;
        private Double min;
        private Double max;
        private Set<Object> distinctValues = new HashSet<>();
        private boolean hasFirstValue;
        private Object firstValue;
        private boolean hasLastValue;
        private Object lastValue;
        private List<TopValue> topValues = new ArrayList<>();
        private List<Object> listValues = new ArrayList<>();
        private Set<Object> setValues = new LinkedHashSet<>();

        private void add(double value) {
            count++;
            sum += value;
            min = min == null ? value : Math.min(min, value);
            max = max == null ? value : Math.max(max, value);
        }

        private MetricValue merge(MetricValue other) {
            count += other.count;
            sum += other.sum;
            if (other.min != null) {
                min = min == null ? other.min : Math.min(min, other.min);
            }
            if (other.max != null) {
                max = max == null ? other.max : Math.max(max, other.max);
            }
            distinctValues.addAll(other.distinctValues);
            if (!hasFirstValue && other.hasFirstValue) {
                firstValue = other.firstValue;
                hasFirstValue = true;
            }
            if (other.hasLastValue) {
                lastValue = other.lastValue;
                hasLastValue = true;
            }
            topValues.addAll(other.topValues);
            listValues.addAll(other.listValues);
            setValues.addAll(other.setValues);
            return this;
        }

        private Object result(RuntimeAggregationSpec spec) {
            return switch (spec.function()) {
                case COUNT -> count;
                case SUM -> sum;
                case AVG -> count == 0 ? null : sum / count;
                case MIN -> min;
                case MAX -> max;
                case COUNT_DISTINCT -> (long) distinctValues.size();
                case FIRST_VALUE -> hasFirstValue ? firstValue : null;
                case LAST_VALUE -> hasLastValue ? lastValue : null;
                case TOP_N -> topValues.stream()
                        .sorted((first, second) -> compareTopValues(first, second, spec.sortDirection()))
                        .limit(spec.limit())
                        .map(TopValue::value)
                        .toList();
                case COLLECT_LIST -> List.copyOf(listValues);
                case COLLECT_SET -> List.copyOf(setValues);
            };
        }
    }

    private record TopValue(Object value, Object sortValue) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    private static Object rawValue(DataEntity entity, RuntimeFieldPath field) {
        FieldPathSupport.Lookup lookup = field.lookup(entity.fields());
        if (!lookup.found() || lookup.value() == null) {
            return null;
        }
        return lookup.value();
    }

    private static Double numeric(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String text) {
            try {
                return new BigDecimal(text.trim()).doubleValue();
            } catch (NumberFormatException exception) {
                return null;
            }
        }
        return null;
    }

    private static Comparable<?> sortValue(Object value) {
        Double number = numeric(value);
        if (number != null) {
            return number;
        }
        return Objects.toString(value, "");
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compareDescending(Object first, Object second) {
        Comparable firstValue = sortValue(first);
        Comparable secondValue = sortValue(second);
        return secondValue.compareTo(firstValue);
    }

    private static int compareTopValues(TopValue first, TopValue second, SortDirection direction) {
        int descending = compareDescending(first.sortValue(), second.sortValue());
        return direction == SortDirection.ASC ? -descending : descending;
    }

    private static final class GroupedGlobalWindowProcessFunction
            extends ProcessWindowFunction<MetricsAccumulator, DataEntity, GroupKey, GlobalWindow> {

        private static final long serialVersionUID = 1L;
        private final AggregateContext aggregateContext;

        private GroupedGlobalWindowProcessFunction(AggregateContext aggregateContext) {
            this.aggregateContext = aggregateContext;
        }

        @Override
        public void process(
                GroupKey key,
                Context context,
                Iterable<MetricsAccumulator> elements,
                Collector<DataEntity> out) {
            MetricsAccumulator accumulator = elements.iterator().next();
            accumulator.group = key.values();
            out.collect(output(aggregateContext, accumulator, countWindow(aggregateContext)));
        }
    }

    private static final class GlobalWindowProcessFunction
            extends ProcessAllWindowFunction<MetricsAccumulator, DataEntity, GlobalWindow> {

        private static final long serialVersionUID = 1L;
        private final AggregateContext aggregateContext;

        private GlobalWindowProcessFunction(AggregateContext aggregateContext) {
            this.aggregateContext = aggregateContext;
        }

        @Override
        public void process(
                Context context,
                Iterable<MetricsAccumulator> elements,
                Collector<DataEntity> out) {
            out.collect(output(aggregateContext, elements.iterator().next(), countWindow(aggregateContext)));
        }
    }

    private static final class GroupedTimeWindowProcessFunction
            extends ProcessWindowFunction<MetricsAccumulator, DataEntity, GroupKey, TimeWindow> {

        private static final long serialVersionUID = 1L;
        private final AggregateContext aggregateContext;

        private GroupedTimeWindowProcessFunction(AggregateContext aggregateContext) {
            this.aggregateContext = aggregateContext;
        }

        @Override
        public void process(
                GroupKey key,
                Context context,
                Iterable<MetricsAccumulator> elements,
                Collector<DataEntity> out) {
            MetricsAccumulator accumulator = elements.iterator().next();
            accumulator.group = key.values();
            out.collect(output(aggregateContext, accumulator, timeWindow(aggregateContext, context.window())));
        }
    }

    private static final class AllTimeWindowProcessFunction
            extends ProcessAllWindowFunction<MetricsAccumulator, DataEntity, TimeWindow> {

        private static final long serialVersionUID = 1L;
        private final AggregateContext aggregateContext;

        private AllTimeWindowProcessFunction(AggregateContext aggregateContext) {
            this.aggregateContext = aggregateContext;
        }

        @Override
        public void process(
                Context context,
                Iterable<MetricsAccumulator> elements,
                Collector<DataEntity> out) {
            out.collect(output(aggregateContext, elements.iterator().next(), timeWindow(aggregateContext, context.window())));
        }
    }
}
