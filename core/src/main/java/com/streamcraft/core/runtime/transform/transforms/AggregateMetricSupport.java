package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.runtime.transform.transforms.AggregateRuntimeConfig.RuntimeAggregateConfig;
import com.streamcraft.core.runtime.transform.transforms.AggregateRuntimeConfig.RuntimeAggregationSpec;
import com.streamcraft.shared.aggregation.AggregateConfig.AggregationFunction;
import com.streamcraft.shared.aggregation.AggregateConfig.SortDirection;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.api.common.functions.AggregateFunction;

final class AggregateMetricSupport {

    private AggregateMetricSupport() {
    }

    static final class MetricsAggregateFunction
            implements AggregateFunction<DataEntity, MetricsAccumulator, MetricsAccumulator> {

        private static final long serialVersionUID = 1L;
        private final RuntimeAggregateConfig config;

        MetricsAggregateFunction(RuntimeAggregateConfig config) {
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

    static final class MetricsAccumulator implements Serializable {

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
                Object rawValue = AggregateRuntimeConfig.rawValue(entity, spec.fieldPath());
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
                                : AggregateRuntimeConfig.rawValue(entity, spec.sortPath());
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

        void setGroup(Map<String, Object> group) {
            this.group = group;
        }

        Map<String, Object> group() {
            return group;
        }

        Map<String, Object> metrics(List<RuntimeAggregationSpec> specs) {
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
}
