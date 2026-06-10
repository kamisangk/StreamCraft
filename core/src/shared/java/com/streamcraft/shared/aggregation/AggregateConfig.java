package com.streamcraft.shared.aggregation;

import java.util.List;

public record AggregateConfig(
        Mode mode,
        List<String> groupBy,
        WindowType windowType,
        TimeMode timeMode,
        TimeUnit timeUnit,
        long windowSize,
        long windowSlide,
        long watermarkDelay,
        String eventTimeField,
        EventTimeUnit eventTimeUnit,
        OutputMode outputMode,
        String windowStartField,
        String windowEndField,
        long countWindowSize,
        List<AggregationSpec> aggregations) {

    public AggregateConfig {
        groupBy = groupBy == null ? List.of() : List.copyOf(groupBy);
        eventTimeField = eventTimeField == null ? "" : eventTimeField.trim();
        eventTimeUnit = eventTimeUnit == null ? EventTimeUnit.MILLISECONDS : eventTimeUnit;
        outputMode = outputMode == null ? OutputMode.NESTED : outputMode;
        windowStartField = windowStartField == null || windowStartField.isBlank() ? "windowStart" : windowStartField.trim();
        windowEndField = windowEndField == null || windowEndField.isBlank() ? "windowEnd" : windowEndField.trim();
        aggregations = aggregations == null ? List.of() : List.copyOf(aggregations);
    }

    public boolean grouped() {
        return mode == Mode.GROUPED;
    }

    public boolean timeWindow() {
        return windowType == WindowType.TUMBLING_TIME || windowType == WindowType.SLIDING_TIME;
    }

    public enum Mode {
        GLOBAL,
        GROUPED
    }

    public enum WindowType {
        TUMBLING_TIME,
        SLIDING_TIME,
        COUNT
    }

    public enum TimeMode {
        PROCESSING_TIME,
        EVENT_TIME
    }

    public enum TimeUnit {
        MILLISECONDS,
        SECONDS,
        MINUTES,
        HOURS
    }

    public enum EventTimeUnit {
        MILLISECONDS,
        SECONDS
    }

    public enum OutputMode {
        NESTED,
        FLAT
    }

    public enum AggregationFunction {
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX,
        COUNT_DISTINCT,
        FIRST_VALUE,
        LAST_VALUE,
        TOP_N,
        COLLECT_LIST,
        COLLECT_SET
    }

    public enum SortDirection {
        ASC,
        DESC
    }

    public record AggregationSpec(
            AggregationFunction function,
            String field,
            String outputField,
            String sortField,
            SortDirection sortDirection,
            int limit) {

        public AggregationSpec {
            sortField = sortField == null ? "" : sortField.trim();
            sortDirection = sortDirection == null ? SortDirection.DESC : sortDirection;
        }
    }
}
