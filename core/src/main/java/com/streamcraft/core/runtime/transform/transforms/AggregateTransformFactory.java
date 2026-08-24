package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.core.runtime.transform.transforms.AggregateMetricSupport.MetricsAccumulator;
import com.streamcraft.core.runtime.transform.transforms.AggregateMetricSupport.MetricsAggregateFunction;
import com.streamcraft.core.runtime.transform.transforms.AggregateRuntimeConfig.RuntimeAggregateConfig;
import com.streamcraft.core.runtime.transform.transforms.AggregateRuntimeConfig.RuntimeFieldPath;
import com.streamcraft.core.runtime.transform.transforms.AggregateWindowSupport.AllTimeWindowProcessFunction;
import com.streamcraft.core.runtime.transform.transforms.AggregateWindowSupport.GlobalWindowProcessFunction;
import com.streamcraft.core.runtime.transform.transforms.AggregateWindowSupport.GroupedGlobalWindowProcessFunction;
import com.streamcraft.core.runtime.transform.transforms.AggregateWindowSupport.GroupedTimeWindowProcessFunction;
import com.streamcraft.shared.aggregation.AggregateConfig.TimeMode;
import com.streamcraft.shared.aggregation.AggregateConfig.WindowType;
import com.streamcraft.shared.aggregation.AggregateConfigParser;
import com.streamcraft.shared.fields.FieldPathSupport;
import java.io.Serializable;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class AggregateTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        RuntimeAggregateConfig config = AggregateRuntimeConfig.from(
                AggregateConfigParser.parse(node.config(), IllegalArgumentException::new));
        AggregateContext context = new AggregateContext(node.id(), config);
        DataStream<DataEntity> stream = config.timeMode() == TimeMode.EVENT_TIME && config.timeWindow()
                ? input.assignTimestampsAndWatermarks(AggregateTimeSupport.watermarkStrategy(config))
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
        Duration windowSize = AggregateTimeSupport.duration(
                context.config().windowSize(),
                context.config().timeUnit());
        Duration windowSlide = AggregateTimeSupport.duration(
                context.config().windowSlide(),
                context.config().timeUnit());

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

    static DataEntity output(AggregateContext context, MetricsAccumulator accumulator, Map<String, Object> window) {
        return AggregateOutputSupport.output(
                context.nodeId(),
                context.config(),
                accumulator.group(),
                accumulator.metrics(context.config().aggregations()),
                window);
    }

    static Map<String, Object> countWindow(AggregateContext context) {
        return AggregateOutputSupport.countWindow(context.config());
    }

    static Map<String, Object> timeWindow(AggregateContext context, TimeWindow timeWindow) {
        return AggregateOutputSupport.timeWindow(context.config(), timeWindow);
    }

    record AggregateContext(String nodeId, RuntimeAggregateConfig config) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    static final class GroupKey implements Serializable {

        private static final long serialVersionUID = 1L;
        private final Map<String, Object> values;

        GroupKey(Map<String, Object> values) {
            this.values = new LinkedHashMap<>(values);
        }

        Map<String, Object> values() {
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

}
