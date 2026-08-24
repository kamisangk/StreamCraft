package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.runtime.transform.transforms.AggregateMetricSupport.MetricsAccumulator;
import com.streamcraft.core.runtime.transform.transforms.AggregateTransformFactory.AggregateContext;
import com.streamcraft.core.runtime.transform.transforms.AggregateTransformFactory.GroupKey;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

final class AggregateWindowSupport {

    private AggregateWindowSupport() {
    }

    static final class GroupedGlobalWindowProcessFunction
            extends ProcessWindowFunction<MetricsAccumulator, DataEntity, GroupKey, GlobalWindow> {

        private static final long serialVersionUID = 1L;
        private final AggregateContext aggregateContext;

        GroupedGlobalWindowProcessFunction(AggregateContext aggregateContext) {
            this.aggregateContext = aggregateContext;
        }

        @Override
        public void process(
                GroupKey key,
                Context context,
                Iterable<MetricsAccumulator> elements,
                Collector<DataEntity> out) {
            MetricsAccumulator accumulator = elements.iterator().next();
            accumulator.setGroup(key.values());
            out.collect(AggregateTransformFactory.output(
                    aggregateContext,
                    accumulator,
                    AggregateTransformFactory.countWindow(aggregateContext)));
        }
    }

    static final class GlobalWindowProcessFunction
            extends ProcessAllWindowFunction<MetricsAccumulator, DataEntity, GlobalWindow> {

        private static final long serialVersionUID = 1L;
        private final AggregateContext aggregateContext;

        GlobalWindowProcessFunction(AggregateContext aggregateContext) {
            this.aggregateContext = aggregateContext;
        }

        @Override
        public void process(
                Context context,
                Iterable<MetricsAccumulator> elements,
                Collector<DataEntity> out) {
            out.collect(AggregateTransformFactory.output(
                    aggregateContext,
                    elements.iterator().next(),
                    AggregateTransformFactory.countWindow(aggregateContext)));
        }
    }

    static final class GroupedTimeWindowProcessFunction
            extends ProcessWindowFunction<MetricsAccumulator, DataEntity, GroupKey, TimeWindow> {

        private static final long serialVersionUID = 1L;
        private final AggregateContext aggregateContext;

        GroupedTimeWindowProcessFunction(AggregateContext aggregateContext) {
            this.aggregateContext = aggregateContext;
        }

        @Override
        public void process(
                GroupKey key,
                Context context,
                Iterable<MetricsAccumulator> elements,
                Collector<DataEntity> out) {
            MetricsAccumulator accumulator = elements.iterator().next();
            accumulator.setGroup(key.values());
            out.collect(AggregateTransformFactory.output(
                    aggregateContext,
                    accumulator,
                    AggregateTransformFactory.timeWindow(aggregateContext, context.window())));
        }
    }

    static final class AllTimeWindowProcessFunction
            extends ProcessAllWindowFunction<MetricsAccumulator, DataEntity, TimeWindow> {

        private static final long serialVersionUID = 1L;
        private final AggregateContext aggregateContext;

        AllTimeWindowProcessFunction(AggregateContext aggregateContext) {
            this.aggregateContext = aggregateContext;
        }

        @Override
        public void process(
                Context context,
                Iterable<MetricsAccumulator> elements,
                Collector<DataEntity> out) {
            out.collect(AggregateTransformFactory.output(
                    aggregateContext,
                    elements.iterator().next(),
                    AggregateTransformFactory.timeWindow(aggregateContext, context.window())));
        }
    }
}
