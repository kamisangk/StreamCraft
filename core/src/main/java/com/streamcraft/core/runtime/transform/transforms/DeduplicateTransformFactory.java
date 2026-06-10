package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.deduplication.DeduplicateConfig;
import com.streamcraft.shared.deduplication.DeduplicateConfig.KeepStrategy;
import com.streamcraft.shared.deduplication.DeduplicateConfig.TimeMode;
import com.streamcraft.shared.deduplication.DeduplicateConfigParser;
import com.streamcraft.shared.fields.FieldPathSupport;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DeduplicateTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        DeduplicateConfig config = DeduplicateConfigParser.parse(node.config(), IllegalArgumentException::new);
        if (config.timeMode() == TimeMode.EVENT_TIME) {
            return TransformOutputs.single(applyEventTime(input, node, config));
        }
        DataStream<DataEntity> stream = input
                .keyBy(new DeduplicateKeySelector(config.keyFields()))
                .process(new DeduplicateProcessFunction(config.ttlSeconds(), config.keepStrategy()))
                .name(node.name());
        return TransformOutputs.single(stream);
    }

    private static DataStream<DataEntity> applyEventTime(
            DataStream<DataEntity> input,
            PipelineNode node,
            DeduplicateConfig config) {
        return input
                .filter(entity -> eventTimestamp(entity, config.eventTimeField()) != null)
                .assignTimestampsAndWatermarks(watermarkStrategy(config))
                .keyBy(new DeduplicateKeySelector(config.keyFields()))
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(config.windowSeconds())))
                .allowedLateness(Duration.ZERO)
                .process(new EventTimeDeduplicateWindowFunction(config.eventTimeField(), config.keepStrategy()))
                .name(node.name());
    }

    private static WatermarkStrategy<DataEntity> watermarkStrategy(DeduplicateConfig config) {
        return WatermarkStrategy
                .<DataEntity>forBoundedOutOfOrderness(Duration.ofSeconds(config.watermarkDelaySeconds()))
                .withTimestampAssigner((SerializableTimestampAssigner<DataEntity>)
                        (element, recordTimestamp) -> eventTimestamp(element, config.eventTimeField()));
    }

    private static Long eventTimestamp(DataEntity element, String eventTimeField) {
        FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(element.fields(), eventTimeField);
        if (!lookup.found()) {
            return null;
        }
        return parseEventTimestamp(lookup.value());
    }

    private static Long parseEventTimestamp(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            String normalized = text.trim();
            try {
                return Long.parseLong(normalized);
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

    private record DeduplicateKeySelector(List<String> keyFields)
            implements KeySelector<DataEntity, DeduplicateKey> {

        private DeduplicateKeySelector {
            keyFields = List.copyOf(keyFields);
        }

        @Override
        public DeduplicateKey getKey(DataEntity entity) {
            List<KeyPart> parts = new ArrayList<>();
            Map<String, Object> fields = entity.fields();
            for (String field : keyFields) {
                FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(fields, field);
                Object value = lookup.found() ? lookup.value() : null;
                parts.add(new KeyPart(
                        field,
                        lookup.found(),
                        value == null ? "" : value.getClass().getName(),
                        Objects.toString(value, "")));
            }
            return new DeduplicateKey(parts);
        }
    }

    private record DeduplicateKey(List<KeyPart> parts) implements Serializable {

        private static final long serialVersionUID = 1L;

        private DeduplicateKey {
            parts = List.copyOf(parts);
        }
    }

    private record KeyPart(
            String field,
            boolean found,
            String valueType,
            String value) implements Serializable {

        private static final long serialVersionUID = 1L;
    }

    private static final class DeduplicateProcessFunction
            extends KeyedProcessFunction<DeduplicateKey, DataEntity, DataEntity> {

        private static final long serialVersionUID = 1L;
        private final long ttlSeconds;
        private final KeepStrategy keepStrategy;
        private transient ValueState<Boolean> seen;
        private transient ValueState<DataEntity> bufferedLast;
        private transient ValueState<Long> emitTimer;

        private DeduplicateProcessFunction(long ttlSeconds, KeepStrategy keepStrategy) {
            this.ttlSeconds = ttlSeconds;
            this.keepStrategy = keepStrategy == null ? KeepStrategy.FIRST : keepStrategy;
        }

        @Override
        public void open(OpenContext openContext) {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Duration.ofSeconds(ttlSeconds))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("deduplicate-seen", Boolean.class);
            descriptor.enableTimeToLive(ttlConfig);
            seen = getRuntimeContext().getState(descriptor);
            bufferedLast = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("deduplicate-buffered-last", DataEntity.class));
            emitTimer = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("deduplicate-last-emit-timer", Long.class));
        }

        @Override
        public void processElement(
                DataEntity value,
                KeyedProcessFunction<DeduplicateKey, DataEntity, DataEntity>.Context context,
                Collector<DataEntity> out) throws Exception {
            if (keepStrategy == KeepStrategy.LAST) {
                bufferedLast.update(value);
                if (emitTimer.value() == null) {
                    long timer = context.timerService().currentProcessingTime() + ttlSeconds * 1000L;
                    emitTimer.update(timer);
                    context.timerService().registerProcessingTimeTimer(timer);
                }
                return;
            }
            if (Boolean.TRUE.equals(seen.value())) {
                return;
            }
            seen.update(Boolean.TRUE);
            out.collect(value);
        }

        @Override
        public void onTimer(
                long timestamp,
                KeyedProcessFunction<DeduplicateKey, DataEntity, DataEntity>.OnTimerContext context,
                Collector<DataEntity> out) throws Exception {
            if (keepStrategy != KeepStrategy.LAST) {
                return;
            }
            Long timer = emitTimer.value();
            if (timer == null || timer != timestamp) {
                return;
            }
            DataEntity last = bufferedLast.value();
            if (last != null) {
                out.collect(last);
            }
            bufferedLast.clear();
            emitTimer.clear();
            seen.clear();
        }
    }

    private static final class EventTimeDeduplicateWindowFunction
            extends ProcessWindowFunction<DataEntity, DataEntity, DeduplicateKey, TimeWindow> {

        private static final long serialVersionUID = 1L;
        private final String eventTimeField;
        private final KeepStrategy keepStrategy;

        private EventTimeDeduplicateWindowFunction(String eventTimeField, KeepStrategy keepStrategy) {
            this.eventTimeField = eventTimeField;
            this.keepStrategy = keepStrategy == null ? KeepStrategy.EVENT_TIME_LATEST : keepStrategy;
        }

        @Override
        public void process(
                DeduplicateKey key,
                ProcessWindowFunction<DataEntity, DataEntity, DeduplicateKey, TimeWindow>.Context context,
                Iterable<DataEntity> elements,
                Collector<DataEntity> out) {
            DataEntity selected = null;
            Long selectedTimestamp = null;
            for (DataEntity element : elements) {
                if (selected == null || keepStrategy == KeepStrategy.LAST) {
                    selected = element;
                }
                if (keepStrategy == KeepStrategy.EVENT_TIME_LATEST) {
                    Long timestamp = eventTimestamp(element, eventTimeField);
                    if (timestamp != null && (selectedTimestamp == null || timestamp > selectedTimestamp)) {
                        selected = element;
                        selectedTimestamp = timestamp;
                    }
                }
            }
            if (selected != null) {
                out.collect(selected);
            }
        }
    }
}
