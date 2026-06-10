package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.fields.FieldPathSupport;
import com.streamcraft.shared.streamjoin.StreamJoinConfig;
import com.streamcraft.shared.streamjoin.StreamJoinConfig.JoinType;
import com.streamcraft.shared.streamjoin.StreamJoinConfig.MissingStrategy;
import com.streamcraft.shared.streamjoin.StreamJoinConfig.TimeMode;
import com.streamcraft.shared.streamjoin.StreamJoinConfig.TimeUnit;
import com.streamcraft.shared.streamjoin.StreamJoinConfigParser;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class StreamJoinTransformFactory implements TransformFactory {

    private static final String LEFT_PORT = "left";
    private static final String RIGHT_PORT = "right";

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        throw new IllegalArgumentException("STREAM_JOIN requires left and right input ports.");
    }

    @Override
    public TransformOutputs apply(Map<String, DataStream<DataEntity>> inputsByPort, PipelineNode node) {
        StreamJoinRuntimeConfig config = StreamJoinRuntimeConfig.from(
                StreamJoinConfigParser.parse(node.config(), IllegalArgumentException::new));
        DataStream<DataEntity> left = TransformFactory.requireInput(inputsByPort, LEFT_PORT);
        DataStream<DataEntity> right = TransformFactory.requireInput(inputsByPort, RIGHT_PORT);

        if (config.timeMode() == TimeMode.EVENT_TIME) {
            WatermarkStrategy<DataEntity> watermarkStrategy = WatermarkStrategy
                    .<DataEntity>forBoundedOutOfOrderness(Duration.ofMillis(config.watermarkDelayMillis()))
                    .withTimestampAssigner((SerializableTimestampAssigner<DataEntity>)
                            (element, recordTimestamp) -> element.timestamp());
            left = left.assignTimestampsAndWatermarks(watermarkStrategy);
            right = right.assignTimestampsAndWatermarks(watermarkStrategy);
        }

        KeyedStream<DataEntity, String> keyedLeft = left.keyBy(new JoinKeySelector(config.leftKeyField()));
        KeyedStream<DataEntity, String> keyedRight = right.keyBy(new JoinKeySelector(config.rightKeyField()));

        DataStream<DataEntity> joined = keyedLeft
                .connect(keyedRight)
                .process(new StreamJoinFunction(node.id(), config))
                .name("stream-join-" + node.id());
        return TransformOutputs.single(joined);
    }

    private static long toMillis(long value, TimeUnit unit) {
        return switch (unit) {
            case MILLISECONDS -> value;
            case SECONDS -> value * 1000L;
            case MINUTES -> value * 60_000L;
            case HOURS -> value * 3_600_000L;
        };
    }

    private record StreamJoinRuntimeConfig(
            String leftKeyField,
            String rightKeyField,
            String targetField,
            JoinType joinType,
            MissingStrategy missingStrategy,
            boolean overwriteTargetField,
            TimeMode timeMode,
            long windowBeforeMillis,
            long windowAfterMillis,
            long watermarkDelayMillis) implements Serializable {

        private static final long serialVersionUID = 1L;

        private static StreamJoinRuntimeConfig from(StreamJoinConfig config) {
            return new StreamJoinRuntimeConfig(
                    config.leftKeyField(),
                    config.rightKeyField(),
                    config.targetField(),
                    config.joinType(),
                    config.missingStrategy(),
                    config.overwriteTargetField(),
                    config.timeMode(),
                    toMillis(config.windowBefore(), config.timeUnit()),
                    toMillis(config.windowAfter(), config.timeUnit()),
                    toMillis(config.watermarkDelay(), config.timeUnit()));
        }
    }

    private static final class JoinKeySelector implements KeySelector<DataEntity, String> {

        private static final long serialVersionUID = 1L;
        private final String fieldPath;

        private JoinKeySelector(String fieldPath) {
            this.fieldPath = fieldPath;
        }

        @Override
        public String getKey(DataEntity entity) {
            FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(entity.fields(), fieldPath);
            return lookup.found() && lookup.value() != null ? String.valueOf(lookup.value()) : "";
        }
    }

    private static final class StreamJoinFunction extends KeyedCoProcessFunction<String, DataEntity, DataEntity, DataEntity> {

        private static final long serialVersionUID = 1L;

        private final String nodeId;
        private final StreamJoinRuntimeConfig config;

        private transient ListState<BufferedJoinRecord> leftState;
        private transient ListState<BufferedJoinRecord> rightState;

        private StreamJoinFunction(String nodeId, StreamJoinRuntimeConfig config) {
            this.nodeId = nodeId;
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) {
            leftState = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "stream-join-left",
                    TypeInformation.of(new TypeHint<BufferedJoinRecord>() {
                    })));
            rightState = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "stream-join-right",
                    TypeInformation.of(new TypeHint<BufferedJoinRecord>() {
                    })));
        }

        @Override
        public void processElement1(DataEntity left, Context context, Collector<DataEntity> out) throws Exception {
            String joinKey = key(left, config.leftKeyField());
            if (joinKey.isBlank()) {
                emitUnmatchedImmediately(left, out);
                return;
            }

            long referenceTime = referenceTime(left, context);
            List<BufferedJoinRecord> leftRecords = snapshot(leftState);
            List<BufferedJoinRecord> rightRecords = snapshot(rightState);

            BufferedJoinRecord currentLeft = BufferedJoinRecord.left(left, referenceTime, referenceTime + config.windowAfterMillis());
            boolean matched = false;
            for (BufferedJoinRecord rightRecord : rightRecords) {
                if (!matches(currentLeft.timestamp(), rightRecord.timestamp())) {
                    continue;
                }
                matched = true;
                out.collect(join(left, rightRecord.entity()));
            }
            currentLeft = currentLeft.withMatched(matched);
            leftRecords.add(currentLeft);
            leftState.update(leftRecords);
            registerTimer(context, currentLeft.expireAt());
        }

        @Override
        public void processElement2(DataEntity right, Context context, Collector<DataEntity> out) throws Exception {
            String joinKey = key(right, config.rightKeyField());
            if (joinKey.isBlank()) {
                return;
            }

            long referenceTime = referenceTime(right, context);
            List<BufferedJoinRecord> leftRecords = snapshot(leftState);
            List<BufferedJoinRecord> rightRecords = snapshot(rightState);
            List<BufferedJoinRecord> updatedLeftRecords = new ArrayList<>(leftRecords.size());

            for (BufferedJoinRecord leftRecord : leftRecords) {
                if (matches(leftRecord.timestamp(), referenceTime)) {
                    out.collect(join(leftRecord.entity(), right));
                    updatedLeftRecords.add(leftRecord.withMatched(true));
                } else {
                    updatedLeftRecords.add(leftRecord);
                }
            }

            rightRecords.add(BufferedJoinRecord.right(right, referenceTime, referenceTime + config.windowBeforeMillis()));
            leftState.update(updatedLeftRecords);
            rightState.update(rightRecords);
            registerTimer(context, referenceTime + config.windowBeforeMillis());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<DataEntity> out) throws Exception {
            List<BufferedJoinRecord> retainedLeft = new ArrayList<>();
            for (BufferedJoinRecord leftRecord : snapshot(leftState)) {
                if (leftRecord.expireAt() > timestamp) {
                    retainedLeft.add(leftRecord);
                    continue;
                }
                if (config.joinType() == JoinType.LEFT && !leftRecord.matched()) {
                    out.collect(unmatched(leftRecord.entity()));
                }
            }

            List<BufferedJoinRecord> retainedRight = new ArrayList<>();
            for (BufferedJoinRecord rightRecord : snapshot(rightState)) {
                if (rightRecord.expireAt() > timestamp) {
                    retainedRight.add(rightRecord);
                }
            }

            leftState.update(retainedLeft);
            rightState.update(retainedRight);
        }

        private long referenceTime(DataEntity entity, Context context) {
            return config.timeMode() == TimeMode.EVENT_TIME
                    ? entity.timestamp()
                    : context.timerService().currentProcessingTime();
        }

        private String key(DataEntity entity, String fieldPath) {
            FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(entity.fields(), fieldPath);
            return lookup.found() && lookup.value() != null ? String.valueOf(lookup.value()) : "";
        }

        private void registerTimer(Context context, long timestamp) {
            if (config.timeMode() == TimeMode.EVENT_TIME) {
                context.timerService().registerEventTimeTimer(timestamp);
                return;
            }
            context.timerService().registerProcessingTimeTimer(timestamp);
        }

        private boolean matches(long leftTimestamp, long rightTimestamp) {
            long lowerBound = leftTimestamp - config.windowBeforeMillis();
            long upperBound = leftTimestamp + config.windowAfterMillis();
            return rightTimestamp >= lowerBound && rightTimestamp <= upperBound;
        }

        private DataEntity join(DataEntity left, DataEntity right) {
            if (!config.overwriteTargetField() && FieldPathSupport.lookup(left.fields(), config.targetField()).found()) {
                return withHeaders(new DataEntity(
                        "stream-join:%s:%s:%s".formatted(nodeId, left.id(), right.id()),
                        Math.max(left.timestamp(), right.timestamp()),
                        left.fields(),
                        left.headers()));
            }

            Map<String, Object> joinedFields = new LinkedHashMap<>(left.fields());
            DataEntity base = new DataEntity(
                    "stream-join:%s:%s:%s".formatted(nodeId, left.id(), right.id()),
                    Math.max(left.timestamp(), right.timestamp()),
                    joinedFields,
                    left.headers());
            return withHeaders(base.withField(config.targetField(), new LinkedHashMap<>(right.fields())));
        }

        private DataEntity unmatched(DataEntity entity) {
            if (config.missingStrategy() == MissingStrategy.KEEP_ORIGINAL) {
                return withHeaders(entity);
            }
            if (!config.overwriteTargetField() && FieldPathSupport.lookup(entity.fields(), config.targetField()).found()) {
                return withHeaders(entity);
            }
            return withHeaders(entity.withField(config.targetField(), null));
        }

        private void emitUnmatchedImmediately(DataEntity entity, Collector<DataEntity> out) {
            if (config.joinType() == JoinType.LEFT) {
                out.collect(unmatched(entity));
            }
        }

        private DataEntity withHeaders(DataEntity entity) {
            Map<String, String> headers = new LinkedHashMap<>(entity.headers());
            headers.put("operator", "STREAM_JOIN");
            headers.put("nodeId", nodeId);
            headers.put("joinType", config.joinType().name());
            return new DataEntity(entity.id(), entity.timestamp(), entity.fields(), headers);
        }

        private List<BufferedJoinRecord> snapshot(ListState<BufferedJoinRecord> state) throws Exception {
            List<BufferedJoinRecord> records = new ArrayList<>();
            for (BufferedJoinRecord record : state.get()) {
                records.add(record);
            }
            return records;
        }
    }

    private record BufferedJoinRecord(
            DataEntity entity,
            long timestamp,
            long expireAt,
            boolean matched) implements Serializable {

        private static final long serialVersionUID = 1L;

        private static BufferedJoinRecord left(DataEntity entity, long timestamp, long expireAt) {
            return new BufferedJoinRecord(entity, timestamp, expireAt, false);
        }

        private static BufferedJoinRecord right(DataEntity entity, long timestamp, long expireAt) {
            return new BufferedJoinRecord(entity, timestamp, expireAt, true);
        }

        private BufferedJoinRecord withMatched(boolean matched) {
            return new BufferedJoinRecord(entity, timestamp, expireAt, matched);
        }
    }
}
