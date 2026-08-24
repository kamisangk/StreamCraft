package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.shared.aggregation.AggregateConfig.EventTimeUnit;
import com.streamcraft.shared.aggregation.AggregateConfig.TimeUnit;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

final class AggregateTimeSupport {

    private AggregateTimeSupport() {
    }

    static WatermarkStrategy<DataEntity> watermarkStrategy(
            AggregateRuntimeConfig.RuntimeAggregateConfig config) {
        return WatermarkStrategy
                .<DataEntity>forBoundedOutOfOrderness(Duration.ofMillis(toMillis(
                        config.watermarkDelay(),
                        config.timeUnit())))
                .withTimestampAssigner((SerializableTimestampAssigner<DataEntity>)
                        (element, recordTimestamp) -> eventTimestamp(element, config));
    }

    static Duration duration(long value, TimeUnit unit) {
        return switch (unit) {
            case MILLISECONDS -> Duration.ofMillis(value);
            case SECONDS -> Duration.ofSeconds(value);
            case MINUTES -> Duration.ofMinutes(value);
            case HOURS -> Duration.ofHours(value);
        };
    }

    private static long eventTimestamp(
            DataEntity element,
            AggregateRuntimeConfig.RuntimeAggregateConfig config) {
        if (config.eventTimePath().path().isBlank()) {
            return element.timestamp();
        }
        Long timestamp = parseEventTimestamp(
                AggregateRuntimeConfig.rawValue(element, config.eventTimePath()),
                config.eventTimeUnit());
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

    private static long toMillis(long value, TimeUnit unit) {
        return switch (unit) {
            case MILLISECONDS -> value;
            case SECONDS -> value * 1000L;
            case MINUTES -> value * 60_000L;
            case HOURS -> value * 3_600_000L;
        };
    }
}
