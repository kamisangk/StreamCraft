package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.fields.FieldPathSupport;
import com.streamcraft.shared.timederive.TimeDeriveConfig;
import com.streamcraft.shared.timederive.TimeDeriveConfig.Derivation;
import com.streamcraft.shared.timederive.TimeDeriveConfig.ParseErrorStrategy;
import com.streamcraft.shared.timederive.TimeDeriveConfig.SourceFormat;
import com.streamcraft.shared.timederive.TimeDeriveConfigParser;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.List;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class TimeDeriveTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        TimeDeriveConfig config = TimeDeriveConfigParser.parse(node.config(), IllegalArgumentException::new);
        return TransformOutputs.single(input
                .map(new TimeDeriveMapFunction(
                        config.sourceField(),
                        config.sourceFormat(),
                        config.sourcePattern(),
                        config.sourceTimeZone(),
                        config.outputTimeZone(),
                        config.parseErrorStrategy(),
                        config.derivations()))
                .name(node.name()));
    }

    private static final class TimeDeriveMapFunction extends RichMapFunction<DataEntity, DataEntity> {

        private static final long serialVersionUID = 1L;
        private final String sourceField;
        private final SourceFormat sourceFormat;
        private final String sourcePattern;
        private final String sourceTimeZone;
        private final String outputTimeZone;
        private final ParseErrorStrategy parseErrorStrategy;
        private final List<Derivation> derivations;

        private TimeDeriveMapFunction(
                String sourceField,
                SourceFormat sourceFormat,
                String sourcePattern,
                String sourceTimeZone,
                String outputTimeZone,
                ParseErrorStrategy parseErrorStrategy,
                List<Derivation> derivations) {
            this.sourceField = sourceField;
            this.sourceFormat = sourceFormat;
            this.sourcePattern = sourcePattern;
            this.sourceTimeZone = sourceTimeZone;
            this.outputTimeZone = outputTimeZone;
            this.parseErrorStrategy = parseErrorStrategy;
            this.derivations = derivations;
        }

        @Override
        public DataEntity map(DataEntity entity) {
            FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(entity.fields(), sourceField);
            if (!lookup.found()) {
                return handleParseFailure(entity, null);
            }

            ZonedDateTime timestamp = parseTimestamp(lookup.value());
            if (timestamp == null) {
                return handleParseFailure(entity, lookup.value());
            }

            DataEntity result = entity;
            for (Derivation derivation : derivations) {
                Object value = derive(timestamp, derivation);
                result = result.withField(derivation.outputField(), value);
            }
            return result;
        }

        private DataEntity handleParseFailure(DataEntity entity, Object value) {
            if (parseErrorStrategy == ParseErrorStrategy.FAIL) {
                throw new IllegalArgumentException("Failed to parse time field " + sourceField + ": " + value);
            }
            if (parseErrorStrategy == ParseErrorStrategy.SET_NULL) {
                DataEntity result = entity;
                for (Derivation derivation : derivations) {
                    result = result.withField(derivation.outputField(), null);
                }
                return result;
            }
            return entity;
        }

        private ZonedDateTime parseTimestamp(Object value) {
            try {
                ZoneId sourceZone = ZoneId.of(sourceTimeZone);
                ZoneId outputZone = ZoneId.of(outputTimeZone);
                if (value instanceof Number number) {
                    long epoch = number.longValue();
                    long epochMillis = sourceFormat == SourceFormat.EPOCH_SECONDS ? epoch * 1000L : epoch;
                    return Instant.ofEpochMilli(epochMillis).atZone(outputZone);
                }

                String text = String.valueOf(value == null ? "" : value).trim();
                if (text.isBlank()) {
                    return null;
                }
                return switch (sourceFormat) {
                    case EPOCH_MILLIS -> Instant.ofEpochMilli(Long.parseLong(text)).atZone(outputZone);
                    case EPOCH_SECONDS -> Instant.ofEpochMilli(Long.parseLong(text) * 1000L).atZone(outputZone);
                    case PATTERN -> LocalDateTime.parse(text, DateTimeFormatter.ofPattern(sourcePattern))
                            .atZone(sourceZone)
                            .withZoneSameInstant(outputZone);
                    case ISO, AUTO -> parseIsoOrEpoch(text, outputZone);
                };
            } catch (Exception exception) {
                return null;
            }
        }

        private ZonedDateTime parseIsoOrEpoch(String text, ZoneId zone) {
            try {
                return Instant.parse(text).atZone(zone);
            } catch (Exception ignored) {
                return Instant.ofEpochMilli(Long.parseLong(text)).atZone(zone);
            }
        }

        private Object derive(ZonedDateTime timestamp, Derivation derivation) {
            return switch (derivation.type()) {
                case DATE -> timestamp.toLocalDate().toString();
                case DATETIME -> timestamp.toLocalDateTime().toString();
                case YEAR -> timestamp.getYear();
                case MONTH -> timestamp.getMonthValue();
                case DAY -> timestamp.getDayOfMonth();
                case HOUR -> timestamp.getHour();
                case MINUTE -> timestamp.getMinute();
                case SECOND -> timestamp.getSecond();
                case WEEK -> timestamp.get(WeekFields.ISO.weekOfWeekBasedYear());
                case QUARTER -> ((timestamp.getMonthValue() - 1) / 3) + 1;
                case DAY_OF_WEEK -> timestamp.getDayOfWeek().getValue();
                case DAY_OF_YEAR -> timestamp.getDayOfYear();
                case EPOCH_MILLIS -> timestamp.toInstant().toEpochMilli();
                case EPOCH_SECONDS -> timestamp.toInstant().getEpochSecond();
                case FORMAT -> DateTimeFormatter.ofPattern(derivation.pattern()).format(timestamp);
            };
        }
    }
}
