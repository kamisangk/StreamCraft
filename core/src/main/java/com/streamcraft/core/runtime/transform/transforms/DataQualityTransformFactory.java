package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.dataquality.DataQualityConfig;
import com.streamcraft.shared.dataquality.DataQualityConfig.Mode;
import com.streamcraft.shared.dataquality.DataQualityConfig.Rule;
import com.streamcraft.shared.dataquality.DataQualityConfig.ValueType;
import com.streamcraft.shared.dataquality.DataQualityConfigParser;
import com.streamcraft.shared.fields.FieldPathSupport;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DataQualityTransformFactory implements TransformFactory {

    private static final String CLEAN_PORT = "output-0";
    private static final String DIRTY_PORT = "dirty";

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        DataQualityConfig config = DataQualityConfigParser.parse(node.config(), IllegalArgumentException::new);
        OutputTag<DataEntity> dirtyTag = new OutputTag<>(node.id() + "-dirty") {
            private static final long serialVersionUID = 1L;
        };

        SingleOutputStreamOperator<DataEntity> cleanStream = input.process(new DataQualityProcessFunction(config, dirtyTag))
                .name(node.name());

        return new TransformOutputs(Map.of(
                CLEAN_PORT, cleanStream,
                DIRTY_PORT, cleanStream.getSideOutput(dirtyTag)));
    }

    private static final class DataQualityProcessFunction extends ProcessFunction<DataEntity, DataEntity> {

        private static final long serialVersionUID = 1L;
        private final DataQualityConfig config;
        private final OutputTag<DataEntity> dirtyTag;
        private transient List<CompiledRule> compiledRules;

        private DataQualityProcessFunction(DataQualityConfig config, OutputTag<DataEntity> dirtyTag) {
            this.config = config;
            this.dirtyTag = dirtyTag;
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            compiledRules = config.rules().stream().map(CompiledRule::new).toList();
        }

        @Override
        public void processElement(
                DataEntity entity,
                ProcessFunction<DataEntity, DataEntity>.Context context,
                Collector<DataEntity> collector) {
            List<String> violations = validate(entity, compiledRules);
            if (violations.isEmpty()) {
                collector.collect(entity);
                return;
            }

            DataEntity enriched = entity.withField(config.errorField(), violations);
            if (config.mode() == Mode.DISCARD) {
                return;
            }
            if (config.mode() == Mode.MARK_ERROR) {
                collector.collect(enriched);
                return;
            }
            if (config.mode() == Mode.FAIL) {
                throw new IllegalArgumentException("Data quality validation failed: " + String.join("; ", violations));
            }
            context.output(dirtyTag, enriched);
        }

        private List<String> validate(DataEntity entity, List<CompiledRule> rules) {
            List<String> violations = new ArrayList<>();
            for (CompiledRule rule : rules) {
                violations.addAll(rule.validate(entity));
            }
            return violations;
        }
    }

    private static final class CompiledRule implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Rule rule;
        private final Pattern pattern;

        private CompiledRule(Rule rule) {
            this.rule = rule;
            this.pattern = rule.pattern() == null || rule.pattern().isBlank() ? null : Pattern.compile(rule.pattern());
        }

        private List<String> validate(DataEntity entity) {
            FieldPathSupport.Lookup lookup = FieldPathSupport.lookup(entity.fields(), rule.field());
            if (rule.ruleType() == DataQualityConfig.RuleType.NOT_NULL) {
                if (!lookup.found() || lookup.value() == null || isBlankString(lookup.value())) {
                    return violation(rule.field() + " is required");
                }
                return List.of();
            }
            if (!lookup.found() || lookup.value() == null || isBlankString(lookup.value())) {
                return List.of();
            }

            Object value = lookup.value();
            return switch (rule.ruleType()) {
                case TYPE -> validateType(value);
                case RANGE -> validateRange(value);
                case LENGTH -> validateLength(value);
                case ENUM -> validateEnum(value);
                case REGEX -> validateRegex(value);
                case NOT_NULL -> List.of();
            };
        }

        private List<String> validateType(Object value) {
            if (rule.valueType() != null && !matchesType(value, rule.valueType())) {
                return violation(rule.field() + " must be of type " + rule.valueType());
            }
            return List.of();
        }

        private List<String> validateRange(Object value) {
            Double numericValue = numericValue(value);
            if (numericValue == null) {
                return violation(rule.field() + " must be numeric");
            }
            List<String> violations = new ArrayList<>();
            if (rule.min() != null && numericValue < rule.min()) {
                violations.add(message(rule.field() + " must be greater than or equal to " + rule.min()));
            }
            if (rule.max() != null && numericValue > rule.max()) {
                violations.add(message(rule.field() + " must be less than or equal to " + rule.max()));
            }
            return violations;
        }

        private List<String> validateLength(Object value) {
            int length = String.valueOf(value).length();
            List<String> violations = new ArrayList<>();
            if (rule.minLength() != null && length < rule.minLength()) {
                violations.add(message(rule.field() + " length must be greater than or equal to " + rule.minLength()));
            }
            if (rule.maxLength() != null && length > rule.maxLength()) {
                violations.add(message(rule.field() + " length must be less than or equal to " + rule.maxLength()));
            }
            return violations;
        }

        private List<String> validateEnum(Object value) {
            String text = String.valueOf(value);
            if (!rule.enumValues().contains(text)) {
                return violation(rule.field() + " must be one of " + rule.enumValues());
            }
            return List.of();
        }

        private List<String> validateRegex(Object value) {
            if (pattern != null && !pattern.matcher(String.valueOf(value)).matches()) {
                return violation(rule.field() + " does not match the expected pattern");
            }
            return List.of();
        }

        private List<String> violation(String fallback) {
            return List.of(message(fallback));
        }

        private String message(String fallback) {
            return rule.customMessage() == null || rule.customMessage().isBlank() ? fallback : rule.customMessage();
        }

        private boolean matchesType(Object value, ValueType type) {
            return switch (type) {
                case STRING -> value instanceof CharSequence;
                case NUMBER -> numericValue(value) != null;
                case INTEGER, LONG -> integralValue(value) != null;
                case DOUBLE -> numericValue(value) != null;
                case BOOLEAN -> value instanceof Boolean || isBooleanText(value);
                case ARRAY -> value instanceof List<?> || value != null && value.getClass().isArray();
                case OBJECT -> value instanceof Map<?, ?>;
            };
        }

        private Double numericValue(Object value) {
            if (value instanceof Number number) {
                return number.doubleValue();
            }
            if (value instanceof CharSequence text) {
                try {
                    return Double.parseDouble(text.toString().trim());
                } catch (Exception exception) {
                    return null;
                }
            }
            return null;
        }

        private Long integralValue(Object value) {
            Double numeric = numericValue(value);
            if (numeric == null) {
                return null;
            }
            long longValue = numeric.longValue();
            return Math.abs(numeric - longValue) < 0.0000001d ? longValue : null;
        }

        private boolean isBooleanText(Object value) {
            if (!(value instanceof CharSequence text)) {
                return false;
            }
            String normalized = text.toString().trim().toLowerCase();
            return "true".equals(normalized) || "false".equals(normalized);
        }

        private boolean isBlankString(Object value) {
            return value instanceof CharSequence text && text.toString().isBlank();
        }
    }
}
