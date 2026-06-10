package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.casewhen.CaseWhenConfig;
import com.streamcraft.shared.casewhen.CaseWhenConfig.CaseRule;
import com.streamcraft.shared.casewhen.CaseWhenConfig.ValueSpec;
import com.streamcraft.shared.casewhen.CaseWhenConfigParser;
import com.streamcraft.shared.expression.SafeExpressionSupport;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class CaseWhenTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        CaseWhenConfig config = CaseWhenConfigParser.parse(node.config(), IllegalArgumentException::new);
        return TransformOutputs.single(input
                .map(new CaseWhenMapFunction(config.targetField(), config.cases(), config.defaultValue()))
                .name(node.name()));
    }

    private static final class CaseWhenMapFunction extends RichMapFunction<DataEntity, DataEntity> {

        private static final long serialVersionUID = 1L;
        private final String targetField;
        private final List<CaseRule> cases;
        private final ValueSpec defaultValue;
        private transient List<CompiledCaseRule> compiledCases;
        private transient CompiledValue defaultCompiledValue;

        private CaseWhenMapFunction(String targetField, List<CaseRule> cases, ValueSpec defaultValue) {
            this.targetField = targetField;
            this.cases = cases;
            this.defaultValue = defaultValue;
        }

        @Override
        public void open(OpenContext openContext) {
            compiledCases = cases.stream()
                    .map(rule -> new CompiledCaseRule(
                            SafeExpressionSupport.compile(rule.condition(), "Case when condition"),
                            compileValue(rule.value())))
                    .toList();
            defaultCompiledValue = compileValue(defaultValue);
        }

        @Override
        public DataEntity map(DataEntity entity) {
            for (CompiledCaseRule rule : compiledCases) {
                try {
                    Boolean matched = rule.condition().evaluateBoolean(entity.fields());
                    if (Boolean.TRUE.equals(matched)) {
                        return entity.withField(targetField, rule.value().evaluate(entity));
                    }
                } catch (Exception ignored) {
                    // A failed condition is treated as not matched, matching existing FILTER behavior.
                }
            }
            if (!defaultValue.emptyValue()) {
                return entity.withField(targetField, defaultCompiledValue.evaluate(entity));
            }
            return entity;
        }

        private CompiledValue compileValue(ValueSpec valueSpec) {
            if (valueSpec.expression().isBlank()) {
                return new CompiledValue(null, valueSpec.literal());
            }
            return new CompiledValue(
                    SafeExpressionSupport.compile(valueSpec.expression(), "Case when value expression"),
                    null);
        }
    }

    private record CompiledCaseRule(
            SafeExpressionSupport.CompiledExpression condition,
            CompiledValue value) {
    }

    private record CompiledValue(
            SafeExpressionSupport.CompiledExpression expression,
            Object literal) {

        private Object evaluate(DataEntity entity) {
            return expression == null ? literal : expression.evaluate(entity.fields());
        }
    }
}
