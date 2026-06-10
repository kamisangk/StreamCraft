package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import com.streamcraft.shared.eval.EvalConfig;
import com.streamcraft.shared.eval.EvalConfig.ErrorStrategy;
import com.streamcraft.shared.eval.EvalConfig.OutputMode;
import com.streamcraft.shared.eval.EvalConfigParser;
import com.streamcraft.shared.expression.SafeExpressionSupport;
import com.streamcraft.shared.fields.FieldPathSupport;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

public class EvalTransformFactory implements TransformFactory {

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        EvalConfig config = EvalConfigParser.parse(node.config(), IllegalArgumentException::new);
        return TransformOutputs.single(input.flatMap(new EvalFlatMapFunction(config)).name(node.name()));
    }

    private static final class EvalFlatMapFunction extends RichFlatMapFunction<DataEntity, DataEntity> {

        private static final long serialVersionUID = 1L;
        private final EvalConfig config;
        private transient SafeExpressionSupport.CompiledExpression compiledExpression;

        private EvalFlatMapFunction(EvalConfig config) {
            this.config = config;
        }

        @Override
        public void open(OpenContext openContext) {
            compiledExpression = SafeExpressionSupport.compile(config.expression(), "Eval expression");
        }

        @Override
        public void flatMap(DataEntity entity, Collector<DataEntity> out) {
            try {
                out.collect(applyResult(entity, compiledExpression.evaluate(entity.fields())));
            } catch (Exception exception) {
                handleError(entity, out, exception);
            }
        }

        private DataEntity applyResult(DataEntity entity, Object result) {
            if (config.outputMode() == OutputMode.WRITE_IF_ABSENT
                    && FieldPathSupport.lookup(entity.fields(), config.targetField()).found()) {
                return entity;
            }
            return entity.withField(config.targetField(), result);
        }

        private void handleError(DataEntity entity, Collector<DataEntity> out, Exception exception) {
            ErrorStrategy strategy = config.errorStrategy();
            switch (strategy) {
                case PUT_NULL -> out.collect(applyResult(entity, null));
                case DISCARD -> {
                }
                case FAIL -> throw new IllegalStateException("EVAL expression failed: " + config.expression(), exception);
                case KEEP_ORIGINAL -> out.collect(entity);
            }
        }
    }
}
