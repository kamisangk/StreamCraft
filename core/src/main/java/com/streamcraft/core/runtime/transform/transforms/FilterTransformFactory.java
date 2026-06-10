package com.streamcraft.core.runtime.transform.transforms;

import com.streamcraft.core.model.DataEntity;
import com.streamcraft.core.model.PipelineNode;
import com.streamcraft.shared.expression.SafeExpressionSupport;
import com.streamcraft.core.runtime.transform.TransformFactory;
import com.streamcraft.core.runtime.transform.TransformOutputs;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterTransformFactory implements TransformFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FilterTransformFactory.class);

    @Override
    public TransformOutputs apply(DataStream<DataEntity> input, PipelineNode node) {
        String condition = node.config().path("condition").asText();
        OutputTag<DataEntity> falseOutputTag = new OutputTag<>(node.id() + "-false") {
            private static final long serialVersionUID = 1L;
        };

        SingleOutputStreamOperator<DataEntity> trueStream = input.process(new ProcessFunction<DataEntity, DataEntity>() {
            private static final long serialVersionUID = 1L;
            private transient SafeExpressionSupport.CompiledExpression compiledCondition;

            @Override
            public void open(OpenContext openContext) {
                compiledCondition = SafeExpressionSupport.compile(condition, "Filter condition");
            }

            @Override
            public void processElement(
                    DataEntity entity,
                    ProcessFunction<DataEntity, DataEntity>.Context context,
                    Collector<DataEntity> collector) {
                Boolean result;
                try {
                    result = compiledCondition.evaluateBoolean(entity.fields());
                } catch (Exception exception) {
                    LOG.warn(
                            "Routing Filter transform record '{}' to false branch because {}",
                            entity.id(),
                            exception.getMessage());
                    context.output(falseOutputTag, entity);
                    return;
                }
                if (result != null && result) {
                    collector.collect(entity);
                    return;
                }
                context.output(falseOutputTag, entity);
            }
        }).name(node.name() + "-true");
        DataStream<DataEntity> falseStream = trueStream.getSideOutput(falseOutputTag)
                .map(entity -> entity)
                .name(node.name() + "-false");
        return new TransformOutputs(Map.of("true", trueStream, "false", falseStream));
    }
}
